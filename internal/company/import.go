package company

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/model"
	"go.uber.org/zap"
)

// Importer bridges the enrichment pipeline to the company golden record.
// It resolves, merges, and persists enrichment results into the companies
// table and all child tables (identifiers, contacts, addresses, tags, sources,
// financials, matches).
type Importer struct {
	resolver *Resolver
	store    CompanyStore
	linker   *Linker // nil if no fed_data pool
}

// NewImporter creates an Importer. If pool is non-nil, fed_data linking is enabled.
func NewImporter(store CompanyStore, pool db.Pool) *Importer {
	imp := &Importer{
		resolver: NewResolver(store),
		store:    store,
	}
	if pool != nil {
		imp.linker = NewLinker(pool, store)
	}
	return imp
}

// Import persists a pipeline enrichment result into the company golden record.
// Child-table failures are logged but do not fail the overall import.
func (imp *Importer) Import(ctx context.Context, co model.Company, result *model.EnrichmentResult, fieldValues map[string]model.FieldValue) (*CompanyRecord, error) {
	log := zap.L().With(zap.String("domain", co.URL), zap.String("company", co.Name))

	// 1. Resolve or create company.
	record, created, err := imp.resolver.FindOrCreate(ctx, co)
	if err != nil {
		return nil, eris.Wrap(err, "import: resolve company")
	}
	if created {
		log.Info("import: created new company record", zap.Int64("company_id", record.ID))
	}

	// 2. Merge field values into golden record.
	MergeGoldenRecord(record, fieldValues)

	// Set enrichment metadata.
	now := time.Now()
	record.LastEnrichedAt = &now
	if result.Score > 0 {
		record.EnrichmentScore = &result.Score
	}
	if result.Report != "" {
		record.EnrichmentReport = result.Report
	}

	// 3. Update company record.
	if err := imp.store.UpdateCompany(ctx, record); err != nil {
		return nil, eris.Wrap(err, "import: update company")
	}

	// --- Child table upserts (non-fatal) ---

	// 4. Identifiers.
	imp.upsertIdentifiers(ctx, log, record.ID, co, fieldValues)

	// 5. Contacts.
	imp.upsertContacts(ctx, log, record.ID, fieldValues, result)

	// 6. Primary address.
	imp.upsertAddress(ctx, log, record.ID, record)

	// 7. Tags.
	imp.upsertTags(ctx, log, record.ID, fieldValues)

	// 8. Sources.
	imp.upsertSources(ctx, log, record.ID, co, result)

	// 9. Financials.
	imp.upsertFinancials(ctx, log, record.ID, fieldValues, result)

	// 10. Link fed_data entities.
	if imp.linker != nil {
		if _, linkErr := imp.linker.LinkFedData(ctx, record.ID); linkErr != nil {
			log.Warn("import: fed_data linking failed", zap.Error(linkErr))
		}
	}

	return record, nil
}

func (imp *Importer) upsertIdentifiers(ctx context.Context, log *zap.Logger, companyID int64, co model.Company, fv map[string]model.FieldValue) {
	upsert := func(system, value string) {
		if value == "" {
			return
		}
		if err := imp.store.UpsertIdentifier(ctx, &Identifier{
			CompanyID:  companyID,
			System:     system,
			Identifier: value,
		}); err != nil {
			log.Warn("import: upsert identifier failed", zap.String("system", system), zap.Error(err))
		}
	}

	upsert(SystemSalesforce, co.SalesforceID)
	upsert(SystemNotion, co.NotionPageID)

	// CRD from pre-seeded data.
	if crd, ok := co.PreSeeded["crd_number"]; ok {
		if s, ok := crd.(string); ok {
			upsert(SystemCRD, s)
		}
	}

	// LinkedIn URL as identifier.
	if v, ok := fv["linkedin_url"]; ok {
		if s, ok := v.Value.(string); ok {
			upsert(SystemLinkedIn, s)
		}
	}

	// Grata ID from pre-seeded data.
	if grataID, ok := co.PreSeeded["grata_id"]; ok {
		if s, ok := grataID.(string); ok {
			upsert(SystemGrata, s)
		}
	}
}

func (imp *Importer) upsertContacts(ctx context.Context, log *zap.Logger, companyID int64, fv map[string]model.FieldValue, result *model.EnrichmentResult) {
	isPrimary := true

	// Executive contact from field values.
	firstName := fieldStr(fv, "exec_first_name")
	lastName := fieldStr(fv, "exec_last_name")
	title := fieldStr(fv, "exec_title")
	linkedIn := fieldStr(fv, "exec_linkedin")

	if firstName != "" || lastName != "" {
		c := &Contact{
			CompanyID:   companyID,
			FirstName:   firstName,
			LastName:    lastName,
			FullName:    strings.TrimSpace(firstName + " " + lastName),
			Title:       title,
			RoleType:    RoleExecutive,
			LinkedInURL: linkedIn,
			IsPrimary:   isPrimary,
			Source:      "enrichment",
		}
		if err := imp.store.UpsertContact(ctx, c); err != nil {
			log.Warn("import: upsert exec contact failed", zap.Error(err))
		} else {
			isPrimary = false
		}
	}

	// Owner contact from field values.
	ownerName := fieldStr(fv, "owner_name")
	if ownerName != "" && ownerName != strings.TrimSpace(firstName+" "+lastName) {
		parts := strings.SplitN(ownerName, " ", 2)
		first := parts[0]
		last := ""
		if len(parts) > 1 {
			last = parts[1]
		}
		c := &Contact{
			CompanyID: companyID,
			FirstName: first,
			LastName:  last,
			FullName:  ownerName,
			RoleType:  RoleOwner,
			IsPrimary: isPrimary,
			Source:    "enrichment",
		}
		if err := imp.store.UpsertContact(ctx, c); err != nil {
			log.Warn("import: upsert owner contact failed", zap.Error(err))
		} else {
			isPrimary = false
		}
	}

	// Key people from extraction answers (JSON array of contact objects).
	for _, ans := range result.Answers {
		if ans.FieldKey != "contacts" && ans.FieldKey != "key_contacts" {
			continue
		}
		raw, err := json.Marshal(ans.Value)
		if err != nil {
			continue
		}
		var contacts []struct {
			Name     string `json:"name"`
			Title    string `json:"title"`
			Email    string `json:"email"`
			Phone    string `json:"phone"`
			LinkedIn string `json:"linkedin"`
		}
		if err := json.Unmarshal(raw, &contacts); err != nil {
			continue
		}
		for _, kc := range contacts {
			parts := strings.SplitN(kc.Name, " ", 2)
			first := parts[0]
			last := ""
			if len(parts) > 1 {
				last = parts[1]
			}
			c := &Contact{
				CompanyID:   companyID,
				FirstName:   first,
				LastName:    last,
				FullName:    kc.Name,
				Title:       kc.Title,
				Email:       kc.Email,
				Phone:       kc.Phone,
				LinkedInURL: kc.LinkedIn,
				RoleType:    RoleKeyPerson,
				IsPrimary:   isPrimary,
				Source:      "enrichment",
			}
			if err := imp.store.UpsertContact(ctx, c); err != nil {
				log.Warn("import: upsert key contact failed", zap.Error(err))
			} else {
				isPrimary = false
			}
		}
	}
}

func (imp *Importer) upsertAddress(ctx context.Context, log *zap.Logger, companyID int64, record *CompanyRecord) {
	if record.Street == "" && record.City == "" && record.State == "" {
		return
	}
	addr := &Address{
		CompanyID:   companyID,
		AddressType: AddressPrincipal,
		Street:      record.Street,
		City:        record.City,
		State:       record.State,
		ZipCode:     record.ZipCode,
		Country:     record.Country,
		IsPrimary:   true,
		Source:      "enrichment",
	}
	if err := imp.store.UpsertAddress(ctx, addr); err != nil {
		log.Warn("import: upsert address failed", zap.Error(err))
	}
}

func (imp *Importer) upsertTags(ctx context.Context, log *zap.Logger, companyID int64, fv map[string]model.FieldValue) {
	tagSets := map[string]string{
		TagService:        "services_list",
		TagCustomerType:   "customer_types",
		TagIndustry:       "end_markets",
		TagDifferentiator: "differentiators",
	}
	// Also check canonical aliases.
	aliasMap := map[string]string{
		TagService: "service_mix",
	}

	for tagType, fieldKey := range tagSets {
		s := fieldStr(fv, fieldKey)
		if s == "" {
			if alias, ok := aliasMap[tagType]; ok {
				s = fieldStr(fv, alias)
			}
		}
		if s == "" {
			continue
		}
		values := splitTags(s)
		if len(values) == 0 {
			continue
		}
		if err := imp.store.SetTags(ctx, companyID, tagType, values); err != nil {
			log.Warn("import: set tags failed", zap.String("type", tagType), zap.Error(err))
		}
	}
}

func (imp *Importer) upsertSources(ctx context.Context, log *zap.Logger, companyID int64, co model.Company, result *model.EnrichmentResult) {
	now := time.Now()
	runID := result.RunID

	// Determine which sources contributed by checking phases.
	sourcePhases := map[string]string{
		"1a_crawl":      "crawl",
		"1b_scrape":     "scrape",
		"1c_linkedin":   "linkedin",
		"1d_ppp":        "ppp",
		"1e_perplexity": "perplexity_intel",
	}

	for _, phase := range result.Phases {
		sourceName, ok := sourcePhases[phase.Name]
		if !ok || phase.Status != model.PhaseStatusComplete {
			continue
		}
		src := &Source{
			CompanyID:  companyID,
			SourceName: sourceName,
			SourceID:   co.URL,
			FetchedAt:  now,
		}
		if runID != "" {
			// Store run ID in raw_data as JSON.
			raw, _ := json.Marshal(map[string]string{"run_id": runID})
			src.RawData = raw
		}
		if err := imp.store.UpsertSource(ctx, src); err != nil {
			log.Warn("import: upsert source failed", zap.String("source", sourceName), zap.Error(err))
		}
	}

	// Grata pre-seed.
	if _, ok := co.PreSeeded["grata_id"]; ok {
		src := &Source{
			CompanyID:  companyID,
			SourceName: "grata",
			SourceID:   co.URL,
			FetchedAt:  now,
		}
		if err := imp.store.UpsertSource(ctx, src); err != nil {
			log.Warn("import: upsert grata source failed", zap.Error(err))
		}
	}

	// ADV pre-fill.
	if _, ok := co.PreSeeded["crd_number"]; ok {
		src := &Source{
			CompanyID:  companyID,
			SourceName: "adv_filing",
			SourceID:   co.URL,
			FetchedAt:  now,
		}
		if err := imp.store.UpsertSource(ctx, src); err != nil {
			log.Warn("import: upsert adv source failed", zap.Error(err))
		}
	}
}

func (imp *Importer) upsertFinancials(ctx context.Context, log *zap.Logger, companyID int64, fv map[string]model.FieldValue, result *model.EnrichmentResult) {
	now := time.Now()

	// Revenue estimate.
	if v, ok := fv["revenue_estimate"]; ok {
		if amt := toFloat64Val(v.Value); amt > 0 {
			f := &Financial{
				CompanyID:  companyID,
				PeriodType: "annual",
				PeriodDate: time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC),
				Metric:     "revenue",
				Value:      amt,
				SourceName: "enrichment",
			}
			if err := imp.store.UpsertFinancial(ctx, f); err != nil {
				log.Warn("import: upsert revenue financial failed", zap.Error(err))
			}
		}
	}

	// PPP loan amounts.
	for _, m := range result.PPPMatches {
		if m.CurrentApproval <= 0 {
			continue
		}
		f := &Financial{
			CompanyID:  companyID,
			PeriodType: "annual",
			PeriodDate: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			Metric:     "ppp_loan_amount",
			Value:      m.CurrentApproval,
			SourceName: "ppp",
		}
		if err := imp.store.UpsertFinancial(ctx, f); err != nil {
			log.Warn("import: upsert ppp financial failed", zap.Error(err))
		}
	}
}

// fieldStr extracts a string field value from the map.
func fieldStr(fv map[string]model.FieldValue, key string) string {
	v, ok := fv[key]
	if !ok {
		return ""
	}
	s, _ := v.Value.(string)
	return s
}

// splitTags splits a comma/semicolon-separated string into trimmed non-empty values.
func splitTags(s string) []string {
	s = strings.ReplaceAll(s, ";", ",")
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// toFloat64Val converts a FieldValue.Value to float64.
func toFloat64Val(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}
