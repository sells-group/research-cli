package dataset

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
)

// Metadata captures the frontend-only descriptive fields for a dataset.
type Metadata struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// CatalogEntry describes one dataset with the additional metadata needed by the frontend.
type CatalogEntry struct {
	Name        string  `json:"name"`
	Label       string  `json:"label"`
	Phase       string  `json:"phase"`
	Cadence     Cadence `json:"cadence"`
	Table       string  `json:"table"`
	Description string  `json:"description"`
}

// Catalog captures the live dataset inventory together with frontend metadata.
type Catalog struct {
	Total     int            `json:"total"`
	ByPhase   []Count        `json:"by_phase"`
	ByCadence []Count        `json:"by_cadence"`
	Datasets  []CatalogEntry `json:"datasets"`
}

var datasetMetadata = map[string]Metadata{
	"cbp":               {Label: "County Business Patterns", Description: "Census CBP establishment and employment data by county and NAICS"},
	"susb":              {Label: "Statistics of US Business", Description: "Census SUSB firm size by employment and receipts"},
	"qcew":              {Label: "Quarterly Census of Employment", Description: "BLS QCEW establishment, employment, and wage data"},
	"oews":              {Label: "Occupational Employment", Description: "BLS OEWS occupation employment and wage estimates"},
	"fpds":              {Label: "Federal Procurement", Description: "SAM.gov federal procurement contract awards"},
	"econ_census":       {Label: "Economic Census", Description: "Census Economic Census comprehensive industry data"},
	"ppp":               {Label: "PPP Loans", Description: "SBA Paycheck Protection Program loan data"},
	"sba_7a_504":        {Label: "SBA 7(a)/504 Loans", Description: "SBA 7(a) and 504 loan program data"},
	"form_5500":         {Label: "Form 5500 ERISA", Description: "DOL Form 5500 employee benefit plan filings"},
	"eo_bmf":            {Label: "IRS Exempt Orgs", Description: "IRS Exempt Organizations Business Master File"},
	"census_geo":        {Label: "Census Geography", Description: "Census CBSA/MSA geographic definitions"},
	"usaspending":       {Label: "USAspending", Description: "USAspending.gov award and subaward data"},
	"adv_part1":         {Label: "ADV Part 1A", Description: "SEC ADV Part 1A investment adviser registrations"},
	"ia_compilation":    {Label: "IARD Daily", Description: "IARD investment adviser representative compilation"},
	"holdings_13f":      {Label: "13F Holdings", Description: "SEC 13F institutional investment manager holdings"},
	"form_d":            {Label: "Form D", Description: "EDGAR Form D private placement notices"},
	"edgar_submissions": {Label: "EDGAR Submissions", Description: "EDGAR bulk company submissions and filings"},
	"entity_xref":       {Label: "Entity Cross-Reference", Description: "Cross-reference relationships across entity datasets"},
	"adv_part2":         {Label: "ADV Part 2 Brochures", Description: "SEC ADV Part 2A brochure PDF extraction"},
	"brokercheck":       {Label: "BrokerCheck", Description: "FINRA BrokerCheck broker-dealer registrations"},
	"sec_enforcement":   {Label: "SEC Enforcement", Description: "SEC enforcement actions and proceedings"},
	"form_bd":           {Label: "Form BD", Description: "FINRA Form BD broker-dealer registrations"},
	"osha_ita":          {Label: "OSHA ITA", Description: "OSHA injury tracking application inspection data"},
	"epa_echo":          {Label: "EPA ECHO", Description: "EPA ECHO facility compliance and enforcement"},
	"nes":               {Label: "Nonemployer Statistics", Description: "Census Nonemployer Statistics"},
	"asm":               {Label: "Annual Survey of Manufactures", Description: "Census Annual Survey of Manufactures"},
	"eci":               {Label: "Employment Cost Index", Description: "BLS Employment Cost Index compensation trends"},
	"fdic_bankfind":     {Label: "FDIC BankFind", Description: "FDIC BankFind financial institution data"},
	"ncen":              {Label: "N-CEN", Description: "SEC Form N-CEN registered fund census filings"},
	"ncua_call_reports": {Label: "NCUA Call Reports", Description: "NCUA quarterly credit union call reports"},
	"bea_regional":      {Label: "BEA Regional", Description: "BEA regional GDP and personal income data"},
	"irs_soi_migration": {Label: "IRS SOI Migration", Description: "IRS SOI county-to-county migration flows"},
	"building_permits":  {Label: "Building Permits", Description: "Census building permits by place and county"},
	"adv_part3":         {Label: "CRS Brochures", Description: "SEC ADV Part 3 CRS relationship summary PDFs"},
	"adv_enrichment":    {Label: "ADV Enrichment", Description: "ADV brochure structured section extraction"},
	"adv_extract":       {Label: "ADV Extract", Description: "ADV advisor answer extraction via LLM"},
	"xbrl_facts":        {Label: "XBRL Facts", Description: "EDGAR XBRL financial fact data"},
	"fred":              {Label: "FRED Series", Description: "Federal Reserve FRED economic data series"},
	"abs":               {Label: "Annual Business Survey", Description: "Census Annual Business Survey"},
	"cps_laus":          {Label: "CPS/LAUS", Description: "BLS Current Population Survey / Local Area Unemployment"},
	"m3":                {Label: "M3 Manufacturers", Description: "Census M3 manufacturers shipments/inventories/orders"},
	"lehd_lodes":        {Label: "LEHD LODES", Description: "Census LEHD LODES origin-destination employment data"},
}

// BuildCatalog returns a live dataset catalog merged with frontend metadata.
func BuildCatalog(cfg *config.Config) (Catalog, error) {
	summary := BuildSummary(cfg)
	entries := make([]CatalogEntry, 0, len(summary.Datasets))
	seen := make(map[string]struct{}, len(summary.Datasets))

	for _, ds := range summary.Datasets {
		meta, ok := datasetMetadata[ds.Name]
		if !ok {
			return Catalog{}, eris.Errorf("dataset metadata missing for %q", ds.Name)
		}

		entries = append(entries, CatalogEntry{
			Name:        ds.Name,
			Label:       meta.Label,
			Phase:       ds.Phase,
			Cadence:     ds.Cadence,
			Table:       ds.Table,
			Description: meta.Description,
		})
		seen[ds.Name] = struct{}{}
	}

	var orphaned []string
	for name := range datasetMetadata {
		if _, ok := seen[name]; ok {
			continue
		}
		orphaned = append(orphaned, name)
	}
	if len(orphaned) > 0 {
		slices.Sort(orphaned)
		return Catalog{}, eris.Errorf("dataset metadata has orphan entries: %s", strings.Join(orphaned, ", "))
	}

	return Catalog{
		Total:     summary.Total,
		ByPhase:   summary.ByPhase,
		ByCadence: summary.ByCadence,
		Datasets:  entries,
	}, nil
}

// RenderTypeScriptCatalog returns the generated frontend dataset catalog module.
func RenderTypeScriptCatalog(cfg *config.Config) (string, error) {
	catalog, err := BuildCatalog(cfg)
	if err != nil {
		return "", err
	}

	payload, err := json.MarshalIndent(catalog.Datasets, "", "  ")
	if err != nil {
		return "", eris.Wrap(err, "render TypeScript catalog")
	}

	var b strings.Builder
	b.WriteString("// Code generated by research-cli fedsync generate; DO NOT EDIT.\n\n")
	b.WriteString("export const datasetsMeta = ")
	b.Write(payload)
	b.WriteString(" as const;\n")
	return b.String(), nil
}

// ReplaceSummaryBlock replaces the marked dataset summary block in content.
func ReplaceSummaryBlock(content, replacement string) (string, error) {
	start := strings.Index(content, SummaryBlockStart)
	if start == -1 {
		return "", eris.New("missing summary start marker")
	}

	end := strings.Index(content, SummaryBlockEnd)
	if end == -1 {
		return "", eris.New("missing summary end marker")
	}
	end += len(SummaryBlockEnd)

	return content[:start] + replacement + content[end:], nil
}

// ValidateCatalog returns an error when dataset metadata and registry data drift apart.
func ValidateCatalog(cfg *config.Config) error {
	_, err := BuildCatalog(cfg)
	return err
}

// RenderCatalogJSON returns the catalog encoded as indented JSON.
func RenderCatalogJSON(cfg *config.Config) (string, error) {
	catalog, err := BuildCatalog(cfg)
	if err != nil {
		return "", err
	}

	payload, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return "", eris.Wrap(err, "render catalog JSON")
	}
	return fmt.Sprintf("%s\n", payload), nil
}
