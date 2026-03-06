package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/model"
)

// FederalContext holds aggregated federal data for a company, used as
// grounding context during T2/T3 extraction.
type FederalContext struct {
	// EntityMatches lists all cross-referenced federal entities.
	EntityMatches []FedEntityMatch `json:"entity_matches,omitempty"`

	// Summary is a formatted text block for injection into LLM prompts.
	Summary string `json:"summary,omitempty"`
}

// FedEntityMatch represents a single cross-reference match to a federal dataset.
type FedEntityMatch struct {
	Dataset    string  `json:"dataset"`
	EntityID   string  `json:"entity_id"`
	EntityName string  `json:"entity_name"`
	MatchType  string  `json:"match_type"`
	Confidence float64 `json:"confidence"`
}

// LookupFederalContext queries entity_xref_multi and fed_data tables to build
// a federal context block for the given company identifiers.
// It returns nil if no federal matches are found or if the pool is not set.
func LookupFederalContext(ctx context.Context, pool db.Pool, identifiers map[string]string) (*FederalContext, error) {
	if pool == nil || len(identifiers) == 0 {
		return nil, nil
	}

	log := zap.L().With(zap.String("phase", "fed_context"))

	// Collect entity matches from xref table.
	var matches []FedEntityMatch

	// Strategy: Direct lookup by known identifiers in entity_xref_multi.
	for system, value := range identifiers {
		if value == "" {
			continue
		}

		dataset, _ := identifierToDataset(system)
		if dataset == "" {
			continue
		}

		rows, err := pool.Query(ctx, `
			SELECT target_dataset, target_id, entity_name, match_type, confidence
			FROM fed_data.entity_xref_multi
			WHERE source_dataset = $1 AND source_id = $2
			UNION
			SELECT source_dataset, source_id, entity_name, match_type, confidence
			FROM fed_data.entity_xref_multi
			WHERE target_dataset = $1 AND target_id = $2
			ORDER BY confidence DESC
			LIMIT 20`, dataset, value)
		if err != nil {
			log.Warn("fed_context: xref query failed",
				zap.String("system", system),
				zap.Error(err),
			)
			continue
		}

		for rows.Next() {
			var m FedEntityMatch
			if scanErr := rows.Scan(&m.Dataset, &m.EntityID, &m.EntityName, &m.MatchType, &m.Confidence); scanErr != nil {
				rows.Close()
				return nil, eris.Wrap(scanErr, "fed_context: scan xref match")
			}
			matches = append(matches, m)
		}
		rows.Close()
		if rows.Err() != nil {
			return nil, eris.Wrap(rows.Err(), "fed_context: iterate xref matches")
		}
	}

	if len(matches) == 0 {
		return nil, nil
	}

	// Deduplicate by dataset+entity_id.
	seen := make(map[string]bool)
	var deduped []FedEntityMatch
	for _, m := range matches {
		key := m.Dataset + ":" + m.EntityID
		if seen[key] {
			continue
		}
		seen[key] = true
		deduped = append(deduped, m)
	}

	// Build enrichment context from matched entities.
	fc := &FederalContext{
		EntityMatches: deduped,
	}

	// Fetch summary data from key federal datasets.
	summaryParts := buildFederalSummary(ctx, pool, deduped, log)
	if len(summaryParts) > 0 {
		fc.Summary = strings.Join(summaryParts, "\n\n")
	}

	return fc, nil
}

// identifierToDataset maps company identifier systems to their primary
// federal dataset and ID column.
func identifierToDataset(system string) (string, string) {
	switch system {
	case "crd":
		return "adv_firms", "crd_number"
	case "cik":
		return "edgar_entities", "cik"
	case "ein":
		return "form_5500", "ack_id"
	case "fdic":
		return "fdic_institutions", "cert"
	case "ncua":
		return "ncua_call_reports", "cu_number"
	default:
		return "", ""
	}
}

// buildFederalSummary pulls key data points from matched federal datasets
// and formats them into human-readable summary lines for LLM context.
func buildFederalSummary(ctx context.Context, pool db.Pool, matches []FedEntityMatch, log *zap.Logger) []string {
	var parts []string
	queried := make(map[string]bool)

	for _, m := range matches {
		// Only query each dataset once.
		if queried[m.Dataset] {
			continue
		}
		queried[m.Dataset] = true

		switch m.Dataset {
		case "adv_firms":
			if summary := queryADVSummary(ctx, pool, m.EntityID, log); summary != "" {
				parts = append(parts, summary)
			}
		case "edgar_entities":
			if summary := queryEDGARSummary(ctx, pool, m.EntityID, log); summary != "" {
				parts = append(parts, summary)
			}
		case "fdic_institutions":
			if summary := queryFDICSummary(ctx, pool, m.EntityID, log); summary != "" {
				parts = append(parts, summary)
			}
		case "ncua_call_reports":
			if summary := queryNCUASummary(ctx, pool, m.EntityID, log); summary != "" {
				parts = append(parts, summary)
			}
		case "form_5500":
			if summary := query5500Summary(ctx, pool, m.EntityID, log); summary != "" {
				parts = append(parts, summary)
			}
		case "fpds_contracts":
			if summary := queryFPDSSummary(ctx, pool, m.EntityID, log); summary != "" {
				parts = append(parts, summary)
			}
		}
	}

	return parts
}

func queryADVSummary(ctx context.Context, pool db.Pool, crdNumber string, log *zap.Logger) string {
	var firmName *string
	var aum, employees *int64
	var state *string
	err := pool.QueryRow(ctx, `
		SELECT firm_name, aum, num_employees, state
		FROM fed_data.adv_firms
		WHERE crd_number::text = $1
		LIMIT 1`, crdNumber).Scan(&firmName, &aum, &employees, &state)
	if err != nil {
		log.Debug("fed_context: ADV query failed", zap.Error(err))
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[SEC ADV Filing Data]")
	if firmName != nil {
		fmt.Fprintf(&sb, "\nRegistered Name: %s", *firmName)
	}
	if aum != nil {
		fmt.Fprintf(&sb, "\nAssets Under Management (AUM): $%s", formatLargeNumber(*aum))
	}
	if employees != nil {
		fmt.Fprintf(&sb, "\nEmployees: %d", *employees)
	}
	if state != nil {
		fmt.Fprintf(&sb, "\nState: %s", *state)
	}
	return sb.String()
}

func queryEDGARSummary(ctx context.Context, pool db.Pool, cik string, log *zap.Logger) string {
	var name, sic, stateOfBusiness *string
	err := pool.QueryRow(ctx, `
		SELECT entity_name, sic, state_of_business
		FROM fed_data.edgar_entities
		WHERE cik = $1
		LIMIT 1`, cik).Scan(&name, &sic, &stateOfBusiness)
	if err != nil {
		log.Debug("fed_context: EDGAR query failed", zap.Error(err))
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[SEC EDGAR Filing Data]")
	if name != nil {
		fmt.Fprintf(&sb, "\nEntity Name: %s", *name)
	}
	if sic != nil && *sic != "" {
		fmt.Fprintf(&sb, "\nSIC Code: %s", *sic)
	}
	if stateOfBusiness != nil && *stateOfBusiness != "" {
		fmt.Fprintf(&sb, "\nState of Business: %s", *stateOfBusiness)
	}
	return sb.String()
}

func queryFDICSummary(ctx context.Context, pool db.Pool, cert string, log *zap.Logger) string {
	var name *string
	var asset *int64
	var offices *int
	var state *string
	err := pool.QueryRow(ctx, `
		SELECT name, asset, offices, stalp
		FROM fed_data.fdic_institutions
		WHERE cert::text = $1
		LIMIT 1`, cert).Scan(&name, &asset, &offices, &state)
	if err != nil {
		log.Debug("fed_context: FDIC query failed", zap.Error(err))
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[FDIC Institution Data]")
	if name != nil {
		fmt.Fprintf(&sb, "\nInstitution Name: %s", *name)
	}
	if asset != nil {
		fmt.Fprintf(&sb, "\nTotal Assets: $%s", formatLargeNumber(*asset*1000))
	}
	if offices != nil {
		fmt.Fprintf(&sb, "\nOffices: %d", *offices)
	}
	if state != nil {
		fmt.Fprintf(&sb, "\nState: %s", *state)
	}
	return sb.String()
}

func queryNCUASummary(ctx context.Context, pool db.Pool, cuNumber string, log *zap.Logger) string {
	var name *string
	var totalAssets, totalLoans *int64
	var members *int
	var state *string
	err := pool.QueryRow(ctx, `
		SELECT cu_name, total_assets, total_loans, members, state
		FROM fed_data.ncua_call_reports
		WHERE cu_number::text = $1
		ORDER BY cycle_date DESC
		LIMIT 1`, cuNumber).Scan(&name, &totalAssets, &totalLoans, &members, &state)
	if err != nil {
		log.Debug("fed_context: NCUA query failed", zap.Error(err))
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[NCUA Credit Union Data]")
	if name != nil {
		fmt.Fprintf(&sb, "\nCredit Union Name: %s", *name)
	}
	if totalAssets != nil {
		fmt.Fprintf(&sb, "\nTotal Assets: $%s", formatLargeNumber(*totalAssets))
	}
	if totalLoans != nil {
		fmt.Fprintf(&sb, "\nTotal Loans: $%s", formatLargeNumber(*totalLoans))
	}
	if members != nil {
		fmt.Fprintf(&sb, "\nMembers: %d", *members)
	}
	if state != nil {
		fmt.Fprintf(&sb, "\nState: %s", *state)
	}
	return sb.String()
}

func query5500Summary(ctx context.Context, pool db.Pool, ackID string, log *zap.Logger) string {
	var name *string
	var totalAssets *int64
	var participants *int
	err := pool.QueryRow(ctx, `
		SELECT sponsor_dfe_name, tot_assets_eoy_amt, tot_act_rtd_sep_benef_cnt
		FROM fed_data.form_5500
		WHERE ack_id = $1
		LIMIT 1`, ackID).Scan(&name, &totalAssets, &participants)
	if err != nil {
		log.Debug("fed_context: 5500 query failed", zap.Error(err))
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[DOL Form 5500 ERISA Plan Data]")
	if name != nil {
		fmt.Fprintf(&sb, "\nPlan Sponsor: %s", *name)
	}
	if totalAssets != nil {
		fmt.Fprintf(&sb, "\nPlan Assets: $%s", formatLargeNumber(*totalAssets))
	}
	if participants != nil {
		fmt.Fprintf(&sb, "\nParticipants: %d", *participants)
	}
	return sb.String()
}

func queryFPDSSummary(ctx context.Context, pool db.Pool, contractID string, log *zap.Logger) string {
	var vendorName *string
	var totalObligation *float64
	var naics *string
	err := pool.QueryRow(ctx, `
		SELECT vendor_name, base_exercised_options_value, naics_code
		FROM fed_data.fpds_contracts
		WHERE contract_id = $1
		LIMIT 1`, contractID).Scan(&vendorName, &totalObligation, &naics)
	if err != nil {
		log.Debug("fed_context: FPDS query failed", zap.Error(err))
		return ""
	}

	var sb strings.Builder
	sb.WriteString("[Federal Contract Data (FPDS)]")
	if vendorName != nil {
		fmt.Fprintf(&sb, "\nVendor: %s", *vendorName)
	}
	if totalObligation != nil {
		fmt.Fprintf(&sb, "\nContract Value: $%s", formatLargeNumber(int64(*totalObligation)))
	}
	if naics != nil && *naics != "" {
		fmt.Fprintf(&sb, "\nNAICS: %s", *naics)
	}
	return sb.String()
}

// formatLargeNumber formats a number with commas for readability.
func formatLargeNumber(n int64) string {
	if n < 0 {
		return "-" + formatLargeNumber(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	remainder := len(s) % 3
	if remainder > 0 {
		result.WriteString(s[:remainder])
		if len(s) > remainder {
			result.WriteString(",")
		}
	}
	for i := remainder; i < len(s); i += 3 {
		if i > remainder {
			result.WriteString(",")
		}
		result.WriteString(s[i : i+3])
	}
	return result.String()
}

// buildIdentifiersMap extracts known federal identifiers from PreSeeded data.
func buildIdentifiersMap(preSeeded map[string]any) map[string]string {
	ids := make(map[string]string)

	// CRD number (from ADV filings).
	if v, ok := preSeeded["crd_number"]; ok {
		ids["crd"] = fmt.Sprintf("%v", v)
	}

	// CIK (SEC EDGAR).
	if v, ok := preSeeded["cik"]; ok {
		ids["cik"] = fmt.Sprintf("%v", v)
	}

	// EIN (IRS / DOL).
	if v, ok := preSeeded["ein"]; ok {
		ids["ein"] = fmt.Sprintf("%v", v)
	}

	// FDIC certificate number.
	if v, ok := preSeeded["fdic_cert"]; ok {
		ids["fdic"] = fmt.Sprintf("%v", v)
	}

	// NCUA charter/CU number.
	if v, ok := preSeeded["ncua_cu_number"]; ok {
		ids["ncua"] = fmt.Sprintf("%v", v)
	}

	return ids
}

// federalContextToPage converts a FederalContext summary into a synthetic
// CrawledPage for inclusion in the page set available to T2/T3 extraction.
func federalContextToPage(fc *FederalContext) model.CrawledPage {
	return model.CrawledPage{
		URL:        "federal_data://entity_xref",
		Title:      "Federal Data Cross-Reference",
		Markdown:   "# Federal Data Cross-Reference\n\n" + fc.Summary,
		StatusCode: 200,
	}
}
