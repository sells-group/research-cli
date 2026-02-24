package advextract

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// PopulateRelationships parses extracted answers and populates normalized relationship tables.
func PopulateRelationships(ctx context.Context, pool db.Pool, crd int, answers []Answer) error {
	answerMap := make(map[string]Answer, len(answers))
	for _, a := range answers {
		answerMap[a.QuestionKey] = a
	}

	if err := populateCustodians(ctx, pool, crd, answerMap); err != nil {
		return eris.Wrap(err, "advextract: populate custodians")
	}

	if err := populateBDAffiliations(ctx, pool, crd, answerMap); err != nil {
		return eris.Wrap(err, "advextract: populate BD affiliations")
	}

	if err := populateServiceProviders(ctx, pool, crd, answerMap); err != nil {
		return eris.Wrap(err, "advextract: populate service providers")
	}

	return nil
}

// populateCustodians extracts custodian names from answers and upserts into adv_custodian_relationships.
func populateCustodians(ctx context.Context, pool db.Pool, crd int, answers map[string]Answer) error {
	custodians := make(map[string]string) // normalized name -> relationship

	// Primary custodian.
	if a, ok := answers["primary_custodian"]; ok && a.Value != nil {
		if name := extractStringValue(a.Value); name != "" {
			custodians[normalizeName(name)] = "primary"
		}
	}

	// Secondary custodians.
	if a, ok := answers["secondary_custodians"]; ok && a.Value != nil {
		for _, name := range extractStringList(a.Value) {
			if name != "" {
				custodians[normalizeName(name)] = "secondary"
			}
		}
	}

	// All custodians list (cross-doc).
	if a, ok := answers["all_custodians_list"]; ok && a.Value != nil {
		for _, name := range extractStringList(a.Value) {
			if name != "" {
				normalized := normalizeName(name)
				if _, exists := custodians[normalized]; !exists {
					custodians[normalized] = "mentioned"
				}
			}
		}
	}

	// Qualified custodian from custody section.
	if a, ok := answers["qualified_custodian_name"]; ok && a.Value != nil {
		if name := extractStringValue(a.Value); name != "" {
			normalized := normalizeName(name)
			if _, exists := custodians[normalized]; !exists {
				custodians[normalized] = "qualified"
			}
		}
	}

	if len(custodians) == 0 {
		return nil
	}

	cols := []string{"crd_number", "custodian_name", "relationship"}
	conflictKeys := []string{"crd_number", "custodian_name"}

	var rows [][]any
	for name, rel := range custodians {
		rows = append(rows, []any{crd, name, rel})
	}

	_, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        "fed_data.adv_custodian_relationships",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return err
}

// populateBDAffiliations extracts broker-dealer affiliation data.
func populateBDAffiliations(ctx context.Context, pool db.Pool, crd int, answers map[string]Answer) error {
	var rows [][]any

	if a, ok := answers["affiliated_broker_dealer_name"]; ok && a.Value != nil {
		if name := extractStringValue(a.Value); name != "" {
			rows = append(rows, []any{crd, normalizeName(name), nil, "affiliated"})
		}
	}

	if len(rows) == 0 {
		return nil
	}

	cols := []string{"crd_number", "bd_name", "bd_crd", "relationship"}
	conflictKeys := []string{"crd_number", "bd_name"}

	_, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        "fed_data.adv_bd_affiliations",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return err
}

// populateServiceProviders extracts service provider relationships.
func populateServiceProviders(ctx context.Context, pool db.Pool, crd int, answers map[string]Answer) error {
	providers := make(map[string]string) // "name|type" -> type

	providerKeys := []struct {
		answerKey    string
		providerType string
	}{
		{"tamp_platform", "tamp"},
		{"fund_auditor", "auditor"},
		{"fund_administrator", "administrator"},
		{"fund_prime_broker", "prime_broker"},
		{"tech_portfolio_accounting", "portfolio_accounting"},
		{"tech_crm_platform", "crm"},
		{"tech_trading_platform", "trading_platform"},
		{"tech_financial_planning_sw", "financial_planning"},
		{"tech_reporting_tools", "reporting"},
		{"tech_cloud_provider", "cloud_provider"},
	}

	for _, pk := range providerKeys {
		if a, ok := answers[pk.answerKey]; ok && a.Value != nil {
			if name := extractStringValue(a.Value); name != "" {
				key := normalizeName(name) + "|" + pk.providerType
				providers[key] = pk.providerType
			}
		}
	}

	if len(providers) == 0 {
		return nil
	}

	cols := []string{"crd_number", "provider_name", "provider_type"}
	conflictKeys := []string{"crd_number", "provider_name", "provider_type"}

	var rows [][]any
	for key, provType := range providers {
		parts := strings.SplitN(key, "|", 2)
		rows = append(rows, []any{crd, parts[0], provType})
	}

	_, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
		Table:        "fed_data.adv_service_providers",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return err
}

// normalizeName trims, lowercases, and normalizes an entity name for deduplication.
func normalizeName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	// Remove common suffixes for dedup.
	for _, suffix := range []string{", llc", ", inc", ", inc.", " llc", " inc", " inc.", " corp", " corp.", ", ltd", " ltd"} {
		s = strings.TrimSuffix(s, suffix)
	}
	return strings.TrimSpace(s)
}

// extractStringValue extracts a string from various answer value types.
func extractStringValue(v any) string {
	switch val := v.(type) {
	case string:
		return strings.TrimSpace(val)
	case json.RawMessage:
		var s string
		if err := json.Unmarshal(val, &s); err == nil {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

// extractStringList extracts a list of strings from various answer value types.
func extractStringList(v any) []string {
	switch val := v.(type) {
	case []any:
		var result []string
		for _, item := range val {
			if s, ok := item.(string); ok && s != "" {
				result = append(result, strings.TrimSpace(s))
			}
		}
		return result
	case string:
		// Try comma-separated.
		parts := strings.Split(val, ",")
		var result []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				result = append(result, p)
			}
		}
		return result
	case json.RawMessage:
		var arr []string
		if err := json.Unmarshal(val, &arr); err == nil {
			return arr
		}
		// Try single string.
		var s string
		if err := json.Unmarshal(val, &s); err == nil && s != "" {
			return []string{s}
		}
	}
	return nil
}
