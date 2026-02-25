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

// advFieldMapping maps question field keys to mv_firm_combined column info.
type advFieldMapping struct {
	column   string
	derived  bool // true if the value requires Go-side derivation
	fieldKey string
}

// advMappings defines which field keys can be pre-filled from ADV data.
var advMappings = []advFieldMapping{
	{fieldKey: "aum_total", column: "aum"},
	{fieldKey: "assets_under_management", column: "aum"},
	{fieldKey: "total_employees", column: "num_employees"},
	{fieldKey: "employee_count", column: "num_employees"},
	{fieldKey: "num_accounts", column: "num_accounts"},
	{fieldKey: "client_count", column: "num_accounts"},
	{fieldKey: "regulatory_status", derived: true},
	{fieldKey: "has_disciplinary_history", column: "has_any_drp"},
}

// advRow holds the columns queried from mv_firm_combined for pre-fill.
type advRow struct {
	AUM             *int64
	NumEmployees    *int
	NumAccounts     *int
	SECRegistered   *bool
	ExemptReporting *bool
	StateRegistered *bool
	HasAnyDRP       *bool
}

// prefillFromADV queries mv_firm_combined for a given CRD number and returns
// pre-filled ExtractionAnswers for questions whose field keys map to ADV data.
// Returns nil (not an error) if the CRD is not found or no questions match.
func prefillFromADV(ctx context.Context, pool db.Pool, crdNumber int, questions []model.Question) ([]model.ExtractionAnswer, error) {
	if pool == nil || crdNumber == 0 {
		return nil, nil
	}

	// Build a set of field keys that have ADV mappings.
	mappableKeys := make(map[string]bool, len(advMappings))
	for _, m := range advMappings {
		mappableKeys[m.fieldKey] = true
	}

	// Check if any questions match.
	var relevant []model.Question
	for _, q := range questions {
		for _, fk := range splitFieldKeys(q.FieldKey) {
			if mappableKeys[fk] {
				relevant = append(relevant, q)
				break
			}
		}
	}
	if len(relevant) == 0 {
		return nil, nil
	}

	// Query mv_firm_combined for the CRD number.
	row := pool.QueryRow(ctx, `
		SELECT aum, num_employees, num_accounts,
		       sec_registered, exempt_reporting, state_registered,
		       has_any_drp
		FROM fed_data.mv_firm_combined
		WHERE crd_number = $1
	`, crdNumber)

	var adv advRow
	err := row.Scan(
		&adv.AUM, &adv.NumEmployees, &adv.NumAccounts,
		&adv.SECRegistered, &adv.ExemptReporting, &adv.StateRegistered,
		&adv.HasAnyDRP,
	)
	if err != nil {
		// pgx returns no rows as an error; treat as "not found".
		if strings.Contains(err.Error(), "no rows") {
			zap.L().Debug("prefill: CRD not found in mv_firm_combined",
				zap.Int("crd_number", crdNumber),
			)
			return nil, nil
		}
		return nil, eris.Wrap(err, "prefill: query mv_firm_combined")
	}

	// Build answers for each matching field key.
	var answers []model.ExtractionAnswer
	for _, q := range relevant {
		for _, fk := range splitFieldKeys(q.FieldKey) {
			val, ok := resolveADVValue(fk, &adv)
			if !ok {
				continue
			}
			answers = append(answers, model.ExtractionAnswer{
				QuestionID: q.ID,
				FieldKey:   fk,
				Value:      val,
				Confidence: 0.9,
				Source:     "adv_filing",
				Tier:       0,
				Reasoning:  "Pre-filled from SEC ADV filing data",
			})
		}
	}

	if len(answers) > 0 {
		zap.L().Info("prefill: pre-filled from ADV data",
			zap.Int("crd_number", crdNumber),
			zap.Int("answers", len(answers)),
		)
	}

	return answers, nil
}

// resolveADVValue returns the value for a field key from the ADV row.
// Returns (nil, false) if the field key is not mapped or the value is NULL.
func resolveADVValue(fieldKey string, adv *advRow) (any, bool) {
	switch fieldKey {
	case "aum_total", "assets_under_management":
		if adv.AUM == nil {
			return nil, false
		}
		return *adv.AUM, true
	case "total_employees", "employee_count":
		if adv.NumEmployees == nil {
			return nil, false
		}
		return *adv.NumEmployees, true
	case "num_accounts", "client_count":
		if adv.NumAccounts == nil {
			return nil, false
		}
		return *adv.NumAccounts, true
	case "regulatory_status":
		return deriveRegulatoryStatus(adv), true
	case "has_disciplinary_history":
		if adv.HasAnyDRP == nil {
			return nil, false
		}
		return *adv.HasAnyDRP, true
	default:
		return nil, false
	}
}

// deriveRegulatoryStatus builds a human-readable regulatory status string
// from the sec_registered, exempt_reporting, and state_registered booleans.
func deriveRegulatoryStatus(adv *advRow) string {
	var parts []string
	if adv.SECRegistered != nil && *adv.SECRegistered {
		parts = append(parts, "SEC Registered")
	}
	if adv.ExemptReporting != nil && *adv.ExemptReporting {
		parts = append(parts, "Exempt Reporting Adviser")
	}
	if adv.StateRegistered != nil && *adv.StateRegistered {
		parts = append(parts, "State Registered")
	}
	if len(parts) == 0 {
		return "Unknown"
	}
	return strings.Join(parts, ", ")
}

// filterPrefilledQuestions removes questions from routed batches when all their
// field keys have been pre-filled, so the LLM does not re-extract them.
func filterPrefilledQuestions(routed []model.RoutedQuestion, prefilledKeys map[string]bool) ([]model.RoutedQuestion, int) {
	var filtered []model.RoutedQuestion
	var skipped int
	for _, rq := range routed {
		keys := splitFieldKeys(rq.Question.FieldKey)
		allPrefilled := true
		for _, k := range keys {
			if !prefilledKeys[k] {
				allPrefilled = false
				break
			}
		}
		if allPrefilled {
			skipped++
			continue
		}
		filtered = append(filtered, rq)
	}
	return filtered, skipped
}

// prefilledKeySet builds a set of field keys from pre-filled answers.
func prefilledKeySet(answers []model.ExtractionAnswer) map[string]bool {
	keys := make(map[string]bool, len(answers))
	for _, a := range answers {
		keys[a.FieldKey] = true
	}
	return keys
}

// FormatCRDSource returns a source description for pre-filled ADV data.
func FormatCRDSource(crdNumber int) string {
	return fmt.Sprintf("adv_filing (CRD %d)", crdNumber)
}
