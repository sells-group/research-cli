package advextract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Model constants.
const (
	ModelHaiku  = "claude-haiku-4-5-20251001"
	ModelSonnet = "claude-sonnet-4-5-20250514"
	ModelOpus   = "claude-opus-4-6-20250610"
)

// ModelForTier returns the Claude model for a given tier.
func ModelForTier(tier int) string {
	switch tier {
	case 1:
		return ModelHaiku
	case 2:
		return ModelSonnet
	case 3:
		return ModelOpus
	default:
		return ModelHaiku
	}
}

// MaxTokensForTier returns the max output tokens for a tier.
func MaxTokensForTier(tier int) int64 {
	switch tier {
	case 1:
		return 256 // reduced from 512 — Haiku answers are single-fact
	case 2:
		return 1024
	case 3:
		return 2048
	default:
		return 256
	}
}

// systemPrompt is the shared system instruction for all tiers.
const systemPrompt = `You are an expert M&A analyst specializing in investment advisory firms. You are analyzing SEC ADV filings to extract structured intelligence about registered investment advisors.

Your role is to answer specific questions about the advisor based on their ADV documents (Part 1 structured data, Part 2 brochure, Part 3 CRS).

Rules:
- Answer ONLY based on information present in the provided documents
- Return valid JSON for every response
- Use null for the value if the information is not found in the documents
- Confidence should be 0.0-1.0 based on how directly the documents address the question
- Be precise and factual — this data will be used for M&A due diligence
- For numerical values, use raw numbers without formatting (e.g., 1000000 not "1,000,000")
- For lists, return JSON arrays
- For yes/no questions, return true/false`

// T1SystemPrompt returns the Tier 1 (Haiku) system prompt with advisor context.
func T1SystemPrompt(docs *AdvisorDocs) string {
	return fmt.Sprintf(`%s

You are performing Tier 1 extraction: single-document fact extraction.
Focus on finding explicit, directly stated facts. Do not infer or synthesize across documents.

Advisor: %s (CRD %d)`, systemPrompt, docs.FirmName, docs.CRDNumber)
}

// T2SystemPrompt returns the Tier 2 (Sonnet) system prompt with cross-document context.
func T2SystemPrompt(docs *AdvisorDocs, t1Answers []Answer) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `%s

You are performing Tier 2 extraction: cross-document synthesis.
You may synthesize information across multiple document sections and types.
Consider the full context of the advisor's business when answering.

Advisor: %s (CRD %d)`, systemPrompt, docs.FirmName, docs.CRDNumber)

	// Include T1 context.
	if len(t1Answers) > 0 {
		sb.WriteString("\n\n--- Previously Extracted Facts (Tier 1) ---\n")
		for _, a := range t1Answers {
			if a.Value != nil {
				valJSON, _ := json.Marshal(a.Value)
				fmt.Fprintf(&sb, "- %s: %s (confidence: %.2f)\n", a.QuestionKey, string(valJSON), a.Confidence)
			}
		}
	}

	return sb.String()
}

// T3SystemPrompt returns the Tier 3 (Opus) system prompt for expert analysis.
func T3SystemPrompt(docs *AdvisorDocs, priorAnswers []Answer) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `%s

You are performing Tier 3 extraction: expert M&A judgment and analysis.
Provide deep analysis, trend assessment, and strategic insights.
You may make reasonable inferences based on the totality of available information.
Clearly distinguish between directly stated facts and your analytical conclusions.

Advisor: %s (CRD %d)`, systemPrompt, docs.FirmName, docs.CRDNumber)

	// Include prior answers as context.
	if len(priorAnswers) > 0 {
		sb.WriteString("\n\n--- Previously Extracted Data (Tiers 1-2) ---\n")
		for _, a := range priorAnswers {
			if a.Value != nil && a.Confidence >= 0.4 {
				valJSON, _ := json.Marshal(a.Value)
				fmt.Fprintf(&sb, "- %s: %s (confidence: %.2f)\n", a.QuestionKey, string(valJSON), a.Confidence)
			}
		}
	}

	return sb.String()
}

// BuildUserMessage constructs the user message for a single question.
func BuildUserMessage(q Question, docContext string) string {
	return fmt.Sprintf(`Question: %s

Document Context:
%s

Respond with ONLY valid JSON in this format:
{
  "value": <your answer — string, number, boolean, array, or object as appropriate>,
  "confidence": <0.0 to 1.0>,
  "reasoning": "<brief explanation of how you derived this answer>"
}`, q.Text, docContext)
}

// StructuredBypassAnswer generates an answer directly from Part 1 structured data
// without any LLM call. funds is the full list of private funds for computing
// aggregate fund metrics (total_fund_count, total_fund_gav).
func StructuredBypassAnswer(q Question, advisor *AdvisorRow, fund *FundRow, funds []FundRow) *Answer {
	if !q.StructuredBypass {
		return nil
	}

	a := &Answer{
		CRDNumber:     advisor.CRDNumber,
		QuestionKey:   q.Key,
		Confidence:    1.0,
		Tier:          0, // tier 0 = structured bypass
		SourceDoc:     "part1",
		SourceSection: "structured",
		Model:         "structured_bypass",
	}

	if fund != nil {
		a.FundID = fund.FundID
	}

	switch q.Key {
	case "aum_current":
		a.Value = map[string]any{
			"total":             advisor.AUMTotal,
			"discretionary":     advisor.AUMDiscretionary,
			"non_discretionary": advisor.AUMNonDiscretionary,
		}
		a.Reasoning = "Extracted directly from ADV Part 1 Item 5F"

	case "aum_discretionary_split":
		if advisor.AUMTotal != nil && *advisor.AUMTotal > 0 {
			discPct := float64(0)
			if advisor.AUMDiscretionary != nil {
				discPct = float64(*advisor.AUMDiscretionary) / float64(*advisor.AUMTotal) * 100
			}
			a.Value = map[string]any{
				"discretionary_pct":     round2(discPct),
				"non_discretionary_pct": round2(100 - discPct),
			}
			a.Reasoning = "Calculated from ADV Part 1 Item 5F AUM fields"
		} else {
			a.Value = nil
			a.Confidence = 0
			a.Reasoning = "No AUM data available in Part 1"
		}

	case "client_types_breakdown":
		if len(advisor.ClientTypes) > 0 && string(advisor.ClientTypes) != "null" {
			a.Value = json.RawMessage(advisor.ClientTypes)
			a.Reasoning = "Extracted directly from ADV Part 1 Item 5D client type data"
		} else {
			a.Value = nil
			a.Confidence = 0
		}

	case "hnw_concentration":
		a.Value = extractHNWConcentration(advisor)
		a.Reasoning = "Calculated from ADV Part 1 Item 5D HNW client categories"
		if a.Value == nil {
			a.Confidence = 0
		}

	case "institutional_vs_retail":
		a.Value = extractInstitutionalRetailMix(advisor)
		a.Reasoning = "Derived from ADV Part 1 Item 5D client type breakdown"
		if a.Value == nil {
			a.Confidence = 0
		}

	case "total_client_count":
		a.Value = advisor.NumAccounts
		a.Reasoning = "Extracted from ADV Part 1 number of accounts"
		if advisor.NumAccounts == nil {
			a.Confidence = 0
		}

	case "avg_account_size":
		if advisor.AUMTotal != nil && advisor.NumAccounts != nil && *advisor.NumAccounts > 0 {
			a.Value = *advisor.AUMTotal / int64(*advisor.NumAccounts)
			a.Reasoning = "Calculated: AUM total / number of accounts"
		} else {
			a.Value = nil
			a.Confidence = 0
		}

	case "total_headcount":
		a.Value = advisor.TotalEmployees
		a.Reasoning = "Extracted from ADV Part 1 employee count"
		if advisor.TotalEmployees == nil {
			a.Confidence = 0
		}

	case "compensation_types":
		a.Value = extractCompensationTypes(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 5E compensation flags"
		if a.Value == nil {
			a.Confidence = 0
		}

	case "regulatory_status":
		a.Value = extractRegulatoryStatus(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 2 registration fields"

	case "disciplinary_history":
		a.Value = extractDisciplinaryFlags(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 11 DRP flags"

	case "key_regulatory_registrations":
		a.Value = extractRegulatoryStatus(advisor)
		a.Reasoning = "Extracted from ADV Part 1 registration status fields"

	case "cross_trading_practices":
		a.Value = extractCrossTradingFlags(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 8 transaction flags"

	case "office_locations":
		a.Value = extractOfficeInfo(advisor)
		a.Reasoning = "Extracted from ADV Part 1 office data"

	// --- New v2 bypass questions ---

	case "aum_per_employee":
		if advisor.AUMTotal != nil && advisor.TotalEmployees != nil && *advisor.TotalEmployees > 0 {
			a.Value = *advisor.AUMTotal / int64(*advisor.TotalEmployees)
			a.Reasoning = "Calculated: AUM total / total employees"
		} else {
			a.Value = nil
			a.Confidence = 0
		}

	case "aum_per_adviser_rep":
		if advisor.AUMTotal != nil && advisor.Filing != nil {
			reps := 0
			if v, ok := advisor.Filing["num_adviser_reps"]; ok {
				switch rv := v.(type) {
				case float64:
					reps = int(rv)
				}
			}
			if reps > 0 {
				a.Value = *advisor.AUMTotal / int64(reps)
				a.Reasoning = "Calculated: AUM total / number of adviser reps"
			} else {
				a.Value = nil
				a.Confidence = 0
			}
		} else {
			a.Value = nil
			a.Confidence = 0
		}

	case "discretionary_pct":
		if advisor.AUMTotal != nil && *advisor.AUMTotal > 0 && advisor.AUMDiscretionary != nil {
			a.Value = round2(float64(*advisor.AUMDiscretionary) / float64(*advisor.AUMTotal) * 100)
			a.Reasoning = "Calculated: discretionary AUM / total AUM * 100"
		} else {
			a.Value = nil
			a.Confidence = 0
		}

	case "has_wrap_fee":
		if advisor.Filing != nil {
			a.Value = isTruthy(advisor.Filing["wrap_fee_program"])
			a.Reasoning = "Extracted from ADV Part 1 Item 5I wrap fee program flag"
		} else {
			a.Value = false
			a.Confidence = 0
		}

	case "wrap_fee_aum":
		if advisor.Filing != nil {
			if v, ok := advisor.Filing["wrap_fee_raum"]; ok {
				a.Value = v
				a.Reasoning = "Extracted from ADV Part 1 Item 5I wrap fee RAUM"
			} else {
				a.Value = nil
				a.Confidence = 0
			}
		} else {
			a.Value = nil
			a.Confidence = 0
		}

	case "total_fund_count":
		a.Value = len(funds)
		a.Reasoning = "Count of adv_private_funds records"

	case "total_fund_gav":
		var total int64
		for _, f := range funds {
			if f.GrossAssetValue != nil {
				total += *f.GrossAssetValue
			}
		}
		a.Value = total
		a.Reasoning = "Sum of gross_asset_value across all private funds"

	case "has_custody":
		if advisor.Filing != nil {
			a.Value = isTruthy(advisor.Filing["custody_client_cash"]) || isTruthy(advisor.Filing["custody_client_securities"])
			a.Reasoning = "Derived from ADV Part 1 Item 9 custody flags"
		} else {
			a.Value = false
			a.Confidence = 0
		}

	case "has_performance_fees":
		if advisor.Filing != nil {
			a.Value = isTruthy(advisor.Filing["comp_performance"])
			a.Reasoning = "Extracted from ADV Part 1 Item 5E compensation flags"
		} else {
			a.Value = false
			a.Confidence = 0
		}

	case "other_business_activities":
		a.Value = extractBizActivities(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 6A business activity flags"
		if a.Value == nil {
			a.Confidence = 0
		}

	case "financial_affiliations":
		a.Value = extractAffiliations(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 7A affiliation flags"
		if a.Value == nil {
			a.Confidence = 0
		}

	case "regulatory_change_of_control":
		a.Value = extractRegulatoryStatus(advisor)
		a.Reasoning = "Extracted from ADV Part 1 Item 2 registration fields"

	// Fund-level structured bypass
	case "fund_aum":
		if fund != nil {
			a.Value = map[string]any{
				"gross_asset_value": fund.GrossAssetValue,
				"net_asset_value":   fund.NetAssetValue,
			}
			a.Reasoning = "Extracted from ADV Part 1 Schedule D 7B1"
		}

	case "fund_type_detail":
		if fund != nil {
			a.Value = fund.FundType
			a.Reasoning = "Extracted from ADV Part 1 Schedule D 7B1 fund type"
		}

	case "fund_regulatory_status":
		if fund != nil {
			a.Value = map[string]any{
				"fund_type": fund.FundType,
				"fund_id":   fund.FundID,
			}
			a.Reasoning = "Extracted from ADV Part 1 Schedule D 7B1"
		}

	default:
		return nil
	}

	return a
}

// --- Structured bypass helpers ---

func extractHNWConcentration(a *AdvisorRow) any {
	if len(a.ClientTypes) == 0 || string(a.ClientTypes) == "null" {
		return nil
	}
	var clients []map[string]any
	if err := json.Unmarshal(a.ClientTypes, &clients); err != nil {
		return nil
	}

	var hnwCount, totalCount float64
	var hnwRAUM, totalRAUM float64
	for _, c := range clients {
		t, _ := c["type"].(string)
		count, _ := c["count"].(float64)
		pctRAUM, _ := c["pct_raum"].(float64)
		totalCount += count
		totalRAUM += pctRAUM
		tl := strings.ToLower(t)
		if strings.Contains(tl, "high net worth") || strings.Contains(tl, "hnw") {
			hnwCount += count
			hnwRAUM += pctRAUM
		}
	}

	if totalCount == 0 {
		return nil
	}
	return map[string]any{
		"hnw_client_pct":  round2(hnwCount / totalCount * 100),
		"hnw_raum_pct":    round2(hnwRAUM),
		"hnw_client_count": int(hnwCount),
	}
}

func extractInstitutionalRetailMix(a *AdvisorRow) any {
	if len(a.ClientTypes) == 0 || string(a.ClientTypes) == "null" {
		return nil
	}
	var clients []map[string]any
	if err := json.Unmarshal(a.ClientTypes, &clients); err != nil {
		return nil
	}

	var instCount, retailCount float64
	var instRAUM, retailRAUM float64
	institutionalTypes := map[string]bool{
		"pension": true, "endowment": true, "foundation": true,
		"corporation": true, "state": true, "municipal": true,
		"sovereign": true, "insurance": true, "investment company": true,
	}

	for _, c := range clients {
		t, _ := c["type"].(string)
		count, _ := c["count"].(float64)
		pctRAUM, _ := c["pct_raum"].(float64)

		isInst := false
		tl := strings.ToLower(t)
		for k := range institutionalTypes {
			if strings.Contains(tl, k) {
				isInst = true
				break
			}
		}

		if isInst {
			instCount += count
			instRAUM += pctRAUM
		} else {
			retailCount += count
			retailRAUM += pctRAUM
		}
	}

	total := instCount + retailCount
	if total == 0 {
		return nil
	}
	return map[string]any{
		"institutional_client_pct": round2(instCount / total * 100),
		"retail_client_pct":        round2(retailCount / total * 100),
		"institutional_raum_pct":   round2(instRAUM),
		"retail_raum_pct":          round2(retailRAUM),
	}
}

func extractCompensationTypes(a *AdvisorRow) any {
	if a.Filing == nil {
		return nil
	}
	types := []struct{ key, label string }{
		{"comp_pct_aum", "percent_of_aum"},
		{"comp_hourly", "hourly"},
		{"comp_subscription", "subscription"},
		{"comp_fixed", "fixed"},
		{"comp_commissions", "commissions"},
		{"comp_performance", "performance"},
		{"comp_other", "other"},
	}
	var active []string
	for _, t := range types {
		if v, ok := a.Filing[t.key]; ok && isTruthy(v) {
			active = append(active, t.label)
		}
	}
	if len(active) == 0 {
		return nil
	}
	return active
}

func extractRegulatoryStatus(a *AdvisorRow) any {
	if a.Filing == nil {
		return nil
	}
	result := make(map[string]bool)
	fields := []struct{ key, label string }{
		{"sec_registered", "sec_registered"},
		{"exempt_reporting", "exempt_reporting"},
		{"state_registered", "state_registered"},
	}
	for _, f := range fields {
		if v, ok := a.Filing[f.key]; ok {
			result[f.label] = isTruthy(v)
		}
	}
	return result
}

func extractDisciplinaryFlags(a *AdvisorRow) any {
	if a.Filing == nil {
		return nil
	}
	hasAny := isTruthy(a.Filing["has_any_drp"])
	result := map[string]any{
		"has_disciplinary_history": hasAny,
	}
	if hasAny {
		drpFields := []struct{ key, label string }{
			{"drp_criminal_firm", "criminal_firm"},
			{"drp_criminal_affiliate", "criminal_affiliate"},
			{"drp_regulatory_firm", "regulatory_firm"},
			{"drp_regulatory_affiliate", "regulatory_affiliate"},
			{"drp_civil_firm", "civil_firm"},
			{"drp_civil_affiliate", "civil_affiliate"},
			{"drp_complaint_firm", "complaint_firm"},
			{"drp_complaint_affiliate", "complaint_affiliate"},
		}
		flags := make(map[string]bool)
		for _, f := range drpFields {
			flags[f.label] = isTruthy(a.Filing[f.key])
		}
		result["flags"] = flags
	}
	return result
}

func extractCrossTradingFlags(a *AdvisorRow) any {
	if a.Filing == nil {
		return nil
	}
	return map[string]any{
		"agency_cross":        isTruthy(a.Filing["txn_agency_cross"]),
		"principal":           isTruthy(a.Filing["txn_principal"]),
		"proprietary_interest": isTruthy(a.Filing["txn_proprietary_interest"]),
		"referral_compensation": isTruthy(a.Filing["txn_referral_compensation"]),
		"revenue_sharing":     isTruthy(a.Filing["txn_revenue_sharing"]),
	}
}

func extractOfficeInfo(a *AdvisorRow) any {
	offices := map[string]any{
		"principal_city":  a.City,
		"principal_state": a.State,
	}
	if a.Filing != nil {
		if v, ok := a.Filing["num_other_offices"]; ok {
			offices["other_office_count"] = v
		}
	}
	return offices
}

func extractBizActivities(a *AdvisorRow) any {
	if a.Filing == nil {
		return nil
	}
	fields := []struct{ key, label string }{
		{"biz_broker_dealer", "broker_dealer"},
		{"biz_registered_rep", "registered_rep"},
		{"biz_cpo_cta", "cpo_cta"},
		{"biz_futures_commission", "futures_commission"},
		{"biz_real_estate", "real_estate"},
		{"biz_insurance", "insurance"},
		{"biz_bank", "bank"},
		{"biz_trust_company", "trust_company"},
		{"biz_municipal_advisor", "municipal_advisor"},
		{"biz_swap_dealer", "swap_dealer"},
		{"biz_major_swap", "major_swap"},
		{"biz_accountant", "accountant"},
		{"biz_lawyer", "lawyer"},
		{"biz_other_financial", "other_financial"},
	}
	var active []string
	for _, f := range fields {
		if v, ok := a.Filing[f.key]; ok && isTruthy(v) {
			active = append(active, f.label)
		}
	}
	if len(active) == 0 {
		return nil
	}
	return active
}

func extractAffiliations(a *AdvisorRow) any {
	if a.Filing == nil {
		return nil
	}
	fields := []struct{ key, label string }{
		{"aff_broker_dealer", "broker_dealer"},
		{"aff_other_adviser", "other_adviser"},
		{"aff_municipal_advisor", "municipal_advisor"},
		{"aff_swap_dealer", "swap_dealer"},
		{"aff_major_swap", "major_swap"},
		{"aff_cpo_cta", "cpo_cta"},
		{"aff_futures_commission", "futures_commission"},
		{"aff_bank", "bank"},
		{"aff_trust_company", "trust_company"},
		{"aff_accountant", "accountant"},
		{"aff_lawyer", "lawyer"},
		{"aff_insurance", "insurance"},
		{"aff_pension_consultant", "pension_consultant"},
		{"aff_real_estate", "real_estate"},
		{"aff_lp_sponsor", "lp_sponsor"},
		{"aff_pooled_vehicle", "pooled_vehicle"},
	}
	var active []string
	for _, f := range fields {
		if v, ok := a.Filing[f.key]; ok && isTruthy(v) {
			active = append(active, f.label)
		}
	}
	if len(active) == 0 {
		return nil
	}
	return active
}

func isTruthy(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return strings.EqualFold(val, "y") || strings.EqualFold(val, "true") || strings.EqualFold(val, "yes")
	case float64:
		return val != 0
	default:
		return v != nil
	}
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
