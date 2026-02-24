package advextract

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/sells-group/research-cli/internal/db"
)

// FeeTier represents one AUM breakpoint in a fee schedule.
type FeeTier struct {
	MinAUM        int64   `json:"min_aum"`
	MaxAUM        int64   `json:"max_aum"`         // 0 = no upper limit
	AnnualRatePct float64 `json:"annual_rate_pct"` // e.g. 1.00 = 1%
}

// GrowthMetrics holds historical growth rate calculations.
type GrowthMetrics struct {
	AUMGrowthCAGR      *float64 // compound annual growth rate (all-time)
	AUM1YrGrowth       *float64 // 1-year AUM growth rate
	AUM3YrCAGR         *float64 // 3-year CAGR
	AUM5YrCAGR         *float64 // 5-year CAGR
	Client3YrCAGR      *float64 // 3-year client CAGR
	Employee3YrCAGR    *float64 // 3-year employee CAGR
	ClientGrowthRate   *float64 // year-over-year
	EmployeeGrowthRate *float64 // year-over-year
}

// Scores holds computed scoring metrics.
type Scores struct {
	CompensationDiversity int // 1-7: count of active comp_* flags
	BusinessComplexity    int // count of active biz_* + aff_* flags
	DRPSeverity           int // 0-10 weighted score
	AcquisitionReadiness  int // 0-100 composite score
}

// ComputedMetrics holds all Go-derived metrics for one advisor.
type ComputedMetrics struct {
	CRDNumber               int
	RevenueEstimate         *int64
	BlendedFeeRateBPS       *int // basis points
	RevenuePerClient        *int
	AUMGrowthCAGR           *float64
	ClientGrowthRate        *float64
	EmployeeGrowthRate      *float64
	HNWRevenuePct           *float64
	InstitutionalRevenuePct *float64
	FundAUMPctTotal         *float64
	CompensationDiversity   int
	BusinessComplexity      int
	DRPSeverity             int
	AcquisitionReadiness    int

	// Windowed growth metrics (Gap 1.3)
	AUM1YrGrowth    *float64
	AUM3YrCAGR      *float64
	AUM5YrCAGR      *float64
	Client3YrCAGR   *float64
	Employee3YrCAGR *float64

	// Concentration risk (Gap 3.2)
	ConcentrationRiskScore *int // 0-10

	// Key-person dependency (Gap 4.2)
	KeyPersonDependencyScore *int // 0-10

	// Profitability estimates (Gap 7)
	HybridRevenueEstimate    *int64
	EstimatedExpenseRatio    *float64
	EstimatedOperatingMargin *float64
	RevenuePerEmployee       *int64

	// Benchmark percentiles (Gap 7.2)
	BenchmarkAUMPerEmployeePctile *float64
	BenchmarkFeeRatePctile        *float64

	// Amendment tracking (Gap 10.2)
	AmendmentsLastYear    *int
	AmendmentsPerYearAvg  *float64
	HasFrequentAmendments bool

	// Regulatory risk (Gap 8.3)
	RegulatoryRiskScore *int // 0-100
}

// ComputeRevenue applies fee schedule tiers to AUM and returns estimated annual revenue.
func ComputeRevenue(feeTiers []FeeTier, aum int64) int64 {
	if len(feeTiers) == 0 || aum <= 0 {
		return 0
	}

	// Sort tiers by MinAUM ascending.
	sorted := make([]FeeTier, len(feeTiers))
	copy(sorted, feeTiers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].MinAUM < sorted[j].MinAUM
	})

	var totalRevenue float64
	remaining := aum

	for _, tier := range sorted {
		if remaining <= 0 {
			break
		}

		// Determine the AUM portion in this tier.
		tierStart := tier.MinAUM
		if tierStart > aum {
			break
		}

		var tierAUM int64
		if tier.MaxAUM > 0 && tier.MaxAUM > tierStart {
			tierAUM = tier.MaxAUM - tierStart
		} else if tier.MaxAUM == 0 {
			// No upper limit — use all remaining.
			tierAUM = remaining
		} else {
			continue
		}

		// Cap at remaining AUM.
		if tierAUM > remaining {
			tierAUM = remaining
		}

		totalRevenue += float64(tierAUM) * (tier.AnnualRatePct / 100.0)
		remaining -= tierAUM
	}

	return int64(math.Round(totalRevenue))
}

// ComputeBlendedRate returns the blended fee rate in basis points.
func ComputeBlendedRate(feeTiers []FeeTier, aum int64) int {
	if aum <= 0 {
		return 0
	}
	revenue := ComputeRevenue(feeTiers, aum)
	if revenue == 0 {
		return 0
	}
	return int(math.Round(float64(revenue) / float64(aum) * 10000))
}

// ParseFeeTiers parses the fee_schedule_aum_tiers answer value into []FeeTier.
func ParseFeeTiers(value any) []FeeTier {
	if value == nil {
		return nil
	}

	// Handle JSON-deserialized []any where each element is a map.
	arr, ok := value.([]any)
	if !ok {
		// Try via JSON round-trip for other types.
		b, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		var tiers []FeeTier
		if err := json.Unmarshal(b, &tiers); err != nil {
			return nil
		}
		if len(tiers) == 0 {
			return nil
		}
		return tiers
	}

	if len(arr) == 0 {
		return nil
	}

	var tiers []FeeTier
	for _, elem := range arr {
		m, ok := elem.(map[string]any)
		if !ok {
			return nil
		}
		tier := FeeTier{
			MinAUM:        toInt64(m["min_aum"]),
			MaxAUM:        toInt64(m["max_aum"]),
			AnnualRatePct: toFloat64(m["annual_rate_pct"]),
		}
		tiers = append(tiers, tier)
	}

	return tiers
}

// toInt64 converts a numeric any value to int64.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

// toFloat64 converts a numeric any value to float64.
func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// filingRow holds a single historical filing data point.
type filingRow struct {
	FilingDate     time.Time
	AUMTotal       *int64
	NumAccounts    *int
	TotalEmployees *int
}

// computeCAGR calculates compound annual growth rate.
func computeCAGR(startVal, endVal float64, years float64) *float64 {
	if startVal <= 0 || endVal <= 0 || years < 0.5 {
		return nil
	}
	ratio := endVal / startVal
	cagr := math.Pow(ratio, 1.0/years) - 1.0
	return &cagr
}

// findFilingNearDate finds the filing closest to a target date from a sorted slice.
func findFilingNearDate(filings []filingRow, target time.Time) *filingRow {
	if len(filings) == 0 {
		return nil
	}
	best := &filings[0]
	bestDiff := absDuration(filings[0].FilingDate.Sub(target))
	for i := 1; i < len(filings); i++ {
		diff := absDuration(filings[i].FilingDate.Sub(target))
		if diff < bestDiff {
			best = &filings[i]
			bestDiff = diff
		}
	}
	// Only use if within 6 months of target.
	if bestDiff > 182*24*time.Hour {
		return nil
	}
	return best
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// ComputeGrowthRates queries historical filings and computes growth metrics.
func ComputeGrowthRates(ctx context.Context, pool db.Pool, crd int) (*GrowthMetrics, error) {
	query := `SELECT filing_date, aum_total, num_accounts, total_employees
		FROM fed_data.adv_filings
		WHERE crd_number = $1 AND filing_date IS NOT NULL
		ORDER BY filing_date ASC`

	rows, err := pool.Query(ctx, query, crd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var filings []filingRow
	for rows.Next() {
		var f filingRow
		if err := rows.Scan(&f.FilingDate, &f.AUMTotal, &f.NumAccounts, &f.TotalEmployees); err != nil {
			return nil, err
		}
		filings = append(filings, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(filings) < 2 {
		return &GrowthMetrics{}, nil
	}

	gm := &GrowthMetrics{}
	latest := filings[len(filings)-1]
	earliest := filings[0]

	// All-time CAGR for AUM.
	if earliest.AUMTotal != nil && latest.AUMTotal != nil && *earliest.AUMTotal > 0 && *latest.AUMTotal > 0 {
		years := latest.FilingDate.Sub(earliest.FilingDate).Hours() / (365.25 * 24)
		gm.AUMGrowthCAGR = computeCAGR(float64(*earliest.AUMTotal), float64(*latest.AUMTotal), years)
	}

	now := latest.FilingDate

	// 1-year AUM growth.
	if f1yr := findFilingNearDate(filings, now.AddDate(-1, 0, 0)); f1yr != nil {
		if f1yr.AUMTotal != nil && latest.AUMTotal != nil && *f1yr.AUMTotal > 0 {
			rate := (float64(*latest.AUMTotal) - float64(*f1yr.AUMTotal)) / float64(*f1yr.AUMTotal)
			gm.AUM1YrGrowth = &rate
		}
	}

	// 3-year AUM CAGR.
	if f3yr := findFilingNearDate(filings, now.AddDate(-3, 0, 0)); f3yr != nil {
		if f3yr.AUMTotal != nil && latest.AUMTotal != nil && *f3yr.AUMTotal > 0 {
			years := now.Sub(f3yr.FilingDate).Hours() / (365.25 * 24)
			gm.AUM3YrCAGR = computeCAGR(float64(*f3yr.AUMTotal), float64(*latest.AUMTotal), years)
		}
	}

	// 5-year AUM CAGR.
	if f5yr := findFilingNearDate(filings, now.AddDate(-5, 0, 0)); f5yr != nil {
		if f5yr.AUMTotal != nil && latest.AUMTotal != nil && *f5yr.AUMTotal > 0 {
			years := now.Sub(f5yr.FilingDate).Hours() / (365.25 * 24)
			gm.AUM5YrCAGR = computeCAGR(float64(*f5yr.AUMTotal), float64(*latest.AUMTotal), years)
		}
	}

	// 3-year client CAGR.
	if f3yr := findFilingNearDate(filings, now.AddDate(-3, 0, 0)); f3yr != nil {
		if f3yr.NumAccounts != nil && latest.NumAccounts != nil && *f3yr.NumAccounts > 0 {
			years := now.Sub(f3yr.FilingDate).Hours() / (365.25 * 24)
			gm.Client3YrCAGR = computeCAGR(float64(*f3yr.NumAccounts), float64(*latest.NumAccounts), years)
		}
	}

	// 3-year employee CAGR.
	if f3yr := findFilingNearDate(filings, now.AddDate(-3, 0, 0)); f3yr != nil {
		if f3yr.TotalEmployees != nil && latest.TotalEmployees != nil && *f3yr.TotalEmployees > 0 {
			years := now.Sub(f3yr.FilingDate).Hours() / (365.25 * 24)
			gm.Employee3YrCAGR = computeCAGR(float64(*f3yr.TotalEmployees), float64(*latest.TotalEmployees), years)
		}
	}

	// YoY growth for clients: use most recent two filings.
	prev := filings[len(filings)-2]
	if prev.NumAccounts != nil && latest.NumAccounts != nil && *prev.NumAccounts > 0 {
		rate := (float64(*latest.NumAccounts) - float64(*prev.NumAccounts)) / float64(*prev.NumAccounts)
		gm.ClientGrowthRate = &rate
	}

	// YoY growth for employees: use most recent two filings.
	if prev.TotalEmployees != nil && latest.TotalEmployees != nil && *prev.TotalEmployees > 0 {
		rate := (float64(*latest.TotalEmployees) - float64(*prev.TotalEmployees)) / float64(*prev.TotalEmployees)
		gm.EmployeeGrowthRate = &rate
	}

	return gm, nil
}

// comp flags checked for compensation diversity.
var compFlags = []string{
	"comp_pct_aum", "comp_hourly", "comp_subscription",
	"comp_fixed", "comp_commissions", "comp_performance", "comp_other",
}

// biz flags checked for business complexity.
var bizFlags = []string{
	"biz_broker_dealer", "biz_registered_rep", "biz_cpo_cta",
	"biz_futures_commission", "biz_real_estate", "biz_insurance",
	"biz_bank", "biz_trust_company", "biz_municipal_advisor",
	"biz_swap_dealer", "biz_major_swap", "biz_accountant",
	"biz_lawyer", "biz_other_financial",
}

// aff flags checked for business complexity.
var affFlags = []string{
	"aff_broker_dealer", "aff_other_adviser", "aff_municipal_advisor",
	"aff_swap_dealer", "aff_major_swap", "aff_cpo_cta",
	"aff_futures_commission", "aff_bank", "aff_trust_company",
	"aff_accountant", "aff_lawyer", "aff_insurance",
	"aff_pension_consultant", "aff_real_estate", "aff_lp_sponsor",
	"aff_pooled_vehicle",
}

// drp weights for DRP severity scoring.
var drpWeights = map[string]int{
	"drp_criminal_firm":         3,
	"drp_criminal_affiliate":    3,
	"drp_regulatory_firm":       2,
	"drp_regulatory_affiliate":  2,
	"drp_civil_firm":            2,
	"drp_civil_affiliate":       2,
	"drp_complaint_firm":        1,
	"drp_complaint_affiliate":   1,
	"drp_termination_firm":      1,
	"drp_termination_affiliate": 1,
	"drp_judgment":              2,
	"drp_financial_firm":        1,
	"drp_financial_affiliate":   1,
}

// ComputeScores computes scoring metrics from filing data and extracted answers.
func ComputeScores(filing map[string]any, answers map[string]Answer) *Scores {
	s := &Scores{}

	if filing != nil {
		// CompensationDiversity: count of truthy comp_* flags (max 7).
		for _, key := range compFlags {
			if v, ok := filing[key]; ok && isTruthy(v) {
				s.CompensationDiversity++
			}
		}

		// BusinessComplexity: count of truthy biz_* + aff_* flags (cap 30).
		for _, key := range bizFlags {
			if v, ok := filing[key]; ok && isTruthy(v) {
				s.BusinessComplexity++
			}
		}
		for _, key := range affFlags {
			if v, ok := filing[key]; ok && isTruthy(v) {
				s.BusinessComplexity++
			}
		}
		if s.BusinessComplexity > 30 {
			s.BusinessComplexity = 30
		}

		// DRPSeverity: weighted scoring (cap 10).
		for key, weight := range drpWeights {
			if v, ok := filing[key]; ok && isTruthy(v) {
				s.DRPSeverity += weight
			}
		}
		if s.DRPSeverity > 10 {
			s.DRPSeverity = 10
		}
	}

	// AcquisitionReadiness: 0-100 composite (always computed).
	s.AcquisitionReadiness = computeAcquisitionReadiness(s, answers)

	return s
}

// computeAcquisitionReadiness calculates the 0-100 composite score.
func computeAcquisitionReadiness(s *Scores, answers map[string]Answer) int {
	score := 50 // neutral start

	// +15 if revenue estimate > 0 (has fee schedule).
	if a, ok := answers["fee_schedule_complete"]; ok {
		tiers := ParseFeeTiers(a.Value)
		if len(tiers) > 0 {
			score += 15
		}
	}

	// +10 if AUM growth CAGR > 5%.
	if a, ok := answers["aum_growth_trend"]; ok && a.Value != nil {
		if f := toFloat64(a.Value); f > 0.05 {
			score += 10
		}
	}

	// +5 if client growth > 0%.
	if a, ok := answers["client_account_growth"]; ok && a.Value != nil {
		if f := toFloat64(a.Value); f > 0 {
			score += 5
		}
	}

	// -10 if DRP severity > 3.
	if s.DRPSeverity > 3 {
		score -= 10
	}

	// +10 if succession_plan_disclosed answer value is true.
	if a, ok := answers["succession_plan"]; ok && isTruthy(a.Value) {
		score += 10
	}

	// +5 if has_code_of_ethics answer value is true.
	if a, ok := answers["code_of_ethics_summary"]; ok && isTruthy(a.Value) {
		score += 5
	}

	// -5 if business_complexity > 15.
	if s.BusinessComplexity > 15 {
		score -= 5
	}

	// +5 if compensation_diversity >= 3.
	if s.CompensationDiversity >= 3 {
		score += 5
	}

	// +5 if best answer confidence avg > 0.7.
	if len(answers) > 0 {
		var totalConf float64
		var count int
		for _, a := range answers {
			if a.Confidence > 0 {
				totalConf += a.Confidence
				count++
			}
		}
		if count > 0 && (totalConf/float64(count)) > 0.7 {
			score += 5
		}
	}

	// -5 if frequent amendments (Gap 10.3).
	if a, ok := answers["has_frequent_amendments"]; ok && isTruthy(a.Value) {
		score -= 5
	}

	// Clamp to 0-100.
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score
}

// ComputeConcentrationRisk computes client concentration risk score (0-10).
func ComputeConcentrationRisk(answers map[string]Answer) *int {
	var hhi, topClient, retention float64
	hasData := false

	// Top client % of AUM (from extraction).
	if a, ok := answers["client_largest_pct_aum"]; ok && a.Value != nil {
		topClient = toFloat64(a.Value)
		hasData = true
	}

	// Top 5 client % of AUM.
	if a, ok := answers["client_top5_pct_aum"]; ok && a.Value != nil {
		top5 := toFloat64(a.Value)
		if top5 > 0 {
			// Simple HHI proxy: assume equal shares among top 5.
			hhi = top5 * top5 / 5.0
			hasData = true
		}
	}

	// Client retention rate.
	if a, ok := answers["client_retention_rate"]; ok && a.Value != nil {
		retention = toFloat64(a.Value)
		hasData = true
	}

	if !hasData {
		return nil
	}

	// Weighted score: HHI 40% + topClient 30% + (1-retention) 30%.
	hhiScore := math.Min(hhi/100.0, 1.0) * 10.0 * 0.4
	topScore := math.Min(topClient/50.0, 1.0) * 10.0 * 0.3
	retScore := 0.0
	if retention > 0 {
		retScore = math.Max(0, (1.0-retention/100.0)) * 10.0 * 0.3
	}

	score := int(math.Round(hhiScore + topScore + retScore))
	if score > 10 {
		score = 10
	}
	return &score
}

// ComputeKeyPersonDependency computes key-person dependency score (0-10, higher = more risk).
func ComputeKeyPersonDependency(answers map[string]Answer, owners []OwnerRow) *int {
	score := 0.0

	// Single owner = high risk.
	if len(owners) == 1 {
		score += 3
	} else if len(owners) <= 2 {
		score += 2
	}

	// No succession plan.
	if a, ok := answers["succession_plan"]; !ok || !isTruthy(a.Value) {
		score += 2
	}
	if a, ok := answers["personnel_succession_identified"]; ok && !isTruthy(a.Value) {
		score++
	}

	// No non-compete.
	if a, ok := answers["personnel_non_compete_exists"]; ok && !isTruthy(a.Value) {
		score++
	}

	// Key person handles high % AUM.
	if a, ok := answers["personnel_key_person_pct_aum"]; ok && a.Value != nil {
		pct := toFloat64(a.Value)
		if pct > 50 {
			score += 2
		} else if pct > 25 {
			score++
		}
	}

	// No equity incentives (less retention).
	if a, ok := answers["personnel_has_equity_incentives"]; ok && !isTruthy(a.Value) {
		score++
	}

	result := int(math.Min(score, 10))
	return &result
}

// ComputeHybridRevenue estimates revenue from multiple sources (AUM fees + flat fees + wrap fees).
func ComputeHybridRevenue(feeTiers []FeeTier, aum int64, answers map[string]Answer) *int64 {
	var total float64

	// AUM-based fee revenue.
	if len(feeTiers) > 0 && aum > 0 {
		total += float64(ComputeRevenue(feeTiers, aum))
	}

	// Flat fee estimate: parse range midpoint * estimated client count.
	if a, ok := answers["fixed_fee_range"]; ok && a.Value != nil {
		midpoint := extractFeeRangeMidpoint(a.Value)
		if midpoint > 0 {
			// Rough estimate of financial planning clients (10% of total accounts).
			clientCount := 50 // default
			if ac, ok2 := answers["client_count"]; ok2 && ac.Value != nil {
				if c := toFloat64(ac.Value); c > 0 {
					clientCount = int(c * 0.1)
					if clientCount < 5 {
						clientCount = 5
					}
				}
			}
			total += midpoint * float64(clientCount)
		}
	}

	// Wrap fee overlay: 0.25% on wrap fee AUM.
	if a, ok := answers["wrap_fee_raum"]; ok && a.Value != nil {
		wrapAUM := toInt64(a.Value)
		if wrapAUM > 0 {
			total += float64(wrapAUM) * 0.0025
		}
	}

	if total <= 0 {
		return nil
	}
	rev := int64(math.Round(total))
	return &rev
}

// extractFeeRangeMidpoint parses a fee range and returns the midpoint.
func extractFeeRangeMidpoint(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case map[string]any:
		low := toFloat64(v["low"])
		high := toFloat64(v["high"])
		if low > 0 && high > 0 {
			return (low + high) / 2
		}
		if low > 0 {
			return low
		}
		return high
	}
	return 0
}

// ComputeProfitabilityIndicators estimates expense ratio and operating margin.
func ComputeProfitabilityIndicators(revenueEstimate int64, totalEmployees *int, numAdviserReps *int) (expenseRatio, operatingMargin *float64, revenuePerEmployee *int64) {
	if revenueEstimate <= 0 || totalEmployees == nil || *totalEmployees <= 0 {
		return nil, nil, nil
	}

	// Industry averages: $150K support staff, $250K advisors.
	supportCount := *totalEmployees
	advisorCount := 0
	if numAdviserReps != nil {
		advisorCount = *numAdviserReps
		supportCount = *totalEmployees - advisorCount
		if supportCount < 0 {
			supportCount = 0
		}
	}

	estimatedExpenses := float64(supportCount)*150000.0 + float64(advisorCount)*250000.0
	if estimatedExpenses <= 0 {
		estimatedExpenses = float64(*totalEmployees) * 175000.0
	}

	er := estimatedExpenses / float64(revenueEstimate)
	expenseRatio = &er

	om := 1.0 - er
	operatingMargin = &om

	rpe := revenueEstimate / int64(*totalEmployees)
	revenuePerEmployee = &rpe

	return
}

// ComputeAmendmentMetrics queries filing amendment frequency.
func ComputeAmendmentMetrics(ctx context.Context, pool db.Pool, crd int) (lastYear *int, perYearAvg *float64, frequent bool, err error) {
	query := `SELECT filing_date, filing_type
		FROM fed_data.adv_filings
		WHERE crd_number = $1 AND filing_date IS NOT NULL
		ORDER BY filing_date ASC`

	rows, err := pool.Query(ctx, query, crd)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()

	var amendmentDates []time.Time
	var allDates []time.Time
	for rows.Next() {
		var filingDate time.Time
		var filingType *string
		if err := rows.Scan(&filingDate, &filingType); err != nil {
			return nil, nil, false, err
		}
		allDates = append(allDates, filingDate)
		if filingType != nil && strings.EqualFold(*filingType, "amendment") {
			amendmentDates = append(amendmentDates, filingDate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, err
	}

	if len(allDates) == 0 {
		return nil, nil, false, nil
	}

	// Count amendments in the last year.
	oneYearAgo := time.Now().AddDate(-1, 0, 0)
	count := 0
	for _, d := range amendmentDates {
		if d.After(oneYearAgo) {
			count++
		}
	}
	lastYear = &count

	// Average per year.
	if len(allDates) >= 2 {
		earliest := allDates[0]
		latest := allDates[len(allDates)-1]
		years := latest.Sub(earliest).Hours() / (365.25 * 24)
		if years >= 0.5 {
			avg := float64(len(amendmentDates)) / years
			perYearAvg = &avg
			frequent = avg > 3.0
		}
	}

	return lastYear, perYearAvg, frequent, nil
}

// ComputeRegulatoryRiskScore computes a comprehensive regulatory risk score (0-100).
func ComputeRegulatoryRiskScore(ctx context.Context, pool db.Pool, crd int, drpSeverity int, amendmentsPerYear *float64) (*int, error) {
	// DRP severity component: 40% weight (scale 0-10 → 0-40).
	drpComponent := float64(drpSeverity) * 4.0

	// SEC enforcement component: 30% weight.
	var enforcementCount int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fed_data.sec_enforcement_actions WHERE crd_number = $1`, crd).Scan(&enforcementCount)
	if err != nil {
		enforcementCount = 0
	}
	enforcementComponent := math.Min(float64(enforcementCount)*10.0, 30.0)

	// BrokerCheck disclosures: 20% weight.
	var disclosureCount int
	err = pool.QueryRow(ctx,
		`SELECT COALESCE(disclosure_count, 0) FROM fed_data.brokercheck WHERE crd_number = $1`, crd).Scan(&disclosureCount)
	if err != nil {
		disclosureCount = 0
	}
	disclosureComponent := math.Min(float64(disclosureCount)*5.0, 20.0)

	// Amendment frequency: 10% weight.
	amendmentComponent := 0.0
	if amendmentsPerYear != nil && *amendmentsPerYear > 3.0 {
		amendmentComponent = math.Min((*amendmentsPerYear-3.0)*5.0, 10.0)
	}

	total := int(math.Round(drpComponent + enforcementComponent + disclosureComponent + amendmentComponent))
	if total > 100 {
		total = 100
	}
	return &total, nil
}

// ComputeAllMetrics orchestrates all metric computations for one advisor.
func ComputeAllMetrics(ctx context.Context, pool db.Pool, crd int, advisor *AdvisorRow, answers []Answer) (*ComputedMetrics, error) {
	cm := &ComputedMetrics{
		CRDNumber: crd,
	}

	// Build answer map from answers slice.
	answerMap := make(map[string]Answer, len(answers))
	for _, a := range answers {
		// Keep the best answer per key (highest tier, then highest confidence).
		if existing, ok := answerMap[a.QuestionKey]; ok {
			if a.Tier > existing.Tier || (a.Tier == existing.Tier && a.Confidence > existing.Confidence) {
				answerMap[a.QuestionKey] = a
			}
		} else {
			answerMap[a.QuestionKey] = a
		}
	}

	// Parse fee tiers from answers.
	var feeTiers []FeeTier
	if a, ok := answerMap["fee_schedule_complete"]; ok {
		feeTiers = ParseFeeTiers(a.Value)
	}

	// Compute revenue if fee tiers and AUM available.
	if len(feeTiers) > 0 && advisor != nil && advisor.AUMTotal != nil && *advisor.AUMTotal > 0 {
		rev := ComputeRevenue(feeTiers, *advisor.AUMTotal)
		cm.RevenueEstimate = &rev

		// Compute blended rate.
		bps := ComputeBlendedRate(feeTiers, *advisor.AUMTotal)
		cm.BlendedFeeRateBPS = &bps

		// Compute revenue per client.
		if advisor.NumAccounts != nil && *advisor.NumAccounts > 0 {
			rpc := int(rev / int64(*advisor.NumAccounts))
			cm.RevenuePerClient = &rpc
		}
	}

	// Query growth rates.
	gm, err := ComputeGrowthRates(ctx, pool, crd)
	if err != nil {
		return nil, err
	}
	if gm != nil {
		cm.AUMGrowthCAGR = gm.AUMGrowthCAGR
		cm.ClientGrowthRate = gm.ClientGrowthRate
		cm.EmployeeGrowthRate = gm.EmployeeGrowthRate
		cm.AUM1YrGrowth = gm.AUM1YrGrowth
		cm.AUM3YrCAGR = gm.AUM3YrCAGR
		cm.AUM5YrCAGR = gm.AUM5YrCAGR
		cm.Client3YrCAGR = gm.Client3YrCAGR
		cm.Employee3YrCAGR = gm.Employee3YrCAGR
	}

	// Compute scores.
	var filing map[string]any
	if advisor != nil {
		filing = advisor.Filing
	}
	scores := ComputeScores(filing, answerMap)
	cm.CompensationDiversity = scores.CompensationDiversity
	cm.BusinessComplexity = scores.BusinessComplexity
	cm.DRPSeverity = scores.DRPSeverity

	// Compute concentration risk (Gap 3.2).
	cm.ConcentrationRiskScore = ComputeConcentrationRisk(answerMap)

	// Compute key-person dependency (Gap 4.2).
	owners, _ := NewStore(pool).LoadOwners(ctx, crd)
	cm.KeyPersonDependencyScore = ComputeKeyPersonDependency(answerMap, owners)

	// Compute hybrid revenue (Gap 7.1).
	var feeTiersForHybrid []FeeTier
	if a, ok := answerMap["fee_schedule_complete"]; ok {
		feeTiersForHybrid = ParseFeeTiers(a.Value)
	}
	var aumForHybrid int64
	if advisor != nil && advisor.AUMTotal != nil {
		aumForHybrid = *advisor.AUMTotal
	}
	cm.HybridRevenueEstimate = ComputeHybridRevenue(feeTiersForHybrid, aumForHybrid, answerMap)

	// Compute profitability (Gap 7.3).
	bestRevenue := int64(0)
	if cm.HybridRevenueEstimate != nil && *cm.HybridRevenueEstimate > 0 {
		bestRevenue = *cm.HybridRevenueEstimate
	} else if cm.RevenueEstimate != nil {
		bestRevenue = *cm.RevenueEstimate
	}
	if bestRevenue > 0 && advisor != nil {
		var numAdviserReps *int
		if filing != nil {
			if v, ok := filing["num_adviser_reps"]; ok {
				n := int(toInt64(v))
				if n > 0 {
					numAdviserReps = &n
				}
			}
		}
		cm.EstimatedExpenseRatio, cm.EstimatedOperatingMargin, cm.RevenuePerEmployee =
			ComputeProfitabilityIndicators(bestRevenue, advisor.TotalEmployees, numAdviserReps)
	}

	// Compute amendment metrics (Gap 10.2).
	amendLastYear, amendAvg, amendFrequent, amendErr := ComputeAmendmentMetrics(ctx, pool, crd)
	if amendErr == nil {
		cm.AmendmentsLastYear = amendLastYear
		cm.AmendmentsPerYearAvg = amendAvg
		cm.HasFrequentAmendments = amendFrequent
	}

	// Compute regulatory risk score (Gap 8.3).
	regScore, regErr := ComputeRegulatoryRiskScore(ctx, pool, crd, scores.DRPSeverity, amendAvg)
	if regErr == nil {
		cm.RegulatoryRiskScore = regScore
	}

	// Acquisition readiness uses all computed data (includes amendment + regulatory factors).
	// Inject amendment data into answers for scoring.
	if amendFrequent {
		answerMap["has_frequent_amendments"] = Answer{Value: true}
	}
	scores.AcquisitionReadiness = computeAcquisitionReadiness(scores, answerMap)
	cm.AcquisitionReadiness = scores.AcquisitionReadiness

	// Compute fund AUM % of total.
	if advisor != nil && advisor.AUMTotal != nil && *advisor.AUMTotal > 0 {
		if a, ok := answerMap["fund_aum"]; ok && a.Value != nil {
			if fundGAV := extractFundGAVTotal(a.Value); fundGAV > 0 {
				pct := float64(fundGAV) / float64(*advisor.AUMTotal) * 100.0
				cm.FundAUMPctTotal = &pct
			}
		}
	}

	// Compute HNW revenue % (simplified from hnw_concentration bypass data).
	if a, ok := answerMap["hnw_concentration"]; ok && a.Value != nil {
		if pct := extractPercentage(a.Value); pct > 0 {
			cm.HNWRevenuePct = &pct
		}
	}

	// Compute institutional revenue %.
	if a, ok := answerMap["institutional_vs_retail"]; ok && a.Value != nil {
		if pct := extractPercentage(a.Value); pct > 0 {
			cm.InstitutionalRevenuePct = &pct
		}
	}

	return cm, nil
}

// extractFundGAVTotal attempts to extract a total GAV from a fund_aum answer value.
func extractFundGAVTotal(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case map[string]any:
		// Try common keys for GAV total.
		for _, key := range []string{"gross_asset_value", "gav", "total_gav", "total"} {
			if val, ok := v[key]; ok {
				return toInt64(val)
			}
		}
	}
	return 0
}

// extractPercentage attempts to extract a percentage from an answer value.
func extractPercentage(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case map[string]any:
		// Try common keys for percentage.
		for _, key := range []string{"pct", "percentage", "percent", "pct_aum", "pct_clients"} {
			if val, ok := v[key]; ok {
				return toFloat64(val)
			}
		}
	case string:
		// Try parsing percentage strings like "45.5%".
		s := strings.TrimSuffix(strings.TrimSpace(v), "%")
		if f := toFloat64(s); f > 0 {
			return f
		}
	}
	return 0
}
