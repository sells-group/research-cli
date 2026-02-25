package scorer

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
)

// FirmScore holds the scoring result for a single firm.
type FirmScore struct {
	CRDNumber       int                 `json:"crd_number"`
	FirmName        string              `json:"firm_name"`
	State           string              `json:"state"`
	AUM             int64               `json:"aum"`
	Website         string              `json:"website"`
	Score           float64             `json:"score"`
	ComponentScores map[string]float64  `json:"component_scores"`
	MatchedKeywords map[string][]string `json:"matched_keywords,omitempty"`
	Passed          bool                `json:"passed"`
}

// ScoreFilters specifies criteria for bulk scoring.
type ScoreFilters struct {
	MinAUM   int64    `json:"min_aum,omitempty"`
	MaxAUM   int64    `json:"max_aum,omitempty"`
	States   []string `json:"states,omitempty"`
	MinScore float64  `json:"min_score,omitempty"`
	Limit    int      `json:"limit,omitempty"`
}

// scoringRow holds the raw database data used for scoring a single firm.
type scoringRow struct {
	CRDNumber    int
	FirmName     string
	State        string
	AUM          int64
	Website      string
	NumEmployees int

	// Service flags.
	SvcFinancialPlanning      bool
	SvcPortfolioIndividuals   bool
	SvcPortfolioPooled        bool
	SvcPortfolioInstitutional bool
	SvcPensionConsulting      bool
	SvcAdviserSelection       bool
	WrapFeeProgram            bool

	// Compensation.
	CompPctAUM      bool
	CompHourly      bool
	CompFixed       bool
	CompCommissions bool
	CompPerformance bool

	// DRP.
	HasAnyDRP         bool
	DRPCriminalFirm   bool
	DRPRegulatoryFirm bool

	// Client types (JSONB array of strings).
	ClientTypes []string

	// Computed metrics (nullable).
	RevenueEstimate      *int64
	AUM3YrCAGRPct        *float64
	HNWRevenuePct        *float64
	InstitutionalRevPct  *float64
	AcquisitionReadiness *int16

	// Document text for keyword search.
	BrochureText string
	CRSText      string
}

// ADVScorer scores firms based on ADV filing data.
type ADVScorer struct {
	pool *pgxpool.Pool
	cfg  config.ScorerConfig
}

// NewADVScorer creates an ADVScorer with the given connection pool and config.
func NewADVScorer(pool *pgxpool.Pool, cfg config.ScorerConfig) *ADVScorer {
	return &ADVScorer{pool: pool, cfg: cfg}
}

// Score performs bulk scoring of firms matching the given filters.
func (s *ADVScorer) Score(ctx context.Context, filters *ScoreFilters) ([]FirmScore, error) {
	rows, err := s.queryFirms(ctx, filters)
	if err != nil {
		return nil, eris.Wrap(err, "scorer: query firms")
	}

	var results []FirmScore
	minScore := s.cfg.MinScore
	if filters != nil && filters.MinScore > 0 {
		minScore = filters.MinScore
	}

	for _, row := range rows {
		score := computeScore(&row, s.cfg)
		if score.Score >= minScore {
			score.Passed = true
		}
		results = append(results, score)
	}

	// Sort by score descending (already ordered by AUM from SQL, re-sort by score).
	sortByScore(results)

	// Apply limit.
	limit := s.cfg.MaxFirms
	if filters != nil && filters.Limit > 0 {
		limit = filters.Limit
	}
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	zap.L().Info("scorer: bulk scoring complete",
		zap.Int("firms_scored", len(rows)),
		zap.Int("firms_passed", countPassed(results)),
	)

	return results, nil
}

// ScoreOne scores a single firm by CRD number.
func (s *ADVScorer) ScoreOne(ctx context.Context, crdNumber int) (*FirmScore, error) {
	row, err := s.queryOneFirm(ctx, crdNumber)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("scorer: query firm %d", crdNumber))
	}

	score := computeScore(row, s.cfg)
	if score.Score >= s.cfg.MinScore {
		score.Passed = true
	}

	zap.L().Info("scorer: scored firm",
		zap.Int("crd_number", crdNumber),
		zap.Float64("score", score.Score),
		zap.Bool("passed", score.Passed),
	)

	return &score, nil
}

// computeScore calculates the composite score from a scoring row and config.
// Exported via its lowercase name for internal use; tested via the test file.
func computeScore(row *scoringRow, cfg config.ScorerConfig) FirmScore {
	components := map[string]float64{
		"aum_fit":           scoreAUMFit(row.AUM, cfg.MinAUM, cfg.MaxAUM),
		"growth":            scoreGrowth(row.AUM3YrCAGRPct),
		"client_quality":    scoreClientQuality(row.HNWRevenuePct, row.InstitutionalRevPct, row.ClientTypes),
		"service_fit":       scoreServiceFit(row),
		"geo_match":         scoreGeoMatch(row.State, cfg.TargetStates, cfg.GeoKeywords, row.BrochureText),
		"industry_match":    scoreIndustryMatch(cfg.IndustryKeywords, row.BrochureText, row.CRSText),
		"regulatory_clean":  scoreRegulatoryClean(row.HasAnyDRP, row.DRPCriminalFirm, row.DRPRegulatoryFirm),
		"succession_signal": scoreSuccessionSignal(cfg.SuccessionKeywords, row.BrochureText, row.CRSText),
	}

	weights := map[string]float64{
		"aum_fit":           cfg.AUMFitWeight,
		"growth":            cfg.GrowthWeight,
		"client_quality":    cfg.ClientQualityWeight,
		"service_fit":       cfg.ServiceFitWeight,
		"geo_match":         cfg.GeoMatchWeight,
		"industry_match":    cfg.IndustryMatchWeight,
		"regulatory_clean":  cfg.RegulatoryCleanWeight,
		"succession_signal": cfg.SuccessionSignalWeight,
	}

	weightSum := WeightSum(cfg)
	var totalScore float64
	for k, component := range components {
		totalScore += component * weights[k]
	}

	// Normalize to 0-100 scale.
	if weightSum > 0 {
		totalScore = (totalScore / weightSum) * 100
	}

	// Apply negative keyword penalty.
	matchedKeywords := make(map[string][]string)
	negHits := matchKeywords(cfg.NegativeKeywords, row.BrochureText, row.CRSText)
	if len(negHits) > 0 {
		matchedKeywords["negative"] = negHits
		// Penalty: 10 points per negative keyword hit, capped at 50.
		penalty := math.Min(float64(len(negHits))*10, 50)
		totalScore = math.Max(0, totalScore-penalty)
	}

	// Collect matched keyword categories for transparency.
	if geoHits := matchKeywords(cfg.GeoKeywords, row.BrochureText, row.CRSText); len(geoHits) > 0 {
		matchedKeywords["geo"] = geoHits
	}
	if indHits := matchKeywords(cfg.IndustryKeywords, row.BrochureText, row.CRSText); len(indHits) > 0 {
		matchedKeywords["industry"] = indHits
	}
	if sucHits := matchKeywords(cfg.SuccessionKeywords, row.BrochureText, row.CRSText); len(sucHits) > 0 {
		matchedKeywords["succession"] = sucHits
	}

	fs := FirmScore{
		CRDNumber:       row.CRDNumber,
		FirmName:        row.FirmName,
		State:           row.State,
		AUM:             row.AUM,
		Website:         row.Website,
		Score:           math.Round(totalScore*100) / 100, // 2 decimal places
		ComponentScores: components,
	}
	if len(matchedKeywords) > 0 {
		fs.MatchedKeywords = matchedKeywords
	}
	return fs
}

// scoreAUMFit returns 0.0-1.0 based on how well the firm's AUM fits the target range.
func scoreAUMFit(aum, minAUM, maxAUM int64) float64 {
	if aum <= 0 {
		return 0
	}
	if maxAUM <= 0 {
		// No upper bound; score based on meeting minimum.
		if aum >= minAUM {
			return 1.0
		}
		return float64(aum) / float64(minAUM)
	}
	if aum >= minAUM && aum <= maxAUM {
		return 1.0
	}
	// Partial credit for being close to the range.
	if aum < minAUM {
		ratio := float64(aum) / float64(minAUM)
		return math.Max(0, ratio)
	}
	// Above max: gentle decay.
	ratio := float64(maxAUM) / float64(aum)
	return math.Max(0, ratio)
}

// scoreGrowth returns 0.0-1.0 based on 3-year AUM CAGR.
func scoreGrowth(cagr *float64) float64 {
	if cagr == nil {
		return 0.5 // neutral when data unavailable
	}
	v := *cagr
	switch {
	case v >= 20:
		return 1.0
	case v >= 10:
		return 0.8
	case v >= 5:
		return 0.6
	case v >= 0:
		return 0.4
	case v >= -5:
		return 0.2
	default:
		return 0.0
	}
}

// scoreClientQuality returns 0.0-1.0 based on HNW concentration and client mix.
func scoreClientQuality(hnwPct, instPct *float64, clientTypes []string) float64 {
	var score float64

	if hnwPct != nil {
		// HNW revenue percentage boosts quality.
		score += math.Min(*hnwPct/100, 1.0) * 0.5
	}
	if instPct != nil {
		// Institutional revenue diversifies.
		score += math.Min(*instPct/100, 1.0) * 0.3
	}
	// Client type diversity bonus.
	if len(clientTypes) > 0 {
		diversity := math.Min(float64(len(clientTypes))/5, 1.0)
		score += diversity * 0.2
	}

	return math.Min(score, 1.0)
}

// scoreServiceFit returns 0.0-1.0 based on service offerings relevant to M&A targets.
func scoreServiceFit(row *scoringRow) float64 {
	var hits int
	// Services that indicate a firm worth acquiring.
	if row.SvcFinancialPlanning {
		hits++
	}
	if row.SvcPortfolioIndividuals {
		hits++
	}
	if row.SvcPortfolioInstitutional {
		hits++
	}
	if row.SvcPensionConsulting {
		hits++
	}
	if row.WrapFeeProgram {
		hits++
	}

	// Fee-based compensation is a positive signal.
	if row.CompPctAUM {
		hits++
	}

	// Max possible hits = 6.
	return math.Min(float64(hits)/4.0, 1.0)
}

// scoreGeoMatch returns 0.0-1.0 based on geographic targeting.
func scoreGeoMatch(state string, targetStates, geoKeywords []string, brochureText string) float64 {
	if len(targetStates) == 0 && len(geoKeywords) == 0 {
		return 0.5 // neutral when no geo preference
	}

	var score float64

	// State match.
	if len(targetStates) > 0 {
		for _, ts := range targetStates {
			if strings.EqualFold(state, ts) {
				score = 1.0
				break
			}
		}
	}

	// Keyword match in brochure text.
	if score < 1.0 && len(geoKeywords) > 0 && brochureText != "" {
		hits := matchKeywords(geoKeywords, brochureText, "")
		if len(hits) > 0 {
			score = math.Max(score, 0.7)
		}
	}

	return score
}

// scoreIndustryMatch returns 0.0-1.0 based on industry keyword matches.
func scoreIndustryMatch(keywords []string, brochureText, crsText string) float64 {
	if len(keywords) == 0 {
		return 0.5 // neutral when no preference
	}
	hits := matchKeywords(keywords, brochureText, crsText)
	if len(hits) == 0 {
		return 0.0
	}
	// Scale by number of keyword hits, capped at 1.0.
	return math.Min(float64(len(hits))/float64(len(keywords)), 1.0)
}

// scoreRegulatoryClean returns 0.0-1.0 based on DRP history.
func scoreRegulatoryClean(hasAnyDRP, criminalFirm, regulatoryFirm bool) float64 {
	if !hasAnyDRP {
		return 1.0
	}
	if criminalFirm {
		return 0.0
	}
	if regulatoryFirm {
		return 0.3
	}
	// Other DRP (e.g., individual rep, not firm-level).
	return 0.6
}

// scoreSuccessionSignal returns 0.0-1.0 based on succession-related keywords.
func scoreSuccessionSignal(keywords []string, brochureText, crsText string) float64 {
	if len(keywords) == 0 {
		return 0.0
	}
	hits := matchKeywords(keywords, brochureText, crsText)
	if len(hits) == 0 {
		return 0.0
	}
	return math.Min(float64(len(hits))/2.0, 1.0)
}

// matchKeywords returns all keywords that appear (case-insensitive) in the given texts.
func matchKeywords(keywords []string, texts ...string) []string {
	var combined string
	for _, t := range texts {
		if t != "" {
			combined += " " + strings.ToLower(t)
		}
	}
	if combined == "" {
		return nil
	}

	var matched []string
	for _, kw := range keywords {
		if strings.Contains(combined, strings.ToLower(kw)) {
			matched = append(matched, kw)
		}
	}
	return matched
}

// sortByScore sorts FirmScores descending by Score.
func sortByScore(scores []FirmScore) {
	// Simple insertion sort is fine for typical result sizes (<1000).
	for i := 1; i < len(scores); i++ {
		for j := i; j > 0 && scores[j].Score > scores[j-1].Score; j-- {
			scores[j], scores[j-1] = scores[j-1], scores[j]
		}
	}
}

func countPassed(scores []FirmScore) int {
	n := 0
	for i := range scores {
		if scores[i].Passed {
			n++
		}
	}
	return n
}

// queryFirms fetches scoring data for firms matching the given filters.
func (s *ADVScorer) queryFirms(ctx context.Context, filters *ScoreFilters) ([]scoringRow, error) {
	// Build query with CTE joining mv_firm_combined + adv_computed_metrics + latest brochure + CRS.
	query := `
WITH latest_brochure AS (
    SELECT DISTINCT ON (crd_number) crd_number, text_content
    FROM fed_data.adv_brochures
    WHERE text_content IS NOT NULL
    ORDER BY crd_number, filing_date DESC
),
latest_crs AS (
    SELECT DISTINCT ON (crd_number) crd_number, text_content
    FROM fed_data.adv_crs
    WHERE text_content IS NOT NULL
    ORDER BY crd_number, filing_date DESC
)
SELECT
    fc.crd_number,
    COALESCE(fc.firm_name, ''),
    COALESCE(fc.state, ''),
    COALESCE(fc.aum, 0),
    COALESCE(fc.website, ''),
    COALESCE(fc.num_employees, 0),
    COALESCE(fc.svc_financial_planning, false),
    COALESCE(fc.svc_portfolio_individuals, false),
    COALESCE(fc.svc_portfolio_pooled, false),
    COALESCE(fc.svc_portfolio_institutional, false),
    COALESCE(fc.svc_pension_consulting, false),
    COALESCE(fc.svc_adviser_selection, false),
    COALESCE(fc.wrap_fee_program, false),
    COALESCE(fc.comp_pct_aum, false),
    COALESCE(fc.comp_hourly, false),
    COALESCE(fc.comp_fixed, false),
    COALESCE(fc.comp_commissions, false),
    COALESCE(fc.comp_performance, false),
    COALESCE(fc.has_any_drp, false),
    COALESCE(fc.drp_criminal_firm, false),
    COALESCE(fc.drp_regulatory_firm, false),
    fc.client_types,
    cm.revenue_estimate,
    cm.aum_3yr_cagr_pct,
    cm.hnw_revenue_pct,
    cm.institutional_revenue_pct,
    cm.acquisition_readiness,
    COALESCE(lb.text_content, ''),
    COALESCE(lc.text_content, '')
FROM fed_data.mv_firm_combined fc
LEFT JOIN fed_data.adv_computed_metrics cm ON cm.crd_number = fc.crd_number
LEFT JOIN latest_brochure lb ON lb.crd_number = fc.crd_number
LEFT JOIN latest_crs lc ON lc.crd_number = fc.crd_number
WHERE fc.aum > 0`

	var args []any
	argNum := 1

	if filters != nil {
		minAUM := filters.MinAUM
		if minAUM <= 0 {
			minAUM = s.cfg.MinAUM
		}
		if minAUM > 0 {
			query += fmt.Sprintf(" AND fc.aum >= $%d", argNum)
			args = append(args, minAUM)
			argNum++
		}

		maxAUM := filters.MaxAUM
		if maxAUM <= 0 {
			maxAUM = s.cfg.MaxAUM
		}
		if maxAUM > 0 {
			query += fmt.Sprintf(" AND fc.aum <= $%d", argNum)
			args = append(args, maxAUM)
			argNum++
		}

		states := filters.States
		if len(states) == 0 {
			states = s.cfg.TargetStates
		}
		if len(states) > 0 {
			query += fmt.Sprintf(" AND fc.state = ANY($%d)", argNum)
			args = append(args, states)
			argNum++
		}
	} else {
		if s.cfg.MinAUM > 0 {
			query += fmt.Sprintf(" AND fc.aum >= $%d", argNum)
			args = append(args, s.cfg.MinAUM)
			argNum++
		}
		if s.cfg.MaxAUM > 0 {
			query += fmt.Sprintf(" AND fc.aum <= $%d", argNum)
			args = append(args, s.cfg.MaxAUM)
			argNum++
		}
		if len(s.cfg.TargetStates) > 0 {
			query += fmt.Sprintf(" AND fc.state = ANY($%d)", argNum)
			args = append(args, s.cfg.TargetStates)
			argNum++
		}
	}

	query += " ORDER BY fc.aum DESC"

	// Suppress unused variable warning.
	_ = argNum

	pgxRows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "scorer: execute query")
	}
	defer pgxRows.Close()

	return scanScoringRows(pgxRows)
}

// queryOneFirm fetches scoring data for a single firm by CRD number.
func (s *ADVScorer) queryOneFirm(ctx context.Context, crdNumber int) (*scoringRow, error) {
	query := `
WITH latest_brochure AS (
    SELECT text_content
    FROM fed_data.adv_brochures
    WHERE crd_number = $1 AND text_content IS NOT NULL
    ORDER BY filing_date DESC
    LIMIT 1
),
latest_crs AS (
    SELECT text_content
    FROM fed_data.adv_crs
    WHERE crd_number = $1 AND text_content IS NOT NULL
    ORDER BY filing_date DESC
    LIMIT 1
)
SELECT
    fc.crd_number,
    COALESCE(fc.firm_name, ''),
    COALESCE(fc.state, ''),
    COALESCE(fc.aum, 0),
    COALESCE(fc.website, ''),
    COALESCE(fc.num_employees, 0),
    COALESCE(fc.svc_financial_planning, false),
    COALESCE(fc.svc_portfolio_individuals, false),
    COALESCE(fc.svc_portfolio_pooled, false),
    COALESCE(fc.svc_portfolio_institutional, false),
    COALESCE(fc.svc_pension_consulting, false),
    COALESCE(fc.svc_adviser_selection, false),
    COALESCE(fc.wrap_fee_program, false),
    COALESCE(fc.comp_pct_aum, false),
    COALESCE(fc.comp_hourly, false),
    COALESCE(fc.comp_fixed, false),
    COALESCE(fc.comp_commissions, false),
    COALESCE(fc.comp_performance, false),
    COALESCE(fc.has_any_drp, false),
    COALESCE(fc.drp_criminal_firm, false),
    COALESCE(fc.drp_regulatory_firm, false),
    fc.client_types,
    cm.revenue_estimate,
    cm.aum_3yr_cagr_pct,
    cm.hnw_revenue_pct,
    cm.institutional_revenue_pct,
    cm.acquisition_readiness,
    COALESCE((SELECT text_content FROM latest_brochure), ''),
    COALESCE((SELECT text_content FROM latest_crs), '')
FROM fed_data.mv_firm_combined fc
LEFT JOIN fed_data.adv_computed_metrics cm ON cm.crd_number = fc.crd_number
WHERE fc.crd_number = $1`

	row := s.pool.QueryRow(ctx, query, crdNumber)

	var sr scoringRow
	if err := scanOneRow(row, &sr); err != nil {
		if eris.Is(err, pgx.ErrNoRows) {
			return nil, eris.Errorf("scorer: firm %d not found", crdNumber)
		}
		return nil, eris.Wrap(err, fmt.Sprintf("scorer: scan firm %d", crdNumber))
	}

	return &sr, nil
}

// scanScoringRows scans pgx rows into scoringRow structs.
func scanScoringRows(rows pgx.Rows) ([]scoringRow, error) {
	var results []scoringRow
	for rows.Next() {
		var sr scoringRow
		err := rows.Scan(
			&sr.CRDNumber,
			&sr.FirmName,
			&sr.State,
			&sr.AUM,
			&sr.Website,
			&sr.NumEmployees,
			&sr.SvcFinancialPlanning,
			&sr.SvcPortfolioIndividuals,
			&sr.SvcPortfolioPooled,
			&sr.SvcPortfolioInstitutional,
			&sr.SvcPensionConsulting,
			&sr.SvcAdviserSelection,
			&sr.WrapFeeProgram,
			&sr.CompPctAUM,
			&sr.CompHourly,
			&sr.CompFixed,
			&sr.CompCommissions,
			&sr.CompPerformance,
			&sr.HasAnyDRP,
			&sr.DRPCriminalFirm,
			&sr.DRPRegulatoryFirm,
			&sr.ClientTypes,
			&sr.RevenueEstimate,
			&sr.AUM3YrCAGRPct,
			&sr.HNWRevenuePct,
			&sr.InstitutionalRevPct,
			&sr.AcquisitionReadiness,
			&sr.BrochureText,
			&sr.CRSText,
		)
		if err != nil {
			return nil, eris.Wrap(err, "scorer: scan row")
		}
		results = append(results, sr)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "scorer: iterate rows")
	}
	return results, nil
}

// scanOneRow scans a single pgx row into a scoringRow.
func scanOneRow(row pgx.Row, sr *scoringRow) error {
	return row.Scan(
		&sr.CRDNumber,
		&sr.FirmName,
		&sr.State,
		&sr.AUM,
		&sr.Website,
		&sr.NumEmployees,
		&sr.SvcFinancialPlanning,
		&sr.SvcPortfolioIndividuals,
		&sr.SvcPortfolioPooled,
		&sr.SvcPortfolioInstitutional,
		&sr.SvcPensionConsulting,
		&sr.SvcAdviserSelection,
		&sr.WrapFeeProgram,
		&sr.CompPctAUM,
		&sr.CompHourly,
		&sr.CompFixed,
		&sr.CompCommissions,
		&sr.CompPerformance,
		&sr.HasAnyDRP,
		&sr.DRPCriminalFirm,
		&sr.DRPRegulatoryFirm,
		&sr.ClientTypes,
		&sr.RevenueEstimate,
		&sr.AUM3YrCAGRPct,
		&sr.HNWRevenuePct,
		&sr.InstitutionalRevPct,
		&sr.AcquisitionReadiness,
		&sr.BrochureText,
		&sr.CRSText,
	)
}
