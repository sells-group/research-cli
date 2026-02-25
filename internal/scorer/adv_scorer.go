package scorer

import (
	"context"
	"encoding/json"
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

// msaEntry represents a single MSA association for a firm (from address_msa).
type msaEntry struct {
	CBSACode       string  `json:"cbsa_code"`
	CentroidKM     float64 `json:"centroid_km"`
	EdgeKM         float64 `json:"edge_km"`
	Classification string  `json:"classification"`
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

	// Geo fields (nullable — from LEFT JOIN v_firm_msa).
	Latitude       *float64
	Longitude      *float64
	CBSACode       *string
	MSAName        *string
	IsWithin       *bool
	CentroidKM     *float64
	EdgeKM         *float64
	Classification *string

	// AllMSAs holds all MSA associations (from JSONB aggregate).
	AllMSAs []msaEntry
}

// msaCentroid holds the centroid lat/lon for an MSA (used for proximity scoring).
type msaCentroid struct {
	CBSACode  string
	Latitude  float64
	Longitude float64
}

// ADVScorer scores firms based on ADV filing data.
type ADVScorer struct {
	pool              *pgxpool.Pool
	cfg               config.ScorerConfig
	acquirerCentroids []msaCentroid // loaded lazily on first score call
}

// NewADVScorer creates an ADVScorer with the given connection pool and config.
func NewADVScorer(pool *pgxpool.Pool, cfg config.ScorerConfig) *ADVScorer {
	return &ADVScorer{pool: pool, cfg: cfg}
}

// Score performs bulk scoring of firms matching the given filters.
func (s *ADVScorer) Score(ctx context.Context, filters *ScoreFilters) ([]FirmScore, error) {
	s.loadAcquirerCentroids(ctx)

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
		score := computeScore(&row, s.cfg, s.acquirerCentroids)
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
	s.loadAcquirerCentroids(ctx)

	row, err := s.queryOneFirm(ctx, crdNumber)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("scorer: query firm %d", crdNumber))
	}

	score := computeScore(row, s.cfg, s.acquirerCentroids)
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
// acquirerCentroids is optional — used for proximity scoring when no CBSA match.
func computeScore(row *scoringRow, cfg config.ScorerConfig, acquirerCentroids []msaCentroid) FirmScore {
	components := map[string]float64{
		"aum_fit":           scoreAUMFit(row.AUM, cfg.MinAUM, cfg.MaxAUM),
		"growth":            scoreGrowth(row.AUM3YrCAGRPct),
		"client_quality":    scoreClientQuality(row.HNWRevenuePct, row.InstitutionalRevPct, row.ClientTypes),
		"service_fit":       scoreServiceFit(row),
		"geo_match":         scoreGeoMatch(row, cfg, acquirerCentroids),
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

// scoreGeoMatch returns 0.0-1.0 based on geographic targeting using MSA data.
// Tiered scoring: multi-MSA > single-MSA > proximity > state+classification > state-only > keyword.
func scoreGeoMatch(row *scoringRow, cfg config.ScorerConfig, acquirerCentroids []msaCentroid) float64 {
	noPreference := len(cfg.TargetStates) == 0 && len(cfg.GeoKeywords) == 0 &&
		len(cfg.AcquirerCBSAs) == 0 && len(cfg.TargetCBSAs) == 0
	if noPreference {
		return 0.5 // neutral when no geo preference
	}

	// Tier 0: Multi-MSA scoring — check ALL MSA associations for best match.
	if len(row.AllMSAs) > 0 {
		var bestScore float64
		for _, msa := range row.AllMSAs {
			for _, ac := range cfg.AcquirerCBSAs {
				if strings.EqualFold(msa.CBSACode, ac) {
					s := applyDistanceDecay(1.0, &msa.CentroidKM)
					if s > bestScore {
						bestScore = s
					}
				}
			}
			for _, tc := range cfg.TargetCBSAs {
				if strings.EqualFold(msa.CBSACode, tc) {
					s := applyDistanceDecay(0.9, &msa.CentroidKM)
					if s > bestScore {
						bestScore = s
					}
				}
			}
		}
		if bestScore > 0 {
			return bestScore
		}
	}

	// Tier 1: Single-MSA scoring when CBSA data is available (fallback for rows without AllMSAs).
	if row.CBSACode != nil {
		cbsa := *row.CBSACode

		// Acquirer CBSA match (firm is near an acquirer office).
		for _, ac := range cfg.AcquirerCBSAs {
			if strings.EqualFold(cbsa, ac) {
				return applyDistanceDecay(1.0, row.CentroidKM)
			}
		}

		// Target market CBSA match (firm is in a target market).
		for _, tc := range cfg.TargetCBSAs {
			if strings.EqualFold(cbsa, tc) {
				return applyDistanceDecay(0.9, row.CentroidKM)
			}
		}

		// State match with classification bonus.
		if stateMatches(row.State, cfg.TargetStates) && row.Classification != nil {
			switch *row.Classification {
			case "urban_core":
				return 0.85
			case "suburban":
				return 0.75
			case "exurban":
				return applyEdgeDecay(0.7, row.EdgeKM)
			case "rural":
				return applyEdgeDecay(0.45, row.EdgeKM)
			}
		}
	}

	// Tier 1.5: Acquirer proximity — firm is near an acquirer MSA but not in it.
	if row.Latitude != nil && row.Longitude != nil && len(acquirerCentroids) > 0 {
		proxScore := acquirerProximityScore(*row.Latitude, *row.Longitude, acquirerCentroids)
		if proxScore > 0 {
			return proxScore
		}
	}

	// Tier 2: Fallback — state match without geo specificity.
	if stateMatches(row.State, cfg.TargetStates) {
		return 0.5
	}

	// Keyword match in brochure text.
	if len(cfg.GeoKeywords) > 0 && row.BrochureText != "" {
		hits := matchKeywords(cfg.GeoKeywords, row.BrochureText, "")
		if len(hits) > 0 {
			return 0.3
		}
	}

	return 0.0
}

// stateMatches returns true if the state matches any target state.
func stateMatches(state string, targets []string) bool {
	for _, ts := range targets {
		if strings.EqualFold(state, ts) {
			return true
		}
	}
	return false
}

// applyEdgeDecay adjusts score based on distance from MSA boundary.
// Firms closer to the edge get a higher score.
func applyEdgeDecay(base float64, edgeKM *float64) float64 {
	if edgeKM == nil {
		return base * 0.9
	}
	km := *edgeKM
	// 0-10km: full base score
	// 10-40km: linear decay to 85% of base
	// >40km: 85% of base
	if km <= 10 {
		return base
	}
	decay := 1.0 - 0.15*math.Min((km-10)/30.0, 1.0)
	return base * decay
}

// applyDistanceDecay applies centroid distance decay to a base score.
// The closer to the MSA centroid, the higher the score (within 50km window).
func applyDistanceDecay(base float64, centroidKM *float64) float64 {
	if centroidKM == nil {
		return base * 0.95 // slight penalty when distance unknown
	}
	km := *centroidKM
	decay := 1.0 - math.Min(km/50.0, 1.0)
	return base * (0.95 + 0.05*decay)
}

// loadAcquirerCentroids queries cbsa_areas for acquirer MSA centroids.
// Called lazily on first Score/ScoreOne invocation.
func (s *ADVScorer) loadAcquirerCentroids(ctx context.Context) {
	if len(s.cfg.AcquirerCBSAs) == 0 || len(s.acquirerCentroids) > 0 {
		return
	}
	rows, err := s.pool.Query(ctx, `
		SELECT cbsa_code, ST_Y(ST_Centroid(geom)) AS lat, ST_X(ST_Centroid(geom)) AS lon
		FROM public.cbsa_areas
		WHERE cbsa_code = ANY($1)`,
		s.cfg.AcquirerCBSAs)
	if err != nil {
		zap.L().Warn("scorer: failed to load acquirer centroids", zap.Error(err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var c msaCentroid
		if scanErr := rows.Scan(&c.CBSACode, &c.Latitude, &c.Longitude); scanErr != nil {
			continue
		}
		s.acquirerCentroids = append(s.acquirerCentroids, c)
	}
}

// haversineKM returns the great-circle distance in km between two lat/lon points.
func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKM = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	lat1R := lat1 * math.Pi / 180.0
	lat2R := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1R)*math.Cos(lat2R)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusKM * c
}

// acquirerProximityScore returns a proximity bonus for firms near (but not in)
// an acquirer MSA. Returns 0 if no proximity is found.
// Distance bands: 0-50km → 0.65, 50-100km → 0.5, >100km → 0.
func acquirerProximityScore(lat, lon float64, centroids []msaCentroid) float64 {
	if len(centroids) == 0 {
		return 0
	}
	bestDist := math.MaxFloat64
	for _, c := range centroids {
		d := haversineKM(lat, lon, c.Latitude, c.Longitude)
		if d < bestDist {
			bestDist = d
		}
	}
	if bestDist <= 50 {
		return 0.65
	}
	if bestDist <= 100 {
		return 0.5
	}
	return 0
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
    COALESCE(lc.text_content, ''),
    fm.latitude,
    fm.longitude,
    fm.cbsa_code,
    fm.msa_name,
    fm.is_within,
    fm.centroid_km,
    fm.edge_km,
    fm.classification,
    (SELECT jsonb_agg(jsonb_build_object(
        'cbsa_code', am.cbsa_code,
        'centroid_km', am.centroid_km,
        'edge_km', am.edge_km,
        'classification', am.classification
    )) FROM public.address_msa am
    JOIN public.company_addresses a ON a.id = am.address_id
    JOIN public.company_matches cm2 ON cm2.company_id = a.company_id
    WHERE cm2.matched_key = fc.crd_number::text
    AND cm2.matched_source = 'adv_firms') AS all_msas
FROM fed_data.mv_firm_combined fc
LEFT JOIN fed_data.adv_computed_metrics cm ON cm.crd_number = fc.crd_number
LEFT JOIN latest_brochure lb ON lb.crd_number = fc.crd_number
LEFT JOIN latest_crs lc ON lc.crd_number = fc.crd_number
LEFT JOIN public.v_firm_msa fm ON fm.crd_number = fc.crd_number
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
    COALESCE((SELECT text_content FROM latest_crs), ''),
    fm.latitude,
    fm.longitude,
    fm.cbsa_code,
    fm.msa_name,
    fm.is_within,
    fm.centroid_km,
    fm.edge_km,
    fm.classification,
    (SELECT jsonb_agg(jsonb_build_object(
        'cbsa_code', am.cbsa_code,
        'centroid_km', am.centroid_km,
        'edge_km', am.edge_km,
        'classification', am.classification
    )) FROM public.address_msa am
    JOIN public.company_addresses a ON a.id = am.address_id
    JOIN public.company_matches cm2 ON cm2.company_id = a.company_id
    WHERE cm2.matched_key = fc.crd_number::text
    AND cm2.matched_source = 'adv_firms') AS all_msas
FROM fed_data.mv_firm_combined fc
LEFT JOIN fed_data.adv_computed_metrics cm ON cm.crd_number = fc.crd_number
LEFT JOIN public.v_firm_msa fm ON fm.crd_number = fc.crd_number
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
		var allMSAsJSON []byte
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
			&sr.Latitude,
			&sr.Longitude,
			&sr.CBSACode,
			&sr.MSAName,
			&sr.IsWithin,
			&sr.CentroidKM,
			&sr.EdgeKM,
			&sr.Classification,
			&allMSAsJSON,
		)
		if err != nil {
			return nil, eris.Wrap(err, "scorer: scan row")
		}
		if len(allMSAsJSON) > 0 {
			if err := json.Unmarshal(allMSAsJSON, &sr.AllMSAs); err != nil {
				return nil, eris.Wrap(err, "scorer: unmarshal all_msas")
			}
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
	var allMSAsJSON []byte
	err := row.Scan(
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
		&sr.Latitude,
		&sr.Longitude,
		&sr.CBSACode,
		&sr.MSAName,
		&sr.IsWithin,
		&sr.CentroidKM,
		&sr.EdgeKM,
		&sr.Classification,
		&allMSAsJSON,
	)
	if err != nil {
		return err
	}
	if len(allMSAsJSON) > 0 {
		if err := json.Unmarshal(allMSAsJSON, &sr.AllMSAs); err != nil {
			return eris.Wrap(err, "scorer: unmarshal all_msas")
		}
	}
	return nil
}
