package scorer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func defaultTestConfig() config.ScorerConfig {
	return DefaultScorerConfig()
}

func ptrFloat64(v float64) *float64 { return &v }

func TestScoreAUMFit(t *testing.T) {
	tests := []struct {
		name   string
		aum    int64
		minAUM int64
		maxAUM int64
		want   float64
	}{
		{"zero aum", 0, 100_000_000, 5_000_000_000, 0},
		{"in range", 500_000_000, 100_000_000, 5_000_000_000, 1.0},
		{"at min", 100_000_000, 100_000_000, 5_000_000_000, 1.0},
		{"at max", 5_000_000_000, 100_000_000, 5_000_000_000, 1.0},
		{"below min half", 50_000_000, 100_000_000, 5_000_000_000, 0.5},
		{"above max double", 10_000_000_000, 100_000_000, 5_000_000_000, 0.5},
		{"no upper bound in range", 200_000_000, 100_000_000, 0, 1.0},
		{"no upper bound below min", 50_000_000, 100_000_000, 0, 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreAUMFit(tt.aum, tt.minAUM, tt.maxAUM)
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestScoreGrowth(t *testing.T) {
	tests := []struct {
		name string
		cagr *float64
		want float64
	}{
		{"nil cagr", nil, 0.5},
		{"high growth 25%", ptrFloat64(25), 1.0},
		{"good growth 15%", ptrFloat64(15), 0.8},
		{"moderate growth 7%", ptrFloat64(7), 0.6},
		{"flat 2%", ptrFloat64(2), 0.4},
		{"slight decline -3%", ptrFloat64(-3), 0.2},
		{"steep decline -10%", ptrFloat64(-10), 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreGrowth(tt.cagr)
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestScoreClientQuality(t *testing.T) {
	tests := []struct {
		name        string
		hnwPct      *float64
		instPct     *float64
		clientTypes []string
		wantMin     float64
		wantMax     float64
	}{
		{"all nil", nil, nil, nil, 0, 0.01},
		{"high hnw", ptrFloat64(80), nil, nil, 0.35, 0.45},
		{"high hnw + inst", ptrFloat64(80), ptrFloat64(50), nil, 0.5, 0.7},
		{"full diversity", ptrFloat64(60), ptrFloat64(30), []string{"a", "b", "c", "d", "e"}, 0.55, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreClientQuality(tt.hnwPct, tt.instPct, tt.clientTypes)
			assert.GreaterOrEqual(t, got, tt.wantMin)
			assert.LessOrEqual(t, got, tt.wantMax)
		})
	}
}

func TestScoreServiceFit(t *testing.T) {
	tests := []struct {
		name string
		row  scoringRow
		want float64
	}{
		{"no services", scoringRow{}, 0},
		{"financial planning + pct aum", scoringRow{
			SvcFinancialPlanning: true,
			CompPctAUM:           true,
		}, 0.5},
		{"all relevant services", scoringRow{
			SvcFinancialPlanning:      true,
			SvcPortfolioIndividuals:   true,
			SvcPortfolioInstitutional: true,
			SvcPensionConsulting:      true,
			WrapFeeProgram:            true,
			CompPctAUM:                true,
		}, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreServiceFit(&tt.row)
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestScoreGeoMatch(t *testing.T) {
	tests := []struct {
		name         string
		state        string
		targetStates []string
		geoKeywords  []string
		brochureText string
		want         float64
	}{
		{"no preferences", "CA", nil, nil, "", 0.5},
		{"state match", "TX", []string{"TX", "CA"}, nil, "", 1.0},
		{"no state match", "NY", []string{"TX", "CA"}, nil, "", 0},
		{"keyword match in brochure", "NY", nil, []string{"new york"}, "We serve clients in New York metropolitan area", 0.7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreGeoMatch(tt.state, tt.targetStates, tt.geoKeywords, tt.brochureText)
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestScoreRegulatoryClean(t *testing.T) {
	tests := []struct {
		name       string
		hasAny     bool
		criminal   bool
		regulatory bool
		want       float64
	}{
		{"clean", false, false, false, 1.0},
		{"criminal", true, true, false, 0.0},
		{"regulatory only", true, false, true, 0.3},
		{"other drp", true, false, false, 0.6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreRegulatoryClean(tt.hasAny, tt.criminal, tt.regulatory)
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestScoreSuccessionSignal(t *testing.T) {
	tests := []struct {
		name     string
		keywords []string
		text     string
		want     float64
	}{
		{"no keywords", nil, "some text", 0},
		{"no text", []string{"retirement"}, "", 0},
		{"one match", []string{"retirement", "succession"}, "planning for retirement", 0.5},
		{"two matches", []string{"retirement", "succession"}, "retirement and succession planning", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreSuccessionSignal(tt.keywords, tt.text, "")
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestMatchKeywords(t *testing.T) {
	tests := []struct {
		name     string
		keywords []string
		texts    []string
		wantLen  int
	}{
		{"empty keywords", nil, []string{"some text"}, 0},
		{"empty texts", []string{"keyword"}, nil, 0},
		{"single match", []string{"hello", "world"}, []string{"Hello there"}, 1},
		{"case insensitive", []string{"UPPER"}, []string{"upper case"}, 1},
		{"across texts", []string{"first", "second"}, []string{"first text", "second text"}, 2},
		{"no match", []string{"missing"}, []string{"nothing here"}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchKeywords(tt.keywords, tt.texts...)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

func TestComputeScore(t *testing.T) {
	cfg := defaultTestConfig()

	t.Run("ideal firm", func(t *testing.T) {
		row := &scoringRow{
			CRDNumber:                 12345,
			FirmName:                  "Ideal Wealth Management",
			State:                     "TX",
			AUM:                       500_000_000,
			Website:                   "https://ideal.com",
			NumEmployees:              20,
			SvcFinancialPlanning:      true,
			SvcPortfolioIndividuals:   true,
			SvcPortfolioInstitutional: true,
			CompPctAUM:                true,
			AUM3YrCAGRPct:             ptrFloat64(15),
			HNWRevenuePct:             ptrFloat64(70),
			InstitutionalRevPct:       ptrFloat64(20),
			ClientTypes:               []string{"individuals", "hnw", "institutions"},
			BrochureText:              "We specialize in retirement planning and succession strategies",
		}

		score := computeScore(row, cfg)
		assert.Equal(t, 12345, score.CRDNumber)
		assert.Equal(t, "Ideal Wealth Management", score.FirmName)
		assert.Greater(t, score.Score, 50.0, "ideal firm should score above 50")
		assert.NotNil(t, score.ComponentScores)
		assert.Contains(t, score.ComponentScores, "aum_fit")
		assert.Contains(t, score.ComponentScores, "regulatory_clean")
	})

	t.Run("minimal firm no metrics", func(t *testing.T) {
		row := &scoringRow{
			CRDNumber: 99999,
			FirmName:  "Bare Minimum LLC",
			AUM:       200_000_000,
		}

		score := computeScore(row, cfg)
		assert.Equal(t, 99999, score.CRDNumber)
		// Should still produce a score (growth defaults to 0.5 when nil).
		assert.GreaterOrEqual(t, score.Score, 0.0)
		assert.LessOrEqual(t, score.Score, 100.0)
	})

	t.Run("zero aum", func(t *testing.T) {
		row := &scoringRow{
			CRDNumber: 11111,
			FirmName:  "Zero AUM",
			AUM:       0,
		}

		score := computeScore(row, cfg)
		assert.Equal(t, 0.0, score.ComponentScores["aum_fit"])
	})

	t.Run("negative keywords penalty", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.NegativeKeywords = []string{"fraud", "ponzi"}

		cleanRow := &scoringRow{
			CRDNumber:    22222,
			FirmName:     "Clean Firm",
			AUM:          500_000_000,
			BrochureText: "We provide excellent financial planning services",
		}
		dirtyRow := &scoringRow{
			CRDNumber:    33333,
			FirmName:     "Problematic Firm",
			AUM:          500_000_000,
			BrochureText: "Despite past fraud allegations and ponzi scheme concerns",
		}

		cleanScore := computeScore(cleanRow, cfg)
		dirtyScore := computeScore(dirtyRow, cfg)

		assert.Greater(t, cleanScore.Score, dirtyScore.Score, "clean firm should score higher")
		assert.NotNil(t, dirtyScore.MatchedKeywords)
		assert.Contains(t, dirtyScore.MatchedKeywords, "negative")
	})

	t.Run("criminal drp zeroes regulatory component", func(t *testing.T) {
		row := &scoringRow{
			CRDNumber:       44444,
			FirmName:        "Criminal Firm",
			AUM:             500_000_000,
			HasAnyDRP:       true,
			DRPCriminalFirm: true,
		}

		score := computeScore(row, cfg)
		assert.Equal(t, 0.0, score.ComponentScores["regulatory_clean"])
	})
}

func TestComputeScoreNormalization(t *testing.T) {
	cfg := defaultTestConfig()

	// A firm with perfect scores across all components should score close to 100.
	row := &scoringRow{
		CRDNumber:                 55555,
		FirmName:                  "Perfect Firm",
		State:                     "TX",
		AUM:                       500_000_000,
		SvcFinancialPlanning:      true,
		SvcPortfolioIndividuals:   true,
		SvcPortfolioInstitutional: true,
		SvcPensionConsulting:      true,
		WrapFeeProgram:            true,
		CompPctAUM:                true,
		AUM3YrCAGRPct:             ptrFloat64(25),
		HNWRevenuePct:             ptrFloat64(100),
		InstitutionalRevPct:       ptrFloat64(50),
		ClientTypes:               []string{"a", "b", "c", "d", "e"},
	}
	cfg.TargetStates = []string{"TX"}
	cfg.IndustryKeywords = []string{"wealth"}
	cfg.SuccessionKeywords = []string{"retirement"}
	row.BrochureText = "wealth management and retirement planning"

	score := computeScore(row, cfg)
	assert.Greater(t, score.Score, 80.0, "perfect firm should score above 80")
	assert.LessOrEqual(t, score.Score, 100.0, "score should not exceed 100")
}

func TestSortByScore(t *testing.T) {
	scores := []FirmScore{
		{CRDNumber: 1, Score: 30},
		{CRDNumber: 2, Score: 90},
		{CRDNumber: 3, Score: 60},
	}
	sortByScore(scores)

	assert.Equal(t, 2, scores[0].CRDNumber)
	assert.Equal(t, 3, scores[1].CRDNumber)
	assert.Equal(t, 1, scores[2].CRDNumber)
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid default config", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		err := ValidateConfig(cfg)
		require.NoError(t, err)
	})

	t.Run("negative weight", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		cfg.AUMFitWeight = -1
		err := ValidateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "aum_fit_weight must be >= 0")
	})

	t.Run("weights dont sum to 100", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		cfg.AUMFitWeight = 50 // sum now > 100
		err := ValidateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "weights should sum to 100")
	})

	t.Run("inverted aum range", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		cfg.MinAUM = 1_000_000_000
		cfg.MaxAUM = 100_000_000
		err := ValidateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_aum must be >= min_aum")
	})

	t.Run("inverted employee range", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		cfg.MinEmployees = 100
		cfg.MaxEmployees = 5
		err := ValidateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_employees must be >= min_employees")
	})

	t.Run("min score out of range", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		cfg.MinScore = 150
		err := ValidateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "min_score must be between 0 and 100")
	})

	t.Run("negative max firms", func(t *testing.T) {
		cfg := DefaultScorerConfig()
		cfg.MaxFirms = -1
		err := ValidateConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max_firms must be >= 0")
	})
}

func TestScoreIndustryMatch(t *testing.T) {
	tests := []struct {
		name     string
		keywords []string
		brochure string
		crs      string
		want     float64
	}{
		{"no keywords neutral", nil, "some text", "", 0.5},
		{"no match", []string{"fintech"}, "traditional advisory firm", "", 0.0},
		{"partial match", []string{"wealth", "retirement", "estate"}, "wealth management focus", "", 1.0 / 3},
		{"full match", []string{"wealth"}, "wealth management focus", "", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scoreIndustryMatch(tt.keywords, tt.brochure, tt.crs)
			assert.InDelta(t, tt.want, got, 0.01)
		})
	}
}

func TestWeightSum(t *testing.T) {
	cfg := DefaultScorerConfig()
	sum := WeightSum(cfg)
	assert.InDelta(t, 100.0, sum, 0.01)
}
