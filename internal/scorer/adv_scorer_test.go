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
func ptrString(v string) *string    { return &v }

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
	t.Run("no preferences returns neutral", func(t *testing.T) {
		row := &scoringRow{State: "CA"}
		cfg := defaultTestConfig()
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.5, got, 0.01)
	})

	t.Run("acquirer CBSA match scores highest", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("12420"),
			CentroidKM:     ptrFloat64(5.0),
			Classification: ptrString("urban_core"),
		}
		cfg := defaultTestConfig()
		cfg.AcquirerCBSAs = []string{"12420"}
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.Greater(t, got, 0.95, "acquirer CBSA match should score >0.95")
		assert.LessOrEqual(t, got, 1.0)
	})

	t.Run("target CBSA match scores high", func(t *testing.T) {
		row := &scoringRow{
			State:          "NY",
			CBSACode:       ptrString("35620"),
			CentroidKM:     ptrFloat64(10.0),
			Classification: ptrString("urban_core"),
		}
		cfg := defaultTestConfig()
		cfg.TargetCBSAs = []string{"35620"}
		cfg.TargetStates = []string{"NY"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.Greater(t, got, 0.85, "target CBSA match should score >0.85")
		assert.LessOrEqual(t, got, 0.95)
	})

	t.Run("state match with urban classification", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			CentroidKM:     ptrFloat64(15.0),
			Classification: ptrString("urban_core"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.85, got, 0.01)
	})

	t.Run("state match with suburban classification", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			Classification: ptrString("suburban"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.75, got, 0.01)
	})

	t.Run("state match with exurban classification nil edge", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			Classification: ptrString("exurban"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		// applyEdgeDecay(0.7, nil) = 0.7 * 0.9 = 0.63
		assert.InDelta(t, 0.63, got, 0.01)
	})

	t.Run("state match with exurban close to edge", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			EdgeKM:         ptrFloat64(5),
			Classification: ptrString("exurban"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		// applyEdgeDecay(0.7, 5) = 0.7 (within 10km plateau)
		assert.InDelta(t, 0.7, got, 0.01)
	})

	t.Run("state match with exurban far from edge", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			EdgeKM:         ptrFloat64(40),
			Classification: ptrString("exurban"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		// applyEdgeDecay(0.7, 40) = 0.7 * 0.85 = 0.595
		assert.InDelta(t, 0.595, got, 0.01)
	})

	t.Run("state match with rural classification nil edge", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			Classification: ptrString("rural"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		// applyEdgeDecay(0.45, nil) = 0.45 * 0.9 = 0.405
		assert.InDelta(t, 0.405, got, 0.01)
	})

	t.Run("state match with rural close to edge", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("99999"),
			EdgeKM:         ptrFloat64(5),
			Classification: ptrString("rural"),
		}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		// applyEdgeDecay(0.45, 5) = 0.45 (within 10km plateau)
		assert.InDelta(t, 0.45, got, 0.01)
	})

	t.Run("state match without CBSA data fallback", func(t *testing.T) {
		row := &scoringRow{State: "TX"}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX", "CA"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.5, got, 0.01, "state-only fallback should be 0.5")
	})

	t.Run("keyword match only", func(t *testing.T) {
		row := &scoringRow{
			State:        "NY",
			BrochureText: "We serve clients in the Austin metropolitan area",
		}
		cfg := defaultTestConfig()
		cfg.GeoKeywords = []string{"austin"}
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.3, got, 0.01, "keyword-only match should be 0.3")
	})

	t.Run("multi-MSA acquirer match picks best", func(t *testing.T) {
		row := &scoringRow{
			State: "TX",
			AllMSAs: []msaEntry{
				{CBSACode: "99999", CentroidKM: 20.0, Classification: "suburban"},
				{CBSACode: "12420", CentroidKM: 5.0, Classification: "urban_core"},
			},
		}
		cfg := defaultTestConfig()
		cfg.AcquirerCBSAs = []string{"12420"}
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.Greater(t, got, 0.95, "multi-MSA acquirer match should score >0.95")
		assert.LessOrEqual(t, got, 1.0)
	})

	t.Run("multi-MSA target match no acquirer", func(t *testing.T) {
		row := &scoringRow{
			State: "NY",
			AllMSAs: []msaEntry{
				{CBSACode: "99999", CentroidKM: 30.0, Classification: "suburban"},
				{CBSACode: "35620", CentroidKM: 10.0, Classification: "urban_core"},
			},
		}
		cfg := defaultTestConfig()
		cfg.TargetCBSAs = []string{"35620"}
		cfg.TargetStates = []string{"NY"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.Greater(t, got, 0.85, "multi-MSA target match should score >0.85")
		assert.LessOrEqual(t, got, 0.95)
	})

	t.Run("multi-MSA no CBSA match falls through", func(t *testing.T) {
		row := &scoringRow{
			State: "TX",
			AllMSAs: []msaEntry{
				{CBSACode: "11111", CentroidKM: 10.0, Classification: "suburban"},
				{CBSACode: "22222", CentroidKM: 20.0, Classification: "exurban"},
			},
		}
		cfg := defaultTestConfig()
		cfg.AcquirerCBSAs = []string{"12420"}
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		// No CBSA match in AllMSAs, no single-MSA CBSACode set → state fallback = 0.5.
		assert.InDelta(t, 0.5, got, 0.01, "no CBSA match should fall through to state")
	})

	t.Run("nil AllMSAs uses single-MSA path", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("12420"),
			CentroidKM:     ptrFloat64(5.0),
			Classification: ptrString("urban_core"),
		}
		cfg := defaultTestConfig()
		cfg.AcquirerCBSAs = []string{"12420"}
		cfg.TargetStates = []string{"TX"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.Greater(t, got, 0.95, "nil AllMSAs should use single-MSA path")
	})

	t.Run("no match at all", func(t *testing.T) {
		row := &scoringRow{State: "NY"}
		cfg := defaultTestConfig()
		cfg.TargetStates = []string{"TX", "CA"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.0, got, 0.01)
	})

	t.Run("centroid distance decay", func(t *testing.T) {
		// Close to centroid (5km) should score higher than far (45km).
		rowClose := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("12420"),
			CentroidKM:     ptrFloat64(5.0),
			Classification: ptrString("urban_core"),
		}
		rowFar := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("12420"),
			CentroidKM:     ptrFloat64(45.0),
			Classification: ptrString("suburban"),
		}
		cfg := defaultTestConfig()
		cfg.AcquirerCBSAs = []string{"12420"}
		cfg.TargetStates = []string{"TX"}

		scoreClose := scoreGeoMatch(rowClose, cfg, nil)
		scoreFar := scoreGeoMatch(rowFar, cfg, nil)
		assert.Greater(t, scoreClose, scoreFar, "closer firm should score higher")
	})

	t.Run("nil centroid km in distance decay", func(t *testing.T) {
		row := &scoringRow{
			State:          "TX",
			CBSACode:       ptrString("12420"),
			CentroidKM:     nil, // unknown distance
			Classification: ptrString("urban_core"),
		}
		cfg := defaultTestConfig()
		cfg.AcquirerCBSAs = []string{"12420"}
		got := scoreGeoMatch(row, cfg, nil)
		assert.InDelta(t, 0.95, got, 0.01, "nil centroid should apply 0.95 base")
	})
}

func TestHaversineKM(t *testing.T) {
	// Austin (30.2672, -97.7431) to Dallas (32.7767, -96.7970) ≈ 290km.
	d := haversineKM(30.2672, -97.7431, 32.7767, -96.7970)
	assert.InDelta(t, 290, d, 10, "Austin-Dallas should be ~290km")

	// Same point should be 0.
	assert.InDelta(t, 0, haversineKM(30.0, -97.0, 30.0, -97.0), 0.001)
}

func TestAcquirerProximityScore(t *testing.T) {
	centroids := []msaCentroid{
		{CBSACode: "12420", Latitude: 30.2672, Longitude: -97.7431}, // Austin
	}

	t.Run("within 50km", func(t *testing.T) {
		// Round Rock is ~25km from Austin centroid.
		got := acquirerProximityScore(30.5083, -97.6789, centroids)
		assert.InDelta(t, 0.65, got, 0.01)
	})

	t.Run("50-100km", func(t *testing.T) {
		// Waco TX is ~85km from Austin centroid.
		got := acquirerProximityScore(30.85, -97.50, centroids)
		assert.InDelta(t, 0.5, got, 0.01)
	})

	t.Run("beyond 100km", func(t *testing.T) {
		// Dallas is ~290km from Austin.
		got := acquirerProximityScore(32.7767, -96.7970, centroids)
		assert.InDelta(t, 0.0, got, 0.01)
	})

	t.Run("no centroids", func(t *testing.T) {
		got := acquirerProximityScore(30.2672, -97.7431, nil)
		assert.InDelta(t, 0.0, got, 0.01)
	})
}

func TestScoreGeoMatch_Proximity(t *testing.T) {
	// Firm near Austin (within 50km) but NOT in any MSA.
	centroids := []msaCentroid{
		{CBSACode: "12420", Latitude: 30.2672, Longitude: -97.7431},
	}
	lat := 30.5083
	lon := -97.6789
	row := &scoringRow{
		State:     "TX",
		Latitude:  &lat,
		Longitude: &lon,
	}
	cfg := defaultTestConfig()
	cfg.AcquirerCBSAs = []string{"12420"}
	cfg.TargetStates = []string{"TX"}

	got := scoreGeoMatch(row, cfg, centroids)
	assert.InDelta(t, 0.65, got, 0.01, "firm within 50km of acquirer should get 0.65")
}

func TestStateMatches(t *testing.T) {
	assert.True(t, stateMatches("TX", []string{"TX", "CA"}))
	assert.True(t, stateMatches("tx", []string{"TX"}))
	assert.False(t, stateMatches("NY", []string{"TX", "CA"}))
	assert.False(t, stateMatches("TX", nil))
}

func TestApplyEdgeDecay(t *testing.T) {
	tests := []struct {
		name   string
		base   float64
		edgeKM *float64
		want   float64
	}{
		{"nil edge 0.7 base", 0.7, nil, 0.63},
		{"0km", 0.7, ptrFloat64(0), 0.7},
		{"5km inside plateau", 0.7, ptrFloat64(5), 0.7},
		{"10km boundary", 0.7, ptrFloat64(10), 0.7},
		{"25km mid decay", 0.7, ptrFloat64(25), 0.6475},
		{"40km full decay", 0.7, ptrFloat64(40), 0.595},
		{"60km capped", 0.7, ptrFloat64(60), 0.595},
		{"nil edge 0.45 base", 0.45, nil, 0.405},
		{"25km 0.45 base", 0.45, ptrFloat64(25), 0.41625},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyEdgeDecay(tt.base, tt.edgeKM)
			assert.InDelta(t, tt.want, got, 0.001)
		})
	}
}

func TestApplyDistanceDecay(t *testing.T) {
	// At centroid (0km): full score.
	assert.InDelta(t, 1.0, applyDistanceDecay(1.0, ptrFloat64(0)), 0.01)

	// At 50km+: minimum decay.
	assert.InDelta(t, 0.95, applyDistanceDecay(1.0, ptrFloat64(50)), 0.01)
	assert.InDelta(t, 0.95, applyDistanceDecay(1.0, ptrFloat64(100)), 0.01)

	// At 25km: midpoint.
	assert.InDelta(t, 0.975, applyDistanceDecay(1.0, ptrFloat64(25)), 0.01)

	// nil centroid: 0.95 factor.
	assert.InDelta(t, 0.95, applyDistanceDecay(1.0, nil), 0.01)
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

		score := computeScore(row, cfg, nil)
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

		score := computeScore(row, cfg, nil)
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

		score := computeScore(row, cfg, nil)
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

		cleanScore := computeScore(cleanRow, cfg, nil)
		dirtyScore := computeScore(dirtyRow, cfg, nil)

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

		score := computeScore(row, cfg, nil)
		assert.Equal(t, 0.0, score.ComponentScores["regulatory_clean"])
	})

	t.Run("MSA data boosts geo score", func(t *testing.T) {
		cfgWithGeo := defaultTestConfig()
		cfgWithGeo.AcquirerCBSAs = []string{"12420"}
		cfgWithGeo.TargetStates = []string{"TX"}

		rowWithMSA := &scoringRow{
			CRDNumber:      66666,
			FirmName:       "MSA Firm",
			State:          "TX",
			AUM:            500_000_000,
			CBSACode:       ptrString("12420"),
			CentroidKM:     ptrFloat64(10.0),
			Classification: ptrString("urban_core"),
		}
		rowWithoutMSA := &scoringRow{
			CRDNumber: 77777,
			FirmName:  "No MSA Firm",
			State:     "TX",
			AUM:       500_000_000,
		}

		scoreWithMSA := computeScore(rowWithMSA, cfgWithGeo, nil)
		scoreWithoutMSA := computeScore(rowWithoutMSA, cfgWithGeo, nil)

		geoWithMSA := scoreWithMSA.ComponentScores["geo_match"]
		geoWithoutMSA := scoreWithoutMSA.ComponentScores["geo_match"]
		assert.Greater(t, geoWithMSA, geoWithoutMSA, "MSA-aware firm should have higher geo_match")
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

	score := computeScore(row, cfg, nil)
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
