// Package scorer implements multi-pass firm scoring for M&A target identification.
package scorer

import (
	"fmt"
	"math"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
)

// DefaultScorerConfig returns a config.ScorerConfig with sensible defaults.
// Weights sum to 100.
func DefaultScorerConfig() config.ScorerConfig {
	return config.ScorerConfig{
		// Weights (sum = 100).
		AUMFitWeight:           25,
		GrowthWeight:           10,
		ClientQualityWeight:    15,
		ServiceFitWeight:       10,
		GeoMatchWeight:         10,
		IndustryMatchWeight:    10,
		RegulatoryCleanWeight:  10,
		SuccessionSignalWeight: 10,

		// Target ranges.
		MinAUM:       100_000_000,   // $100M
		MaxAUM:       5_000_000_000, // $5B
		MinEmployees: 3,
		MaxEmployees: 200,

		// Keywords.
		SuccessionKeywords: []string{
			"succession", "retirement", "transition", "selling practice",
			"exit planning", "next chapter", "winding down",
		},
		NegativeKeywords: []string{
			"bankruptcy", "fraud", "ponzi", "sec enforcement",
			"criminal charges", "revoked",
		},

		// Thresholds.
		MinScore: 50,
		MaxFirms: 500,
	}
}

// WeightSum returns the sum of all component weights.
func WeightSum(c config.ScorerConfig) float64 {
	return c.AUMFitWeight + c.GrowthWeight + c.ClientQualityWeight +
		c.ServiceFitWeight + c.GeoMatchWeight + c.IndustryMatchWeight +
		c.RegulatoryCleanWeight + c.SuccessionSignalWeight
}

// ValidateConfig checks that a ScorerConfig is internally consistent.
func ValidateConfig(c config.ScorerConfig) error {
	var errs []string

	// All weights must be non-negative.
	weights := map[string]float64{
		"aum_fit_weight":           c.AUMFitWeight,
		"growth_weight":            c.GrowthWeight,
		"client_quality_weight":    c.ClientQualityWeight,
		"service_fit_weight":       c.ServiceFitWeight,
		"geo_match_weight":         c.GeoMatchWeight,
		"industry_match_weight":    c.IndustryMatchWeight,
		"regulatory_clean_weight":  c.RegulatoryCleanWeight,
		"succession_signal_weight": c.SuccessionSignalWeight,
	}
	for name, w := range weights {
		if w < 0 {
			errs = append(errs, fmt.Sprintf("%s must be >= 0", name))
		}
	}

	sum := WeightSum(c)

	// Weights must sum to a positive number.
	if sum <= 0 {
		errs = append(errs, "weight sum must be > 0")
	}

	// Weights should be close to 100 (allow tolerance for floating-point).
	if math.Abs(sum-100) > 1 {
		errs = append(errs, fmt.Sprintf("weights should sum to 100, got %.1f", sum))
	}

	// AUM range.
	if c.MinAUM < 0 {
		errs = append(errs, "min_aum must be >= 0")
	}
	if c.MaxAUM > 0 && c.MaxAUM < c.MinAUM {
		errs = append(errs, "max_aum must be >= min_aum")
	}

	// Employee range.
	if c.MinEmployees < 0 {
		errs = append(errs, "min_employees must be >= 0")
	}
	if c.MaxEmployees > 0 && c.MaxEmployees < c.MinEmployees {
		errs = append(errs, "max_employees must be >= min_employees")
	}

	// Thresholds.
	if c.MinScore < 0 || c.MinScore > 100 {
		errs = append(errs, "min_score must be between 0 and 100")
	}
	if c.MaxFirms < 0 {
		errs = append(errs, "max_firms must be >= 0")
	}

	if len(errs) > 0 {
		return eris.Errorf("scorer: config validation failed: %s", strings.Join(errs, "; "))
	}
	return nil
}
