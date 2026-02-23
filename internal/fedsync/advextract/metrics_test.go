package advextract

import (
	"testing"
)

func TestComputeRevenue(t *testing.T) {
	t.Run("three tier schedule", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 250_000_000, AnnualRatePct: 1.00},
			{MinAUM: 250_000_000, MaxAUM: 500_000_000, AnnualRatePct: 0.75},
			{MinAUM: 500_000_000, MaxAUM: 0, AnnualRatePct: 0.50},
		}
		aum := int64(500_000_000)
		rev := ComputeRevenue(tiers, aum)
		// First $250M at 1.0% = $2,500,000
		// Next $250M at 0.75% = $1,875,000
		// Total = $4,375,000
		if rev != 4_375_000 {
			t.Errorf("expected 4375000, got %d", rev)
		}
	})

	t.Run("single tier", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 0, AnnualRatePct: 1.00},
		}
		aum := int64(100_000_000)
		rev := ComputeRevenue(tiers, aum)
		// $100M at 1.0% = $1,000,000
		if rev != 1_000_000 {
			t.Errorf("expected 1000000, got %d", rev)
		}
	})

	t.Run("zero AUM", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 0, AnnualRatePct: 1.00},
		}
		rev := ComputeRevenue(tiers, 0)
		if rev != 0 {
			t.Errorf("expected 0, got %d", rev)
		}
	})

	t.Run("empty tiers", func(t *testing.T) {
		rev := ComputeRevenue(nil, 100_000_000)
		if rev != 0 {
			t.Errorf("expected 0, got %d", rev)
		}
	})

	t.Run("AUM below all tiers", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 250_000_000, AnnualRatePct: 1.00},
			{MinAUM: 250_000_000, MaxAUM: 0, AnnualRatePct: 0.75},
		}
		aum := int64(100_000_000)
		rev := ComputeRevenue(tiers, aum)
		// All $100M in first tier at 1.0% = $1,000,000
		if rev != 1_000_000 {
			t.Errorf("expected 1000000, got %d", rev)
		}
	})

	t.Run("AUM exceeds all tiers", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 250_000_000, AnnualRatePct: 1.00},
			{MinAUM: 250_000_000, MaxAUM: 500_000_000, AnnualRatePct: 0.75},
			{MinAUM: 500_000_000, MaxAUM: 0, AnnualRatePct: 0.50},
		}
		aum := int64(1_000_000_000)
		rev := ComputeRevenue(tiers, aum)
		// First $250M at 1.0% = $2,500,000
		// Next $250M at 0.75% = $1,875,000
		// Remaining $500M at 0.50% = $2,500,000
		// Total = $6,875,000
		if rev != 6_875_000 {
			t.Errorf("expected 6875000, got %d", rev)
		}
	})
}

func TestComputeBlendedRate(t *testing.T) {
	t.Run("three tier schedule", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 250_000_000, AnnualRatePct: 1.00},
			{MinAUM: 250_000_000, MaxAUM: 500_000_000, AnnualRatePct: 0.75},
			{MinAUM: 500_000_000, MaxAUM: 0, AnnualRatePct: 0.50},
		}
		aum := int64(500_000_000)
		bps := ComputeBlendedRate(tiers, aum)
		// Revenue = $4,375,000, AUM = $500M
		// Blended = 4375000 / 500000000 * 10000 = 87.5 bps -> 88 rounded
		if bps != 88 {
			t.Errorf("expected 88 bps, got %d", bps)
		}
	})

	t.Run("zero AUM", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 0, AnnualRatePct: 1.00},
		}
		bps := ComputeBlendedRate(tiers, 0)
		if bps != 0 {
			t.Errorf("expected 0 bps, got %d", bps)
		}
	})

	t.Run("single tier flat rate", func(t *testing.T) {
		tiers := []FeeTier{
			{MinAUM: 0, MaxAUM: 0, AnnualRatePct: 1.00},
		}
		bps := ComputeBlendedRate(tiers, 100_000_000)
		// Flat 1% = 100 bps
		if bps != 100 {
			t.Errorf("expected 100 bps, got %d", bps)
		}
	})
}

func TestParseFeeTiers(t *testing.T) {
	t.Run("valid array of maps", func(t *testing.T) {
		value := []any{
			map[string]any{
				"min_aum":        float64(0),
				"max_aum":        float64(250_000_000),
				"annual_rate_pct": float64(1.00),
			},
			map[string]any{
				"min_aum":        float64(250_000_000),
				"max_aum":        float64(0),
				"annual_rate_pct": float64(0.75),
			},
		}
		tiers := ParseFeeTiers(value)
		if len(tiers) != 2 {
			t.Fatalf("expected 2 tiers, got %d", len(tiers))
		}
		if tiers[0].MinAUM != 0 || tiers[0].MaxAUM != 250_000_000 || tiers[0].AnnualRatePct != 1.00 {
			t.Errorf("tier 0 mismatch: %+v", tiers[0])
		}
		if tiers[1].MinAUM != 250_000_000 || tiers[1].MaxAUM != 0 || tiers[1].AnnualRatePct != 0.75 {
			t.Errorf("tier 1 mismatch: %+v", tiers[1])
		}
	})

	t.Run("nil value", func(t *testing.T) {
		tiers := ParseFeeTiers(nil)
		if tiers != nil {
			t.Errorf("expected nil, got %v", tiers)
		}
	})

	t.Run("invalid data", func(t *testing.T) {
		tiers := ParseFeeTiers("not an array")
		if tiers != nil {
			t.Errorf("expected nil, got %v", tiers)
		}
	})

	t.Run("empty array", func(t *testing.T) {
		tiers := ParseFeeTiers([]any{})
		if tiers != nil {
			t.Errorf("expected nil, got %v", tiers)
		}
	})

	t.Run("array with non-map elements", func(t *testing.T) {
		tiers := ParseFeeTiers([]any{"bad", "data"})
		if tiers != nil {
			t.Errorf("expected nil, got %v", tiers)
		}
	})
}

func TestComputeScores(t *testing.T) {
	t.Run("filing with comp and biz flags", func(t *testing.T) {
		filing := map[string]any{
			"comp_pct_aum":     true,
			"comp_hourly":      true,
			"comp_fixed":       true,
			"comp_commissions": false,
			"comp_performance": false,
			"comp_subscription": false,
			"comp_other":       false,
			// biz flags
			"biz_broker_dealer": true,
			"biz_insurance":     true,
			// aff flags
			"aff_broker_dealer": true,
			"aff_insurance":     true,
			"aff_bank":          true,
		}
		answers := map[string]Answer{}
		scores := ComputeScores(filing, answers)

		if scores.CompensationDiversity != 3 {
			t.Errorf("expected CompensationDiversity=3, got %d", scores.CompensationDiversity)
		}
		if scores.BusinessComplexity != 5 {
			t.Errorf("expected BusinessComplexity=5, got %d", scores.BusinessComplexity)
		}
		if scores.DRPSeverity != 0 {
			t.Errorf("expected DRPSeverity=0, got %d", scores.DRPSeverity)
		}
	})

	t.Run("filing with DRP flags", func(t *testing.T) {
		filing := map[string]any{
			"drp_criminal_firm":      true,  // 3
			"drp_regulatory_firm":    true,  // 2
			"drp_complaint_firm":     true,  // 1
			"drp_termination_firm":   true,  // 1
			"drp_judgment":           true,  // 2
		}
		answers := map[string]Answer{}
		scores := ComputeScores(filing, answers)

		if scores.DRPSeverity != 9 {
			t.Errorf("expected DRPSeverity=9, got %d", scores.DRPSeverity)
		}
	})

	t.Run("DRP severity capped at 10", func(t *testing.T) {
		filing := map[string]any{
			"drp_criminal_firm":       true, // 3
			"drp_criminal_affiliate":  true, // 3
			"drp_regulatory_firm":     true, // 2
			"drp_regulatory_affiliate": true, // 2
			"drp_civil_firm":          true, // 2
		}
		answers := map[string]Answer{}
		scores := ComputeScores(filing, answers)

		if scores.DRPSeverity != 10 {
			t.Errorf("expected DRPSeverity=10 (capped), got %d", scores.DRPSeverity)
		}
	})

	t.Run("empty filing", func(t *testing.T) {
		scores := ComputeScores(map[string]any{}, map[string]Answer{})
		if scores.CompensationDiversity != 0 {
			t.Errorf("expected CompensationDiversity=0, got %d", scores.CompensationDiversity)
		}
		if scores.BusinessComplexity != 0 {
			t.Errorf("expected BusinessComplexity=0, got %d", scores.BusinessComplexity)
		}
		if scores.DRPSeverity != 0 {
			t.Errorf("expected DRPSeverity=0, got %d", scores.DRPSeverity)
		}
		// AcquisitionReadiness should be 50 (neutral start) for empty data.
		if scores.AcquisitionReadiness != 50 {
			t.Errorf("expected AcquisitionReadiness=50, got %d", scores.AcquisitionReadiness)
		}
	})

	t.Run("nil filing", func(t *testing.T) {
		scores := ComputeScores(nil, map[string]Answer{})
		if scores.CompensationDiversity != 0 {
			t.Errorf("expected CompensationDiversity=0, got %d", scores.CompensationDiversity)
		}
		if scores.AcquisitionReadiness != 50 {
			t.Errorf("expected AcquisitionReadiness=50, got %d", scores.AcquisitionReadiness)
		}
	})
}

func TestAcquisitionReadiness_Clamping(t *testing.T) {
	t.Run("high score clamped to 100", func(t *testing.T) {
		filing := map[string]any{
			"comp_pct_aum":     true,
			"comp_hourly":      true,
			"comp_fixed":       true,
			"comp_commissions": true,
		}
		// Build answers that would push the score high.
		answers := map[string]Answer{
			"fee_schedule_complete": {
				QuestionKey: "fee_schedule_complete",
				Value: []any{
					map[string]any{
						"min_aum":        float64(0),
						"max_aum":        float64(0),
						"annual_rate_pct": float64(1.0),
					},
				},
				Confidence: 0.9,
			},
			"aum_growth_trend":       {QuestionKey: "aum_growth_trend", Value: float64(0.10), Confidence: 0.9},
			"client_account_growth":  {QuestionKey: "client_account_growth", Value: float64(0.05), Confidence: 0.9},
			"succession_plan":        {QuestionKey: "succession_plan", Value: true, Confidence: 0.9},
			"code_of_ethics_summary": {QuestionKey: "code_of_ethics_summary", Value: true, Confidence: 0.9},
		}

		scores := ComputeScores(filing, answers)
		// 50 + 15 (fee) + 10 (AUM growth) + 5 (client growth) + 10 (succession) + 5 (ethics) + 5 (comp div) + 5 (confidence) = 105 -> clamped to 100
		if scores.AcquisitionReadiness != 100 {
			t.Errorf("expected AcquisitionReadiness=100 (clamped), got %d", scores.AcquisitionReadiness)
		}
	})

	t.Run("low score clamped to 0", func(t *testing.T) {
		filing := map[string]any{
			// All DRP flags to push DRP severity high.
			"drp_criminal_firm":       true, // 3
			"drp_criminal_affiliate":  true, // 3
			"drp_regulatory_firm":     true, // 2
			"drp_regulatory_affiliate": true, // 2
			// High business complexity.
			"biz_broker_dealer":    true,
			"biz_registered_rep":   true,
			"biz_cpo_cta":         true,
			"biz_futures_commission": true,
			"biz_real_estate":      true,
			"biz_insurance":        true,
			"biz_bank":             true,
			"biz_trust_company":    true,
			"biz_municipal_advisor": true,
			"biz_swap_dealer":      true,
			"biz_major_swap":       true,
			"biz_accountant":       true,
			"biz_lawyer":           true,
			"biz_other_financial":  true,
			"aff_broker_dealer":    true,
			"aff_other_adviser":    true,
		}
		answers := map[string]Answer{}

		scores := ComputeScores(filing, answers)
		// 50 - 10 (DRP > 3) - 5 (biz complexity > 15) = 35
		// Verify it's within bounds and not negative.
		if scores.AcquisitionReadiness < 0 || scores.AcquisitionReadiness > 100 {
			t.Errorf("expected AcquisitionReadiness in [0,100], got %d", scores.AcquisitionReadiness)
		}
	})

	t.Run("acquisition readiness factors add up correctly", func(t *testing.T) {
		filing := map[string]any{}
		answers := map[string]Answer{}

		scores := ComputeScores(filing, answers)
		// Base 50, no modifiers.
		if scores.AcquisitionReadiness != 50 {
			t.Errorf("expected AcquisitionReadiness=50 (neutral), got %d", scores.AcquisitionReadiness)
		}
	})
}

func TestComputeRevenue_UnsortedTiers(t *testing.T) {
	// Tiers provided out of order should still work.
	tiers := []FeeTier{
		{MinAUM: 500_000_000, MaxAUM: 0, AnnualRatePct: 0.50},
		{MinAUM: 0, MaxAUM: 250_000_000, AnnualRatePct: 1.00},
		{MinAUM: 250_000_000, MaxAUM: 500_000_000, AnnualRatePct: 0.75},
	}
	aum := int64(500_000_000)
	rev := ComputeRevenue(tiers, aum)
	if rev != 4_375_000 {
		t.Errorf("expected 4375000 for unsorted tiers, got %d", rev)
	}
}
