package advextract

import (
	"testing"
)

func TestCalculateCost_Haiku(t *testing.T) {
	cost := CalculateCost(1, 10000, 500, 0, 0)
	// Input: 10000/1M * $0.40 = $0.004
	// Output: 500/1M * $2.00 = $0.001
	// Total: $0.005
	if cost < 0.004 || cost > 0.006 {
		t.Errorf("expected ~$0.005 for Haiku, got $%.4f", cost)
	}
}

func TestCalculateCost_Sonnet(t *testing.T) {
	cost := CalculateCost(2, 10000, 500, 0, 0)
	// Input: 10000/1M * $1.50 = $0.015
	// Output: 500/1M * $7.50 = $0.00375
	// Total: ~$0.01875
	if cost < 0.017 || cost > 0.020 {
		t.Errorf("expected ~$0.019 for Sonnet, got $%.4f", cost)
	}
}

func TestCalculateCost_Opus(t *testing.T) {
	cost := CalculateCost(3, 10000, 500, 0, 0)
	// Input: 10000/1M * $7.50 = $0.075
	// Output: 500/1M * $37.50 = $0.01875
	// Total: ~$0.09375
	if cost < 0.090 || cost > 0.10 {
		t.Errorf("expected ~$0.094 for Opus, got $%.4f", cost)
	}
}

func TestCalculateCost_StructuredBypass(t *testing.T) {
	cost := CalculateCost(0, 10000, 500, 0, 0)
	if cost != 0 {
		t.Errorf("expected $0 for structured bypass (tier 0), got $%.4f", cost)
	}
}

func TestCostTracker_RecordAndTotal(t *testing.T) {
	ct := NewCostTracker(0)

	ct.RecordUsage(12345, 1, 50000, 2000, 0, 0)
	ct.RecordUsage(12345, 2, 100000, 5000, 0, 0)
	ct.RecordUsage(67890, 1, 30000, 1000, 0, 0)

	if ct.TotalAdvisors() != 2 {
		t.Errorf("expected 2 advisors, got %d", ct.TotalAdvisors())
	}

	total := ct.TotalCost()
	if total <= 0 {
		t.Error("expected positive total cost")
	}

	advisor1 := ct.AdvisorTotal(12345)
	if advisor1.InputTokens != 150000 {
		t.Errorf("expected 150000 input tokens for CRD 12345, got %d", advisor1.InputTokens)
	}
}

func TestCostTracker_Budget(t *testing.T) {
	ct := NewCostTracker(0.01) // $0.01 budget

	// Record enough usage to exceed budget.
	ct.RecordUsage(12345, 2, 100000, 10000, 0, 0) // ~$0.225

	if !ct.CheckBudget(12345) {
		t.Error("expected budget to be exceeded")
	}

	// Different advisor should not be affected.
	if ct.CheckBudget(67890) {
		t.Error("expected budget not exceeded for different advisor")
	}
}

func TestCostTracker_UnlimitedBudget(t *testing.T) {
	ct := NewCostTracker(0) // unlimited

	ct.RecordUsage(12345, 3, 1000000, 100000, 0, 0)

	if ct.CheckBudget(12345) {
		t.Error("expected no budget exceeded with unlimited (0) max cost")
	}
}

func TestEstimateBatchCost(t *testing.T) {
	estimate := EstimateBatchCost(10, 3)
	if estimate == "" {
		t.Error("expected non-empty estimate")
	}
	if !containsSubstr(estimate, "10 advisors") {
		t.Error("estimate should mention advisor count")
	}
	if !containsSubstr(estimate, "T1 Haiku") {
		t.Error("estimate should mention T1")
	}
	if !containsSubstr(estimate, "T3 Opus") {
		t.Error("estimate should mention T3 for max tier 3")
	}

	// T1 only estimate should not mention T2/T3.
	estimateT1 := EstimateBatchCost(10, 1)
	if containsSubstr(estimateT1, "T2 Sonnet") {
		t.Error("T1-only estimate should not mention T2")
	}
}
