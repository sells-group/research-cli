package peextract

import (
	"fmt"
	"sync"
)

// Batch API pricing per 1M tokens (50% discount from standard).
const (
	HaikuInputPer1M      = 0.40
	HaikuOutputPer1M     = 2.00
	HaikuCacheWritePer1M = 0.50
	HaikuCacheReadPer1M  = 0.04

	SonnetInputPer1M      = 1.50
	SonnetOutputPer1M     = 7.50
	SonnetCacheWritePer1M = 1.875
	SonnetCacheReadPer1M  = 0.15
)

// CostTracker tracks per-firm and total extraction costs.
type CostTracker struct {
	mu    sync.Mutex
	firms map[int64]*FirmCost
	max   float64 // per-firm budget (0 = unlimited)
}

// FirmCost tracks costs for a single PE firm.
type FirmCost struct {
	PEFirmID       int64
	InputTokens    int64
	OutputTokens   int64
	CacheWrite     int64
	CacheRead      int64
	CostUSD        float64
	BudgetExceeded bool
}

// NewCostTracker creates a cost tracker with optional per-firm budget.
func NewCostTracker(maxCostPerFirm float64) *CostTracker {
	return &CostTracker{
		firms: make(map[int64]*FirmCost),
		max:   maxCostPerFirm,
	}
}

// RecordUsage records token usage for a firm at a specific tier.
func (ct *CostTracker) RecordUsage(firmID int64, tier int, inputTokens, outputTokens, cacheWrite, cacheRead int64) float64 {
	cost := CalculateCost(tier, inputTokens, outputTokens, cacheWrite, cacheRead)

	ct.mu.Lock()
	defer ct.mu.Unlock()

	fc, ok := ct.firms[firmID]
	if !ok {
		fc = &FirmCost{PEFirmID: firmID}
		ct.firms[firmID] = fc
	}

	fc.InputTokens += inputTokens
	fc.OutputTokens += outputTokens
	fc.CacheWrite += cacheWrite
	fc.CacheRead += cacheRead
	fc.CostUSD += cost

	return cost
}

// CheckBudget returns true if the firm has exceeded their budget.
func (ct *CostTracker) CheckBudget(firmID int64) bool {
	if ct.max <= 0 {
		return false
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()

	fc, ok := ct.firms[firmID]
	if !ok {
		return false
	}

	if fc.CostUSD >= ct.max {
		fc.BudgetExceeded = true
		return true
	}
	return false
}

// FirmTotal returns the cost summary for a specific firm.
func (ct *CostTracker) FirmTotal(firmID int64) *FirmCost {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	fc, ok := ct.firms[firmID]
	if !ok {
		return &FirmCost{PEFirmID: firmID}
	}
	c := *fc
	return &c
}

// TotalCost returns the total cost across all firms.
func (ct *CostTracker) TotalCost() float64 {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	var total float64
	for _, fc := range ct.firms {
		total += fc.CostUSD
	}
	return total
}

// CalculateCost computes the dollar cost for token usage at a given tier (Batch API pricing).
func CalculateCost(tier int, inputTokens, outputTokens, cacheWrite, cacheRead int64) float64 {
	var inputRate, outputRate, cacheWriteRate, cacheReadRate float64

	switch tier {
	case 1:
		inputRate = HaikuInputPer1M
		outputRate = HaikuOutputPer1M
		cacheWriteRate = HaikuCacheWritePer1M
		cacheReadRate = HaikuCacheReadPer1M
	case 2:
		inputRate = SonnetInputPer1M
		outputRate = SonnetOutputPer1M
		cacheWriteRate = SonnetCacheWritePer1M
		cacheReadRate = SonnetCacheReadPer1M
	default:
		return 0
	}

	cost := float64(inputTokens) / 1_000_000 * inputRate
	cost += float64(outputTokens) / 1_000_000 * outputRate
	cost += float64(cacheWrite) / 1_000_000 * cacheWriteRate
	cost += float64(cacheRead) / 1_000_000 * cacheReadRate

	return cost
}

// EstimateBatchCost estimates the total cost for extracting a batch of PE firms.
func EstimateBatchCost(firmCount int, maxTier int) string {
	var totalCost float64
	var breakdown []string

	// T1 Haiku: 25 questions, avg ~3K input + 150 output each
	if maxTier >= 1 {
		t1Questions := 25
		t1InputFresh := int64(t1Questions * 800)
		t1CacheRead := int64(t1Questions * 2200)
		t1Output := int64(t1Questions * 150)
		t1Cost := CalculateCost(1, t1InputFresh, t1Output, 0, t1CacheRead)
		totalCost += t1Cost * float64(firmCount)
		breakdown = append(breakdown, fmt.Sprintf("  T1 Haiku: ~%d questions, ~$%.4f/firm", t1Questions, t1Cost))
	}

	// T2 Sonnet: 4 synthesis + 10 blog intel questions, avg ~5K input + 400 output
	if maxTier >= 2 {
		t2Questions := 14
		t2Cost := CalculateCost(2, int64(t2Questions*5000), int64(t2Questions*400), 0, 0)
		totalCost += t2Cost * float64(firmCount)
		breakdown = append(breakdown, fmt.Sprintf("  T2 Sonnet: ~%d questions, ~$%.4f/firm", t2Questions, t2Cost))
	}

	return fmt.Sprintf("Estimated cost for %d PE firms (max tier %d):\n%s\n  Total: ~$%.2f (~$%.4f/firm)",
		firmCount, maxTier,
		joinLines(breakdown),
		totalCost,
		totalCost/float64(firmCount))
}

func joinLines(lines []string) string {
	result := ""
	for _, l := range lines {
		result += l + "\n"
	}
	return result
}
