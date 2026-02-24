package advextract

import (
	"fmt"
	"sync"
)

// Batch API pricing per 1M tokens (50% discount from standard).
const (
	HaikuInputPer1M          = 0.40  // $0.80 standard / 2
	HaikuOutputPer1M         = 2.00  // $4.00 standard / 2
	HaikuCacheWritePer1M     = 0.50  // $1.00 standard / 2
	HaikuCacheReadPer1M      = 0.04  // $0.08 standard / 2

	SonnetInputPer1M         = 1.50  // $3.00 standard / 2
	SonnetOutputPer1M        = 7.50  // $15.00 standard / 2
	SonnetCacheWritePer1M    = 1.875 // $3.75 standard / 2
	SonnetCacheReadPer1M     = 0.15  // $0.30 standard / 2

	OpusInputPer1M           = 7.50  // $15.00 standard / 2
	OpusOutputPer1M          = 37.50 // $75.00 standard / 2
	OpusCacheWritePer1M      = 9.375 // $18.75 standard / 2
	OpusCacheReadPer1M       = 0.75  // $1.50 standard / 2
)

// CostTracker tracks per-advisor and total extraction costs.
type CostTracker struct {
	mu       sync.Mutex
	advisors map[int]*AdvisorCost
	maxCost  float64 // per-advisor budget (0 = unlimited)
}

// AdvisorCost tracks costs for a single advisor.
type AdvisorCost struct {
	CRDNumber    int
	InputTokens  int64
	OutputTokens int64
	CacheWrite   int64
	CacheRead    int64
	CostUSD      float64
	BudgetExceeded bool
}

// NewCostTracker creates a cost tracker with optional per-advisor budget.
func NewCostTracker(maxCostPerAdvisor float64) *CostTracker {
	return &CostTracker{
		advisors: make(map[int]*AdvisorCost),
		maxCost:  maxCostPerAdvisor,
	}
}

// RecordUsage records token usage for an advisor at a specific tier.
func (ct *CostTracker) RecordUsage(crd int, tier int, inputTokens, outputTokens, cacheWrite, cacheRead int64) float64 {
	cost := CalculateCost(tier, inputTokens, outputTokens, cacheWrite, cacheRead)

	ct.mu.Lock()
	defer ct.mu.Unlock()

	ac, ok := ct.advisors[crd]
	if !ok {
		ac = &AdvisorCost{CRDNumber: crd}
		ct.advisors[crd] = ac
	}

	ac.InputTokens += inputTokens
	ac.OutputTokens += outputTokens
	ac.CacheWrite += cacheWrite
	ac.CacheRead += cacheRead
	ac.CostUSD += cost

	return cost
}

// CheckBudget returns true if the advisor has exceeded their budget.
func (ct *CostTracker) CheckBudget(crd int) bool {
	if ct.maxCost <= 0 {
		return false
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()

	ac, ok := ct.advisors[crd]
	if !ok {
		return false
	}

	if ac.CostUSD >= ct.maxCost {
		ac.BudgetExceeded = true
		return true
	}
	return false
}

// AdvisorTotal returns the cost summary for a specific advisor.
func (ct *CostTracker) AdvisorTotal(crd int) *AdvisorCost {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ac, ok := ct.advisors[crd]
	if !ok {
		return &AdvisorCost{CRDNumber: crd}
	}
	copy := *ac
	return &copy
}

// TotalCost returns the total cost across all advisors.
func (ct *CostTracker) TotalCost() float64 {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	var total float64
	for _, ac := range ct.advisors {
		total += ac.CostUSD
	}
	return total
}

// TotalAdvisors returns the number of advisors tracked.
func (ct *CostTracker) TotalAdvisors() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return len(ct.advisors)
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
	case 3:
		inputRate = OpusInputPer1M
		outputRate = OpusOutputPer1M
		cacheWriteRate = OpusCacheWritePer1M
		cacheReadRate = OpusCacheReadPer1M
	default:
		return 0 // structured bypass = free
	}

	cost := float64(inputTokens) / 1_000_000 * inputRate
	cost += float64(outputTokens) / 1_000_000 * outputRate
	cost += float64(cacheWrite) / 1_000_000 * cacheWriteRate
	cost += float64(cacheRead) / 1_000_000 * cacheReadRate

	return cost
}

// EstimateBatchCost estimates the total cost for extracting a batch of advisors.
// v2 question bank: ~29 bypass ($0), ~160 Haiku T1, ~8 optional Sonnet T2, ~15 Go-computed ($0).
func EstimateBatchCost(advisorCount int, maxTier int) string {
	var totalCost float64
	var breakdown []string

	// Structured bypass: ~29 questions, $0
	bypass := 29
	breakdown = append(breakdown, fmt.Sprintf("  Structured bypass (T0): %d questions, $0.00", bypass))

	// Go-computed metrics: ~15, $0
	breakdown = append(breakdown, "  Go-computed metrics: ~15 questions, $0.00")

	// T1 Haiku: ~160 questions, avg ~2K input + 100 output each (reduced max_tokens=256)
	if maxTier >= 1 {
		t1Questions := 160
		// With prompt caching, ~80% cache hit after primer → cache_read tokens
		t1InputFresh := int64(t1Questions * 500)                     // uncached portion
		t1CacheRead := int64(t1Questions * 1500)                     // cached system prompt
		t1Output := int64(t1Questions * 100)                         // smaller outputs with 256 max
		t1Cost := CalculateCost(1, t1InputFresh, t1Output, 0, t1CacheRead)
		totalCost += t1Cost * float64(advisorCount)
		breakdown = append(breakdown, fmt.Sprintf("  T1 Haiku: ~%d questions, ~$%.3f/advisor", t1Questions, t1Cost))
	}

	// T2 Sonnet: ~8 judgment questions, avg ~5K input + 400 output
	if maxTier >= 2 {
		t2Questions := 8
		t2Cost := CalculateCost(2, int64(t2Questions*5000), int64(t2Questions*400), 0, 0)
		totalCost += t2Cost * float64(advisorCount)
		breakdown = append(breakdown, fmt.Sprintf("  T2 Sonnet: ~%d questions, ~$%.3f/advisor", t2Questions, t2Cost))
	}

	return fmt.Sprintf("Estimated cost for %d advisors (max tier %d):\n%s\n  Total: ~$%.2f (~$%.3f/advisor)",
		advisorCount, maxTier,
		joinLines(breakdown),
		totalCost,
		totalCost/float64(advisorCount))
}

func joinLines(lines []string) string {
	result := ""
	for _, l := range lines {
		result += l + "\n"
	}
	return result
}
