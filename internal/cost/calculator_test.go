package cost

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testRates() Rates {
	return Rates{
		Anthropic: map[string]ModelRate{
			"haiku": {
				Input: 0.80, Output: 4.00,
				BatchDiscount: 0.5, CacheWriteMul: 1.25, CacheReadMul: 0.1,
			},
			"sonnet": {
				Input: 3.00, Output: 15.00,
				BatchDiscount: 0.5, CacheWriteMul: 1.25, CacheReadMul: 0.1,
			},
		},
		Jina:       JinaRate{PerMTok: 0.02},
		Perplexity: PerplexityRate{PerQuery: 0.005},
		Firecrawl:  FirecrawlRate{PlanMonthly: 19.0, CreditsIncluded: 3000},
	}
}

func TestClaude(t *testing.T) {
	t.Parallel()
	calc := NewCalculator(testRates())

	tests := []struct {
		name       string
		model      string
		isBatch    bool
		input      int
		output     int
		cacheWrite int
		cacheRead  int
		want       float64
	}{
		{
			name: "haiku non-batch simple",
			model: "haiku", isBatch: false,
			input: 1000000, output: 100000,
			want: 0.80 + 0.40, // 0.80 input + 0.40 output
		},
		{
			name: "haiku batch 50% discount",
			model: "haiku", isBatch: true,
			input: 1000000, output: 100000,
			want: (0.80 * 0.5) + (0.40 * 0.5), // 0.40 + 0.20
		},
		{
			name: "haiku with cache",
			model: "haiku", isBatch: false,
			input: 500000, output: 50000,
			cacheWrite: 200000, cacheRead: 300000,
			// in: 0.5M/1M * 0.80 = 0.40
			// out: 0.05M/1M * 4.00 = 0.20
			// cw: 0.2M/1M * 0.80 * 1.25 = 0.20
			// cr: 0.3M/1M * 0.80 * 0.1 = 0.024
			want: 0.40 + 0.20 + 0.20 + 0.024,
		},
		{
			name: "sonnet non-batch",
			model: "sonnet", isBatch: false,
			input: 1000000, output: 100000,
			want: 3.00 + 1.50, // 3.00 input + 1.50 output
		},
		{
			name: "unknown model returns 0",
			model: "unknown", isBatch: false,
			input: 1000000, output: 1000000,
			want: 0,
		},
		{
			name: "zero tokens returns 0",
			model: "haiku", isBatch: false,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := calc.Claude(tt.model, tt.isBatch, tt.input, tt.output, tt.cacheWrite, tt.cacheRead)
			assert.InDelta(t, tt.want, got, 0.001)
		})
	}
}

func TestJina(t *testing.T) {
	t.Parallel()
	calc := NewCalculator(testRates())

	tests := []struct {
		name   string
		tokens int
		want   float64
	}{
		{"1M tokens", 1000000, 0.02},
		{"500K tokens", 500000, 0.01},
		{"zero tokens", 0, 0},
		{"small", 2150, 2150.0 / 1e6 * 0.02},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := calc.Jina(tt.tokens)
			assert.InDelta(t, tt.want, got, 0.0001)
		})
	}
}

func TestPerplexityQuery(t *testing.T) {
	t.Parallel()
	calc := NewCalculator(testRates())
	assert.InDelta(t, 0.005, calc.PerplexityQuery(), 0.0001)
}

func TestDefaultRates(t *testing.T) {
	t.Parallel()
	rates := DefaultRates()

	assert.Contains(t, rates.Anthropic, "claude-haiku-4-5-20251001")
	assert.Contains(t, rates.Anthropic, "claude-sonnet-4-5-20250929")
	assert.Contains(t, rates.Anthropic, "claude-opus-4-6")
	assert.InDelta(t, 0.02, rates.Jina.PerMTok, 0.001)
	assert.InDelta(t, 0.005, rates.Perplexity.PerQuery, 0.001)
}
