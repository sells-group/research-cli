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
			name:  "haiku non-batch simple",
			model: "haiku", isBatch: false,
			input: 1000000, output: 100000,
			want: 0.80 + 0.40, // 0.80 input + 0.40 output
		},
		{
			name:  "haiku batch 50% discount",
			model: "haiku", isBatch: true,
			input: 1000000, output: 100000,
			want: (0.80 * 0.5) + (0.40 * 0.5), // 0.40 + 0.20
		},
		{
			name:  "haiku with cache",
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
			name:  "sonnet non-batch",
			model: "sonnet", isBatch: false,
			input: 1000000, output: 100000,
			want: 3.00 + 1.50, // 3.00 input + 1.50 output
		},
		{
			name:  "unknown model returns 0",
			model: "unknown", isBatch: false,
			input: 1000000, output: 1000000,
			want: 0,
		},
		{
			name:  "zero tokens returns 0",
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
	assert.InDelta(t, 19.0, rates.Firecrawl.PlanMonthly, 0.001)
	assert.InDelta(t, 3000.0, rates.Firecrawl.CreditsIncluded, 0.001)
}

func TestRatesFromConfig_EmptyConfig(t *testing.T) {
	t.Parallel()
	rates := RatesFromConfig(PricingConfig{})
	defaults := DefaultRates()

	assert.Equal(t, defaults.Jina, rates.Jina)
	assert.Equal(t, defaults.Perplexity, rates.Perplexity)
	assert.Equal(t, defaults.Firecrawl, rates.Firecrawl)
	assert.Len(t, rates.Anthropic, len(defaults.Anthropic))
	for model, defRate := range defaults.Anthropic {
		assert.Equal(t, defRate, rates.Anthropic[model], "model %s should match default", model)
	}
}

func TestRatesFromConfig_OverrideAnthropicModel(t *testing.T) {
	t.Parallel()
	cfg := PricingConfig{
		Anthropic: map[string]ModelPricing{
			"claude-haiku-4-5-20251001": {
				Input:  1.00,
				Output: 5.00,
			},
		},
	}
	rates := RatesFromConfig(cfg)

	haiku := rates.Anthropic["claude-haiku-4-5-20251001"]
	assert.InDelta(t, 1.00, haiku.Input, 0.001)
	assert.InDelta(t, 5.00, haiku.Output, 0.001)
	// BatchDiscount and CacheWriteMul should retain defaults since config values are zero
	defaults := DefaultRates()
	assert.InDelta(t, defaults.Anthropic["claude-haiku-4-5-20251001"].BatchDiscount, haiku.BatchDiscount, 0.001)
	assert.InDelta(t, defaults.Anthropic["claude-haiku-4-5-20251001"].CacheWriteMul, haiku.CacheWriteMul, 0.001)
	assert.InDelta(t, defaults.Anthropic["claude-haiku-4-5-20251001"].CacheReadMul, haiku.CacheReadMul, 0.001)

	// Other models should still have defaults
	sonnet := rates.Anthropic["claude-sonnet-4-5-20250929"]
	assert.InDelta(t, defaults.Anthropic["claude-sonnet-4-5-20250929"].Input, sonnet.Input, 0.001)
}

func TestRatesFromConfig_OverrideJinaPerplexityFirecrawl(t *testing.T) {
	t.Parallel()
	cfg := PricingConfig{
		Jina:       JinaPricing{PerMTok: 0.05},
		Perplexity: PerplexityPricing{PerQuery: 0.01},
		Firecrawl:  FirecrawlPricing{PlanMonthly: 49.0, CreditsIncluded: 10000},
	}
	rates := RatesFromConfig(cfg)

	assert.InDelta(t, 0.05, rates.Jina.PerMTok, 0.001)
	assert.InDelta(t, 0.01, rates.Perplexity.PerQuery, 0.001)
	assert.InDelta(t, 49.0, rates.Firecrawl.PlanMonthly, 0.001)
	assert.InDelta(t, 10000.0, rates.Firecrawl.CreditsIncluded, 0.001)
}

func TestRatesFromConfig_ZeroValuesKeepDefaults(t *testing.T) {
	t.Parallel()
	cfg := PricingConfig{
		Jina:       JinaPricing{PerMTok: 0},
		Perplexity: PerplexityPricing{PerQuery: 0},
		Firecrawl:  FirecrawlPricing{PlanMonthly: 0, CreditsIncluded: 0},
	}
	rates := RatesFromConfig(cfg)
	defaults := DefaultRates()

	assert.InDelta(t, defaults.Jina.PerMTok, rates.Jina.PerMTok, 0.001)
	assert.InDelta(t, defaults.Perplexity.PerQuery, rates.Perplexity.PerQuery, 0.001)
	assert.InDelta(t, defaults.Firecrawl.PlanMonthly, rates.Firecrawl.PlanMonthly, 0.001)
	assert.InDelta(t, defaults.Firecrawl.CreditsIncluded, rates.Firecrawl.CreditsIncluded, 0.001)
}

func TestRatesFromConfig_NewAnthropicModel(t *testing.T) {
	t.Parallel()
	cfg := PricingConfig{
		Anthropic: map[string]ModelPricing{
			"custom-model": {
				Input:         2.00,
				Output:        10.00,
				BatchDiscount: 0.6,
				CacheWriteMul: 1.5,
				CacheReadMul:  0.2,
			},
		},
	}
	rates := RatesFromConfig(cfg)

	custom := rates.Anthropic["custom-model"]
	assert.InDelta(t, 2.00, custom.Input, 0.001)
	assert.InDelta(t, 10.00, custom.Output, 0.001)
	assert.InDelta(t, 0.6, custom.BatchDiscount, 0.001)
	assert.InDelta(t, 1.5, custom.CacheWriteMul, 0.001)
	assert.InDelta(t, 0.2, custom.CacheReadMul, 0.001)
}

func TestClaude_BatchWithCache(t *testing.T) {
	t.Parallel()
	calc := NewCalculator(testRates())

	// haiku batch with cache: all multipliers apply
	got := calc.Claude("haiku", true, 1000000, 100000, 500000, 200000)
	// in: 1M/1M * 0.80 * 0.5 = 0.40
	// out: 0.1M/1M * 4.00 * 0.5 = 0.20
	// cw: 0.5M/1M * 0.80 * 1.25 * 0.5 = 0.25
	// cr: 0.2M/1M * 0.80 * 0.1 * 0.5 = 0.008
	want := 0.40 + 0.20 + 0.25 + 0.008
	assert.InDelta(t, want, got, 0.001)
}

func TestNewCalculator(t *testing.T) {
	t.Parallel()
	rates := testRates()
	calc := NewCalculator(rates)
	assert.NotNil(t, calc)
	assert.Equal(t, rates, calc.rates)
}
