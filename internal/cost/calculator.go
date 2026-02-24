// Package cost tracks and calculates API usage costs across providers.
package cost

// Rates holds per-provider pricing configuration.
type Rates struct {
	Anthropic  map[string]ModelRate `yaml:"anthropic" mapstructure:"anthropic"`
	Jina       JinaRate             `yaml:"jina" mapstructure:"jina"`
	Perplexity PerplexityRate       `yaml:"perplexity" mapstructure:"perplexity"`
	Firecrawl  FirecrawlRate        `yaml:"firecrawl" mapstructure:"firecrawl"`
}

// ModelRate holds per-model token pricing (per million tokens).
type ModelRate struct {
	Input         float64 `yaml:"input" mapstructure:"input"`
	Output        float64 `yaml:"output" mapstructure:"output"`
	BatchDiscount float64 `yaml:"batch_discount" mapstructure:"batch_discount"`
	CacheWriteMul float64 `yaml:"cache_write_mul" mapstructure:"cache_write_mul"`
	CacheReadMul  float64 `yaml:"cache_read_mul" mapstructure:"cache_read_mul"`
}

// JinaRate holds Jina Reader pricing.
type JinaRate struct {
	PerMTok float64 `yaml:"per_mtok" mapstructure:"per_mtok"`
}

// PerplexityRate holds Perplexity pricing.
type PerplexityRate struct {
	PerQuery float64 `yaml:"per_query" mapstructure:"per_query"`
}

// FirecrawlRate holds Firecrawl pricing.
type FirecrawlRate struct {
	PlanMonthly     float64 `yaml:"plan_monthly" mapstructure:"plan_monthly"`
	CreditsIncluded float64 `yaml:"credits_included" mapstructure:"credits_included"`
}

// Calculator computes costs for API usage.
type Calculator struct {
	rates Rates
}

// NewCalculator creates a Calculator with the given rates.
func NewCalculator(rates Rates) *Calculator {
	return &Calculator{rates: rates}
}

// Claude computes the cost for a Claude API call.
func (c *Calculator) Claude(model string, isBatch bool, input, output, cacheWrite, cacheRead int) float64 {
	rate, ok := c.rates.Anthropic[model]
	if !ok {
		return 0
	}

	batchMul := 1.0
	if isBatch {
		batchMul = rate.BatchDiscount
	}

	inCost := (float64(input) / 1e6) * rate.Input * batchMul
	outCost := (float64(output) / 1e6) * rate.Output * batchMul
	cwCost := (float64(cacheWrite) / 1e6) * rate.Input * rate.CacheWriteMul * batchMul
	crCost := (float64(cacheRead) / 1e6) * rate.Input * rate.CacheReadMul * batchMul

	return inCost + outCost + cwCost + crCost
}

// Jina computes the cost for Jina Reader token usage.
func (c *Calculator) Jina(tokens int) float64 {
	return (float64(tokens) / 1e6) * c.rates.Jina.PerMTok
}

// PerplexityQuery returns the flat cost per Perplexity query.
func (c *Calculator) PerplexityQuery() float64 {
	return c.rates.Perplexity.PerQuery
}

// RatesFromConfig converts config pricing into cost rates, falling back
// to DefaultRates() for any zero-value fields.
func RatesFromConfig(cfg PricingConfig) Rates {
	defaults := DefaultRates()

	rates := Rates{
		Anthropic:  make(map[string]ModelRate),
		Jina:       defaults.Jina,
		Perplexity: defaults.Perplexity,
		Firecrawl:  defaults.Firecrawl,
	}

	// Copy default Anthropic rates first.
	for k, v := range defaults.Anthropic {
		rates.Anthropic[k] = v
	}

	// Override with config values where set.
	for model, mp := range cfg.Anthropic {
		r := ModelRate{}
		if existing, ok := rates.Anthropic[model]; ok {
			r = existing
		}
		if mp.Input > 0 {
			r.Input = mp.Input
		}
		if mp.Output > 0 {
			r.Output = mp.Output
		}
		if mp.BatchDiscount > 0 {
			r.BatchDiscount = mp.BatchDiscount
		}
		if mp.CacheWriteMul > 0 {
			r.CacheWriteMul = mp.CacheWriteMul
		}
		if mp.CacheReadMul > 0 {
			r.CacheReadMul = mp.CacheReadMul
		}
		rates.Anthropic[model] = r
	}

	if cfg.Jina.PerMTok > 0 {
		rates.Jina.PerMTok = cfg.Jina.PerMTok
	}
	if cfg.Perplexity.PerQuery > 0 {
		rates.Perplexity.PerQuery = cfg.Perplexity.PerQuery
	}
	if cfg.Firecrawl.PlanMonthly > 0 {
		rates.Firecrawl.PlanMonthly = cfg.Firecrawl.PlanMonthly
	}
	if cfg.Firecrawl.CreditsIncluded > 0 {
		rates.Firecrawl.CreditsIncluded = cfg.Firecrawl.CreditsIncluded
	}

	return rates
}

// PricingConfig mirrors config.PricingConfig to avoid an import cycle.
// Used by RatesFromConfig to convert config types into cost types.
type PricingConfig struct {
	Anthropic  map[string]ModelPricing
	Jina       JinaPricing
	Perplexity PerplexityPricing
	Firecrawl  FirecrawlPricing
}

// ModelPricing mirrors config.ModelPricing.
type ModelPricing struct {
	Input         float64
	Output        float64
	BatchDiscount float64
	CacheWriteMul float64
	CacheReadMul  float64
}

// JinaPricing mirrors config.JinaPricing.
type JinaPricing struct {
	PerMTok float64
}

// PerplexityPricing mirrors config.PerplexityPricing.
type PerplexityPricing struct {
	PerQuery float64
}

// FirecrawlPricing mirrors config.FirecrawlPricing.
type FirecrawlPricing struct {
	PlanMonthly     float64
	CreditsIncluded float64
}

// DefaultRates returns the default pricing rates.
func DefaultRates() Rates {
	return Rates{
		Anthropic: map[string]ModelRate{
			"claude-haiku-4-5-20251001": {
				Input: 0.80, Output: 4.00,
				BatchDiscount: 0.5, CacheWriteMul: 1.25, CacheReadMul: 0.1,
			},
			"claude-sonnet-4-5-20250929": {
				Input: 3.00, Output: 15.00,
				BatchDiscount: 0.5, CacheWriteMul: 1.25, CacheReadMul: 0.1,
			},
			"claude-opus-4-6": {
				Input: 15.00, Output: 75.00,
				BatchDiscount: 0.5, CacheWriteMul: 1.25, CacheReadMul: 0.1,
			},
		},
		Jina:       JinaRate{PerMTok: 0.02},
		Perplexity: PerplexityRate{PerQuery: 0.005},
		Firecrawl:  FirecrawlRate{PlanMonthly: 19.00, CreditsIncluded: 3000},
	}
}
