package config

import (
	"strings"

	"github.com/rotisserie/eris"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config holds the full application configuration.
type Config struct {
	Store      StoreConfig      `yaml:"store" mapstructure:"store"`
	Notion     NotionConfig     `yaml:"notion" mapstructure:"notion"`
	Jina       JinaConfig       `yaml:"jina" mapstructure:"jina"`
	Firecrawl  FirecrawlConfig  `yaml:"firecrawl" mapstructure:"firecrawl"`
	Perplexity PerplexityConfig `yaml:"perplexity" mapstructure:"perplexity"`
	Anthropic  AnthropicConfig  `yaml:"anthropic" mapstructure:"anthropic"`
	Salesforce SalesforceConfig `yaml:"salesforce" mapstructure:"salesforce"`
	ToolJet    ToolJetConfig    `yaml:"tooljet" mapstructure:"tooljet"`
	PPP        PPPConfig        `yaml:"ppp" mapstructure:"ppp"`
	Pricing    PricingConfig    `yaml:"pricing" mapstructure:"pricing"`
	Crawl      CrawlConfig      `yaml:"crawl" mapstructure:"crawl"`
	Scrape     ScrapeConfig     `yaml:"scrape" mapstructure:"scrape"`
	Pipeline   PipelineConfig   `yaml:"pipeline" mapstructure:"pipeline"`
	Batch      BatchConfig      `yaml:"batch" mapstructure:"batch"`
	Server     ServerConfig     `yaml:"server" mapstructure:"server"`
	Log        LogConfig        `yaml:"log" mapstructure:"log"`
	Fedsync    FedsyncConfig    `yaml:"fedsync" mapstructure:"fedsync"`
}

// FedsyncConfig configures the federal data sync pipeline.
type FedsyncConfig struct {
	DatabaseURL    string    `yaml:"database_url" mapstructure:"database_url"`
	TempDir        string    `yaml:"temp_dir" mapstructure:"temp_dir"`
	SAMKey         string    `yaml:"sam_api_key" mapstructure:"sam_api_key"`
	FREDKey        string    `yaml:"fred_api_key" mapstructure:"fred_api_key"`
	BLSKey         string    `yaml:"bls_api_key" mapstructure:"bls_api_key"`
	CensusKey      string    `yaml:"census_api_key" mapstructure:"census_api_key"`
	EDGARUserAgent string    `yaml:"edgar_user_agent" mapstructure:"edgar_user_agent"`
	N8NWebhook     string    `yaml:"n8n_webhook_url" mapstructure:"n8n_webhook_url"`
	MistralKey     string    `yaml:"mistral_api_key" mapstructure:"mistral_api_key"`
	MistralModel   string    `yaml:"mistral_ocr_model" mapstructure:"mistral_ocr_model"`
	OCR            OCRConfig `yaml:"ocr" mapstructure:"ocr"`
}

// OCRConfig configures PDF text extraction.
type OCRConfig struct {
	Provider      string `yaml:"provider" mapstructure:"provider"`
	PdfToTextPath string `yaml:"pdftotext_path" mapstructure:"pdftotext_path"`
}

// StoreConfig configures the database backend.
type StoreConfig struct {
	Driver      string `yaml:"driver" mapstructure:"driver"`
	DatabaseURL string `yaml:"database_url" mapstructure:"database_url"`
}

// NotionConfig holds Notion API credentials and database IDs.
type NotionConfig struct {
	Token      string `yaml:"token" mapstructure:"token"`
	LeadDB     string `yaml:"lead_db" mapstructure:"lead_db"`
	QuestionDB string `yaml:"question_db" mapstructure:"question_db"`
	FieldDB    string `yaml:"field_db" mapstructure:"field_db"`
}

// JinaConfig holds Jina AI Reader settings.
type JinaConfig struct {
	Key           string `yaml:"key" mapstructure:"key"`
	BaseURL       string `yaml:"base_url" mapstructure:"base_url"`
	SearchBaseURL string `yaml:"search_base_url" mapstructure:"search_base_url"`
}

// FirecrawlConfig holds Firecrawl API settings (fallback only).
type FirecrawlConfig struct {
	Key      string `yaml:"key" mapstructure:"key"`
	BaseURL  string `yaml:"base_url" mapstructure:"base_url"`
	MaxPages int    `yaml:"max_pages" mapstructure:"max_pages"`
}

// PerplexityConfig holds Perplexity API settings.
type PerplexityConfig struct {
	Key     string `yaml:"key" mapstructure:"key"`
	BaseURL string `yaml:"base_url" mapstructure:"base_url"`
	Model   string `yaml:"model" mapstructure:"model"`
}

// AnthropicConfig holds Anthropic API settings.
type AnthropicConfig struct {
	Key                 string `yaml:"key" mapstructure:"key"`
	HaikuModel          string `yaml:"haiku_model" mapstructure:"haiku_model"`
	SonnetModel         string `yaml:"sonnet_model" mapstructure:"sonnet_model"`
	OpusModel           string `yaml:"opus_model" mapstructure:"opus_model"`
	MaxBatchSize        int    `yaml:"max_batch_size" mapstructure:"max_batch_size"`
	NoBatch             bool   `yaml:"no_batch" mapstructure:"no_batch"`
	SmallBatchThreshold int    `yaml:"small_batch_threshold" mapstructure:"small_batch_threshold"`
}

// SalesforceConfig holds Salesforce JWT auth settings.
type SalesforceConfig struct {
	ClientID string `yaml:"client_id" mapstructure:"client_id"`
	Username string `yaml:"username" mapstructure:"username"`
	KeyPath  string `yaml:"key_path" mapstructure:"key_path"`
	LoginURL string `yaml:"login_url" mapstructure:"login_url"`
}

// ToolJetConfig holds ToolJet webhook settings.
type ToolJetConfig struct {
	WebhookURL string `yaml:"webhook_url" mapstructure:"webhook_url"`
}

// PPPConfig configures the PPP loan lookup phase.
type PPPConfig struct {
	URL                 string  `yaml:"url" mapstructure:"url"`
	SimilarityThreshold float64 `yaml:"similarity_threshold" mapstructure:"similarity_threshold"`
	MaxCandidates       int     `yaml:"max_candidates" mapstructure:"max_candidates"`
}

// PricingConfig holds per-provider pricing rates.
type PricingConfig struct {
	Anthropic  map[string]ModelPricing `yaml:"anthropic" mapstructure:"anthropic"`
	Jina       JinaPricing             `yaml:"jina" mapstructure:"jina"`
	Perplexity PerplexityPricing       `yaml:"perplexity" mapstructure:"perplexity"`
	Firecrawl  FirecrawlPricing        `yaml:"firecrawl" mapstructure:"firecrawl"`
}

// ModelPricing holds per-model token pricing (USD per million tokens).
type ModelPricing struct {
	Input         float64 `yaml:"input" mapstructure:"input"`
	Output        float64 `yaml:"output" mapstructure:"output"`
	BatchDiscount float64 `yaml:"batch_discount" mapstructure:"batch_discount"`
	CacheWriteMul float64 `yaml:"cache_write_mul" mapstructure:"cache_write_mul"`
	CacheReadMul  float64 `yaml:"cache_read_mul" mapstructure:"cache_read_mul"`
}

// JinaPricing holds Jina Reader pricing.
type JinaPricing struct {
	PerMTok float64 `yaml:"per_mtok" mapstructure:"per_mtok"`
}

// PerplexityPricing holds Perplexity pricing.
type PerplexityPricing struct {
	PerQuery float64 `yaml:"per_query" mapstructure:"per_query"`
}

// FirecrawlPricing holds Firecrawl pricing.
type FirecrawlPricing struct {
	PlanMonthly     float64 `yaml:"plan_monthly" mapstructure:"plan_monthly"`
	CreditsIncluded float64 `yaml:"credits_included" mapstructure:"credits_included"`
}

// CrawlConfig configures the crawl phase.
type CrawlConfig struct {
	MaxPages      int      `yaml:"max_pages" mapstructure:"max_pages"`
	MaxDepth      int      `yaml:"max_depth" mapstructure:"max_depth"`
	TimeoutSecs   int      `yaml:"timeout_secs" mapstructure:"timeout_secs"`
	CacheTTLHours int      `yaml:"cache_ttl_hours" mapstructure:"cache_ttl_hours"`
	ExcludePaths  []string `yaml:"exclude_paths" mapstructure:"exclude_paths"`
}

// ScrapeConfig configures the Phase 1B external scrape behavior.
type ScrapeConfig struct {
	SearchTimeoutSecs int `yaml:"search_timeout_secs" mapstructure:"search_timeout_secs"`
	SearchRetries     int `yaml:"search_retries" mapstructure:"search_retries"`
}

// PipelineConfig configures extraction behavior.
type PipelineConfig struct {
	ConfidenceEscalationThreshold float64 `yaml:"confidence_escalation_threshold" mapstructure:"confidence_escalation_threshold"`
	Tier3Gate                     string  `yaml:"tier3_gate" mapstructure:"tier3_gate"`
	QualityScoreThreshold         float64 `yaml:"quality_score_threshold" mapstructure:"quality_score_threshold"`
}

// BatchConfig configures batch processing.
type BatchConfig struct {
	MaxConcurrentCompanies int `yaml:"max_concurrent_companies" mapstructure:"max_concurrent_companies"`
}

// ServerConfig configures the webhook server.
type ServerConfig struct {
	Port int `yaml:"port" mapstructure:"port"`
}

// LogConfig configures logging.
type LogConfig struct {
	Level  string `yaml:"level" mapstructure:"level"`
	Format string `yaml:"format" mapstructure:"format"`
}

// Load reads configuration from file and environment.
func Load() (*Config, error) {
	v := viper.New()

	// Config file
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	// Environment
	v.SetEnvPrefix("RESEARCH")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("store.driver", "postgres")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("server.port", 8080)
	v.SetDefault("batch.max_concurrent_companies", 5)
	v.SetDefault("crawl.max_pages", 50)
	v.SetDefault("crawl.max_depth", 2)
	v.SetDefault("crawl.timeout_secs", 60)
	v.SetDefault("crawl.cache_ttl_hours", 24)
	v.SetDefault("crawl.exclude_paths", []string{"/blog/*", "/news/*", "/press/*", "/careers/*"})
	v.SetDefault("scrape.search_timeout_secs", 15)
	v.SetDefault("scrape.search_retries", 1)
	v.SetDefault("pipeline.confidence_escalation_threshold", 0.4)
	v.SetDefault("pipeline.tier3_gate", "off")
	v.SetDefault("pipeline.quality_score_threshold", 0.6)
	v.SetDefault("jina.base_url", "https://r.jina.ai")
	v.SetDefault("jina.search_base_url", "https://s.jina.ai")
	v.SetDefault("firecrawl.base_url", "https://api.firecrawl.dev/v2")
	v.SetDefault("firecrawl.max_pages", 50)
	v.SetDefault("perplexity.base_url", "https://api.perplexity.ai")
	v.SetDefault("perplexity.model", "sonar-pro")
	v.SetDefault("anthropic.haiku_model", "claude-haiku-4-5-20251001")
	v.SetDefault("anthropic.sonnet_model", "claude-sonnet-4-5-20250929")
	v.SetDefault("anthropic.opus_model", "claude-opus-4-6")
	v.SetDefault("anthropic.max_batch_size", 100)
	v.SetDefault("anthropic.small_batch_threshold", 3)
	v.SetDefault("salesforce.login_url", "https://login.salesforce.com")
	v.SetDefault("ppp.similarity_threshold", 0.4)
	v.SetDefault("ppp.max_candidates", 10)
	v.SetDefault("fedsync.temp_dir", "/tmp/fedsync")
	v.SetDefault("fedsync.edgar_user_agent", "Sells Advisors blake@sellsadvisors.com")
	v.SetDefault("fedsync.mistral_ocr_model", "pixtral-large-latest")
	v.SetDefault("fedsync.ocr.provider", "local")
	v.SetDefault("fedsync.ocr.pdftotext_path", "pdftotext")
	v.SetDefault("pricing.jina.per_mtok", 0.02)
	v.SetDefault("pricing.perplexity.per_query", 0.005)
	v.SetDefault("pricing.firecrawl.plan_monthly", 19.00)
	v.SetDefault("pricing.firecrawl.credits_included", 3000)

	// Read config file (optional)
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, eris.Wrap(err, "config: read file")
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, eris.Wrap(err, "config: unmarshal")
	}

	return &cfg, nil
}

// InitLogger initializes the global zap logger.
func InitLogger(cfg LogConfig) error {
	var zapCfg zap.Config
	if cfg.Format == "console" {
		zapCfg = zap.NewDevelopmentConfig()
	} else {
		zapCfg = zap.NewProductionConfig()
	}

	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return eris.Wrap(err, "config: parse log level")
	}
	zapCfg.Level.SetLevel(level)

	logger, err := zapCfg.Build()
	if err != nil {
		return eris.Wrap(err, "config: build logger")
	}
	zap.ReplaceGlobals(logger)

	return nil
}
