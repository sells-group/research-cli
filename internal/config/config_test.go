package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoadDefaults(t *testing.T) {
	// Change to temp dir so no config.yaml is found
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { os.Chdir(origDir) })

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "postgres", cfg.Store.Driver)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, 8080, cfg.Server.Port)
	assert.Equal(t, 15, cfg.Batch.MaxConcurrentCompanies)
	assert.Equal(t, 50, cfg.Crawl.MaxPages)
	assert.Equal(t, 2, cfg.Crawl.MaxDepth)
	assert.Equal(t, 60, cfg.Crawl.TimeoutSecs)
	assert.Equal(t, 24, cfg.Crawl.CacheTTLHours)
	assert.InDelta(t, 0.4, cfg.Pipeline.ConfidenceEscalationThreshold, 0.001)
	assert.Equal(t, "off", cfg.Pipeline.Tier3Gate)
	assert.InDelta(t, 0.6, cfg.Pipeline.QualityScoreThreshold, 0.001)
	assert.Equal(t, "https://r.jina.ai", cfg.Jina.BaseURL)
	assert.Equal(t, "https://api.firecrawl.dev/v2", cfg.Firecrawl.BaseURL)
	assert.Equal(t, 50, cfg.Firecrawl.MaxPages)
	assert.Equal(t, "sonar-pro", cfg.Perplexity.Model)
	assert.Equal(t, "claude-haiku-4-5-20251001", cfg.Anthropic.HaikuModel)
	assert.Equal(t, 100, cfg.Anthropic.MaxBatchSize)
	assert.Equal(t, "https://login.salesforce.com", cfg.Salesforce.LoginURL)
	assert.InDelta(t, 0.50, cfg.Pipeline.QualityWeights.Confidence, 0.001)
	assert.InDelta(t, 0.25, cfg.Pipeline.QualityWeights.Completeness, 0.001)
	assert.InDelta(t, 0.15, cfg.Pipeline.QualityWeights.Diversity, 0.001)
	assert.InDelta(t, 0.10, cfg.Pipeline.QualityWeights.Freshness, 0.001)
}

func TestLoadFromYAML(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { os.Chdir(origDir) })

	yaml := `
store:
  driver: sqlite
log:
  level: debug
  format: console
server:
  port: 9090
batch:
  max_concurrent_companies: 10
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0644))

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "sqlite", cfg.Store.Driver)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "console", cfg.Log.Format)
	assert.Equal(t, 9090, cfg.Server.Port)
	assert.Equal(t, 10, cfg.Batch.MaxConcurrentCompanies)
	// Defaults still apply for unset values
	assert.Equal(t, 50, cfg.Crawl.MaxPages)
}

func TestLoadEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { os.Chdir(origDir) })

	yaml := `
store:
  driver: sqlite
log:
  level: debug
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0644))

	t.Setenv("RESEARCH_STORE_DRIVER", "postgres")
	t.Setenv("RESEARCH_LOG_LEVEL", "warn")

	cfg, err := Load()
	require.NoError(t, err)

	// Env overrides file
	assert.Equal(t, "postgres", cfg.Store.Driver)
	assert.Equal(t, "warn", cfg.Log.Level)
}

func TestLoadEnvOverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { os.Chdir(origDir) })

	t.Setenv("RESEARCH_SERVER_PORT", "3000")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 3000, cfg.Server.Port)
}

func TestInitLoggerConsole(t *testing.T) {
	err := InitLogger(LogConfig{Level: "debug", Format: "console"})
	require.NoError(t, err)
	assert.NotNil(t, zap.L())
}

func TestInitLoggerJSON(t *testing.T) {
	err := InitLogger(LogConfig{Level: "info", Format: "json"})
	require.NoError(t, err)
	assert.NotNil(t, zap.L())
}

func TestInitLoggerInvalidLevel(t *testing.T) {
	err := InitLogger(LogConfig{Level: "invalid", Format: "json"})
	assert.Error(t, err)
}

// validDefaults returns a Config with all defaults populated for validation tests.
func validDefaults() *Config {
	cfg := &Config{}
	cfg.Batch.MaxConcurrentCompanies = 15
	cfg.Pipeline.ConfidenceEscalationThreshold = 0.4
	cfg.Pipeline.QualityScoreThreshold = 0.6
	cfg.Pipeline.SkipConfidenceThreshold = 0.8
	cfg.Server.Port = 8080
	return cfg
}

func TestValidateEnrichment_AllPresent(t *testing.T) {
	cfg := validDefaults()
	cfg.Store.DatabaseURL = "postgres://localhost/test"
	cfg.Notion.Token = "ntn_token"
	cfg.Notion.LeadDB = "lead-db-id"
	cfg.Notion.QuestionDB = "question-db-id"
	cfg.Notion.FieldDB = "field-db-id"
	cfg.Anthropic.Key = "sk-ant-key"

	assert.NoError(t, cfg.Validate("enrichment"))
}

func TestValidateEnrichment_MissingFields(t *testing.T) {
	cfg := validDefaults()
	// All enrichment-required fields are empty

	err := cfg.Validate("enrichment")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "store.database_url is required")
	assert.Contains(t, err.Error(), "notion.token is required")
	assert.Contains(t, err.Error(), "anthropic.key is required")
}

func TestValidateFedsync_WithDedicatedURL(t *testing.T) {
	cfg := validDefaults()
	cfg.Fedsync.DatabaseURL = "postgres://localhost/fedsync"

	assert.NoError(t, cfg.Validate("fedsync"))
}

func TestValidateFedsync_FallsBackToStoreURL(t *testing.T) {
	cfg := validDefaults()
	cfg.Store.DatabaseURL = "postgres://localhost/main"

	assert.NoError(t, cfg.Validate("fedsync"))
}

func TestValidateFedsync_NoDB(t *testing.T) {
	cfg := validDefaults()

	err := cfg.Validate("fedsync")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database_url")
}

func TestValidateServe_ValidPort(t *testing.T) {
	cfg := validDefaults()
	cfg.Server.Port = 9090

	assert.NoError(t, cfg.Validate("serve"))
}

func TestValidateServe_InvalidPort(t *testing.T) {
	cfg := validDefaults()
	cfg.Server.Port = 0

	err := cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server.port must be > 0")
}

func TestValidateUnknownMode(t *testing.T) {
	cfg := validDefaults()
	err := cfg.Validate("unknown")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown mode")
}

func TestValidateConcurrencyBounds(t *testing.T) {
	cfg := validDefaults()
	cfg.Server.Port = 8080

	cfg.Batch.MaxConcurrentCompanies = 0
	err := cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max_concurrent_companies must be between 1 and 50")

	cfg.Batch.MaxConcurrentCompanies = 51
	err = cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max_concurrent_companies must be between 1 and 50")

	cfg.Batch.MaxConcurrentCompanies = 50
	err = cfg.Validate("serve")
	assert.NoError(t, err)
}

func TestValidateConfidenceThresholds(t *testing.T) {
	cfg := validDefaults()
	cfg.Server.Port = 8080

	cfg.Pipeline.ConfidenceEscalationThreshold = -0.1
	err := cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "confidence_escalation_threshold")

	cfg.Pipeline.ConfidenceEscalationThreshold = 1.1
	err = cfg.Validate("serve")
	assert.Error(t, err)

	cfg.Pipeline.ConfidenceEscalationThreshold = 0.4
	cfg.Pipeline.QualityScoreThreshold = 2.0
	err = cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "quality_score_threshold")

	cfg.Pipeline.QualityScoreThreshold = 0.6
	cfg.Pipeline.SkipConfidenceThreshold = -1
	err = cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "skip_confidence_threshold")
}

func TestValidateQualityWeights_Negative(t *testing.T) {
	cfg := validDefaults()
	cfg.Server.Port = 8080

	cfg.Pipeline.QualityWeights.Confidence = -0.1
	err := cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "quality_weights values must be >= 0")

	cfg.Pipeline.QualityWeights.Confidence = 0.5
	cfg.Pipeline.QualityWeights.Diversity = -1
	err = cfg.Validate("serve")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "quality_weights values must be >= 0")

	cfg.Pipeline.QualityWeights.Diversity = 0.15
	err = cfg.Validate("serve")
	assert.NoError(t, err)
}
