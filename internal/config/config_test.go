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
	assert.Equal(t, 5, cfg.Batch.MaxConcurrentCompanies)
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
