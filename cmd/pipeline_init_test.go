//go:build !integration

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func TestPipelineEnv_Close_Nil(t *testing.T) {
	// Close with all nil fields should not panic.
	pe := &pipelineEnv{}
	assert.NotPanics(t, func() {
		pe.Close()
	})
}

func TestPipelineEnv_Close_WithStore(t *testing.T) {
	// Set up a real SQLite store to verify Close() calls through.
	tmpDir := t.TempDir()
	dsn := filepath.Join(tmpDir, "test_close.db")

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: dsn,
		},
	}

	st, err := initStore(context.Background())
	require.NoError(t, err)

	pe := &pipelineEnv{
		Store: st,
	}

	// Should not panic and should close the store cleanly.
	assert.NotPanics(t, func() {
		pe.Close()
	})
}

func TestInitPipeline_FailsOnValidation(t *testing.T) {
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	env, err := initPipeline(context.Background())
	assert.Nil(t, env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config: validation failed")
}

func TestInitPipeline_FailsOnBadSalesforce(t *testing.T) {
	// SQLite store will succeed, SF returns nil gracefully, then
	// loading fails because the Notion token is fake.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_pipe.db"),
		},
		Anthropic: config.AnthropicConfig{Key: "test-key"},
		Notion: config.NotionConfig{
			Token:      "test-token",
			LeadDB:     "test-lead-db",
			QuestionDB: "test-question-db",
			FieldDB:    "test-field-db",
		},
		Salesforce: config.SalesforceConfig{
			ClientID: "", // returns nil gracefully
		},
		Batch: config.BatchConfig{MaxConcurrentCompanies: 15},
		Pipeline: config.PipelineConfig{
			ConfidenceEscalationThreshold: 0.4,
			QualityScoreThreshold:         0.6,
			SkipConfidenceThreshold:       0.8,
		},
	}

	env, err := initPipeline(context.Background())
	assert.Nil(t, env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load question registry")
}
