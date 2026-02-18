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

func TestBatchCmd_RunE_FailsOnValidation(t *testing.T) {
	// Config validation should fail fast with missing required fields.
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	batchCmd.SetContext(context.Background())
	defer batchCmd.SetContext(nil)

	err := batchCmd.RunE(batchCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config: validation failed")
}

func TestBatchCmd_RunE_FailsOnInitPipeline_BadSF(t *testing.T) {
	// Store succeeds (SQLite), SF returns nil gracefully, then fixture
	// loading fails because testdata/ doesn't exist in the temp dir.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_batch.db"),
		},
		Anthropic: config.AnthropicConfig{Key: "test-key"},
		Notion: config.NotionConfig{
			Token:      "test-token",
			LeadDB:     "test-lead-db",
			QuestionDB: "test-question-db",
			FieldDB:    "test-field-db",
		},
		Salesforce: config.SalesforceConfig{
			ClientID: "",
		},
		Batch: config.BatchConfig{MaxConcurrentCompanies: 15},
		Pipeline: config.PipelineConfig{
			ConfidenceEscalationThreshold: 0.4,
			QualityScoreThreshold:         0.6,
			SkipConfidenceThreshold:       0.8,
		},
	}

	batchCmd.SetContext(context.Background())
	defer batchCmd.SetContext(nil)

	err := batchCmd.RunE(batchCmd, nil)
	require.Error(t, err)
	// Fails loading registries from Notion (fake token).
	assert.Contains(t, err.Error(), "load question registry")
}
