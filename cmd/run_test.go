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

func TestRunCmd_RunE_FailsOnValidation(t *testing.T) {
	// Config validation fails because required fields are missing.
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	runCmd.SetContext(context.Background())
	defer runCmd.SetContext(context.TODO())

	runURL = "https://example.com"
	runSFID = "001TEST"
	defer func() {
		runURL = ""
		runSFID = ""
	}()

	err := runCmd.RunE(runCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config: validation failed")
}

func TestRunCmd_RunE_FailsOnInitPipeline_BadSF(t *testing.T) {
	// Store succeeds, but loading fails because the Notion token is fake.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir) //nolint:errcheck

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_run.db"),
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

	runCmd.SetContext(context.Background())
	defer runCmd.SetContext(context.TODO())

	runURL = "https://example.com"
	runSFID = "001TEST"
	defer func() {
		runURL = ""
		runSFID = ""
	}()

	err := runCmd.RunE(runCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load question registry")
}

func TestRunCmd_Flags_Exist(t *testing.T) {
	urlFlag := runCmd.Flags().Lookup("url")
	require.NotNil(t, urlFlag)

	sfFlag := runCmd.Flags().Lookup("sf-id")
	require.NotNil(t, sfFlag)
}
