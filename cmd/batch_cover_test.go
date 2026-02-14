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

func TestBatchCmd_RunE_FailsOnInitPipeline_BadDriver(t *testing.T) {
	// initPipeline will fail because store driver is unsupported.
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	batchCmd.SetContext(context.Background())
	defer batchCmd.SetContext(nil)

	err := batchCmd.RunE(batchCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported store driver")
}

func TestBatchCmd_RunE_FailsOnInitPipeline_BadSF(t *testing.T) {
	// Store succeeds (SQLite), but Salesforce init fails.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_batch.db"),
		},
		Salesforce: config.SalesforceConfig{
			ClientID: "",
		},
	}

	batchCmd.SetContext(context.Background())
	defer batchCmd.SetContext(nil)

	err := batchCmd.RunE(batchCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "salesforce client ID is required")
}
