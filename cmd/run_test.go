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

func TestRunCmd_RunE_FailsOnInitPipeline_BadDriver(t *testing.T) {
	// initPipeline will fail because store driver is unsupported.
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	runCmd.SetContext(context.Background())
	defer runCmd.SetContext(nil)

	runURL = "https://example.com"
	runSFID = "001TEST"
	defer func() {
		runURL = ""
		runSFID = ""
	}()

	err := runCmd.RunE(runCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported store driver")
}

func TestRunCmd_RunE_FailsOnInitPipeline_BadSF(t *testing.T) {
	// Store succeeds, but Salesforce init fails.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_run.db"),
		},
		Salesforce: config.SalesforceConfig{
			ClientID: "",
		},
	}

	runCmd.SetContext(context.Background())
	defer runCmd.SetContext(nil)

	runURL = "https://example.com"
	runSFID = "001TEST"
	defer func() {
		runURL = ""
		runSFID = ""
	}()

	err := runCmd.RunE(runCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "salesforce client ID is required")
}

func TestRunCmd_Flags_Exist(t *testing.T) {
	urlFlag := runCmd.Flags().Lookup("url")
	require.NotNil(t, urlFlag)

	sfFlag := runCmd.Flags().Lookup("sf-id")
	require.NotNil(t, sfFlag)
}
