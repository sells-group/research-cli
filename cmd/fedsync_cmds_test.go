//go:build !integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func TestFedsyncMigrateCmd_RunE_NoDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "",
		},
		Store: config.StoreConfig{
			DatabaseURL: "",
		},
	}

	err := fedsyncMigrateCmd.RunE(fedsyncMigrateCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no database_url configured")
}

func TestFedsyncStatusCmd_RunE_NoDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "",
		},
		Store: config.StoreConfig{
			DatabaseURL: "",
		},
	}

	err := fedsyncStatusCmd.RunE(fedsyncStatusCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no database_url configured")
}

func TestFedsyncSyncCmd_RunE_NoDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "",
		},
		Store: config.StoreConfig{
			DatabaseURL: "",
		},
	}

	fedsyncSyncCmd.SetContext(context.Background())
	defer fedsyncSyncCmd.SetContext(nil)

	err := fedsyncSyncCmd.RunE(fedsyncSyncCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no database_url configured")
}

func TestFedsyncXrefCmd_RunE_NoDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "",
		},
		Store: config.StoreConfig{
			DatabaseURL: "",
		},
	}

	fedsyncXrefCmd.SetContext(context.Background())
	defer fedsyncXrefCmd.SetContext(nil)

	err := fedsyncXrefCmd.RunE(fedsyncXrefCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no database_url configured")
}

func TestFedsyncMigrateCmd_RunE_InvalidDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "postgres://invalid:invalid@localhost:1/nonexistent",
		},
	}

	fedsyncMigrateCmd.SetContext(context.Background())
	defer fedsyncMigrateCmd.SetContext(nil)

	err := fedsyncMigrateCmd.RunE(fedsyncMigrateCmd, nil)
	require.Error(t, err)
}

func TestFedsyncStatusCmd_RunE_InvalidDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "postgres://invalid:invalid@localhost:1/nonexistent",
		},
	}

	fedsyncStatusCmd.SetContext(context.Background())
	defer fedsyncStatusCmd.SetContext(nil)

	err := fedsyncStatusCmd.RunE(fedsyncStatusCmd, nil)
	require.Error(t, err)
}

func TestFedsyncSyncCmd_RunE_InvalidDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "postgres://invalid:invalid@localhost:1/nonexistent",
		},
	}

	fedsyncSyncCmd.SetContext(context.Background())
	defer fedsyncSyncCmd.SetContext(nil)

	err := fedsyncSyncCmd.RunE(fedsyncSyncCmd, nil)
	require.Error(t, err)
}

func TestFedsyncXrefCmd_RunE_InvalidDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "postgres://invalid:invalid@localhost:1/nonexistent",
		},
	}

	fedsyncXrefCmd.SetContext(context.Background())
	defer fedsyncXrefCmd.SetContext(nil)

	err := fedsyncXrefCmd.RunE(fedsyncXrefCmd, nil)
	require.Error(t, err)
}
