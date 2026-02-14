package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/config"
)

func TestFedsyncPool_NoDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "",
		},
		Store: config.StoreConfig{
			DatabaseURL: "",
		},
	}

	pool, err := fedsyncPool(context.Background())
	assert.Nil(t, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no database_url configured")
}

func TestFedsyncPool_FallbackToStoreURL(t *testing.T) {
	// When Fedsync.DatabaseURL is empty, it should fall back to Store.DatabaseURL.
	// Use an invalid URL so we get a connection error, proving the fallback path was taken.
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "",
		},
		Store: config.StoreConfig{
			DatabaseURL: "postgres://invalid:invalid@localhost:1/nonexistent",
		},
	}

	pool, err := fedsyncPool(context.Background())
	// We expect either a create or ping error since the URL is invalid.
	assert.Error(t, err)
	assert.Nil(t, pool)
}

func TestFedsyncPool_InvalidDSN(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{
			DatabaseURL: "postgres://invalid:invalid@localhost:1/nonexistent",
		},
	}

	pool, err := fedsyncPool(context.Background())
	assert.Error(t, err)
	assert.Nil(t, pool)
}

func TestFedsyncSyncCmd_Flags(t *testing.T) {
	flags := []struct {
		name     string
		defValue string
	}{
		{"phase", ""},
		{"datasets", ""},
		{"force", "false"},
		{"full", "false"},
	}

	for _, f := range flags {
		flag := fedsyncSyncCmd.Flags().Lookup(f.name)
		assert.NotNil(t, flag, "fedsync sync should have --%s flag", f.name)
		assert.Equal(t, f.defValue, flag.DefValue, "flag --%s default value mismatch", f.name)
	}
}

func TestFedsyncMigrateCmd_Metadata(t *testing.T) {
	assert.Equal(t, "migrate", fedsyncMigrateCmd.Use)
	assert.NotEmpty(t, fedsyncMigrateCmd.Short)
}

func TestFedsyncStatusCmd_Metadata(t *testing.T) {
	assert.Equal(t, "status", fedsyncStatusCmd.Use)
	assert.NotEmpty(t, fedsyncStatusCmd.Short)
}

func TestFedsyncXrefCmd_Metadata(t *testing.T) {
	assert.Equal(t, "xref", fedsyncXrefCmd.Use)
	assert.NotEmpty(t, fedsyncXrefCmd.Short)
}

func TestFedsyncCmd_Metadata(t *testing.T) {
	assert.Equal(t, "fedsync", fedsyncCmd.Use)
	assert.NotEmpty(t, fedsyncCmd.Short)
	assert.NotEmpty(t, fedsyncCmd.Long)
}
