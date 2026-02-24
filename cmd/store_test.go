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

func TestInitStore_SQLite(t *testing.T) {
	tmpDir := t.TempDir()
	dsn := filepath.Join(tmpDir, "test.db")

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: dsn,
		},
	}

	st, err := initStore(context.Background())
	require.NoError(t, err)
	require.NotNil(t, st)
	defer st.Close() //nolint:errcheck
}

func TestInitStore_SQLiteDefaultDSN(t *testing.T) {
	// When DatabaseURL is empty, initStore should default to "research.db".
	// We'll set up in a temp dir so we don't create files in the project root.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir) //nolint:errcheck

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: "",
		},
	}

	st, err := initStore(context.Background())
	require.NoError(t, err)
	require.NotNil(t, st)
	defer st.Close() //nolint:errcheck

	// Verify the default file was created.
	_, statErr := os.Stat(filepath.Join(tmpDir, "research.db"))
	assert.NoError(t, statErr)
}

func TestInitStore_UnsupportedDriver(t *testing.T) {
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	st, err := initStore(context.Background())
	assert.Nil(t, st)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported store driver")
}

func TestInitStore_UnknownDriver(t *testing.T) {
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "mysql",
		},
	}

	st, err := initStore(context.Background())
	assert.Nil(t, st)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported store driver")
}

func TestInitSalesforce_MissingClientID(t *testing.T) {
	cfg = &config.Config{
		Salesforce: config.SalesforceConfig{
			ClientID: "",
		},
	}

	client, err := initSalesforce()
	assert.Nil(t, client)
	assert.NoError(t, err)
}

func TestInitSalesforce_BadKeyPath(t *testing.T) {
	cfg = &config.Config{
		Salesforce: config.SalesforceConfig{
			ClientID: "test-client-id",
			KeyPath:  "/nonexistent/path/to/key.pem",
		},
	}

	client, err := initSalesforce()
	assert.Nil(t, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read salesforce JWT private key")
}

func TestInitSalesforce_InvalidPEM(t *testing.T) {
	// Write a bad PEM file and verify init fails.
	tmpDir := t.TempDir()
	badPEM := filepath.Join(tmpDir, "bad.pem")
	require.NoError(t, os.WriteFile(badPEM, []byte("not a valid pem"), 0o600))

	cfg = &config.Config{
		Salesforce: config.SalesforceConfig{
			ClientID: "test-client-id",
			KeyPath:  badPEM,
			Username: "user@test.com",
			LoginURL: "https://login.salesforce.com",
		},
	}

	client, err := initSalesforce()
	assert.Nil(t, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "init salesforce")
}
