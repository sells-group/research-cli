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
	"github.com/sells-group/research-cli/pkg/ppp"
)

// mockPPP implements ppp.Querier for testing Close().
type mockPPP struct {
	closed bool
}

func (m *mockPPP) FindLoans(_ context.Context, _, _, _ string) ([]ppp.LoanMatch, error) {
	return nil, nil
}

func (m *mockPPP) Close() {
	m.closed = true
}

func TestPipelineEnv_Close_WithPPPNil(t *testing.T) {
	// PPP is nil, Store is nil — should not panic.
	pe := &pipelineEnv{
		PPP:   nil,
		Store: nil,
	}
	assert.NotPanics(t, func() {
		pe.Close()
	})
}

func TestPipelineEnv_Close_WithPPPSet(t *testing.T) {
	// PPP is set — Close should call PPP.Close().
	mock := &mockPPP{}
	pe := &pipelineEnv{
		PPP:   mock,
		Store: nil,
	}
	pe.Close()
	assert.True(t, mock.closed, "PPP.Close() should have been called")
}

func TestPipelineEnv_Close_WithStoreAndPPP(t *testing.T) {
	// Both PPP and Store are set — both should be closed.
	tmpDir := t.TempDir()
	dsn := filepath.Join(tmpDir, "test_close_both.db")

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: dsn,
		},
	}

	st, err := initStore(context.Background())
	require.NoError(t, err)

	mock := &mockPPP{}
	pe := &pipelineEnv{
		Store: st,
		PPP:   mock,
	}

	assert.NotPanics(t, func() {
		pe.Close()
	})
	assert.True(t, mock.closed)
}

func TestPipelineEnv_Close_StoreOnly(t *testing.T) {
	tmpDir := t.TempDir()
	dsn := filepath.Join(tmpDir, "test_close_store.db")

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
		PPP:   nil,
	}

	assert.NotPanics(t, func() {
		pe.Close()
	})
}

func TestInitPipeline_FailsOnMigrateError(t *testing.T) {
	// Use a directory instead of a file to force SQLite to fail on migrate.
	tmpDir := t.TempDir()
	badDir := filepath.Join(tmpDir, "notadb")
	require.NoError(t, os.MkdirAll(badDir, 0o755))

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: badDir, // directories are not valid SQLite databases
		},
	}

	env, pErr := initPipeline(context.Background())
	// This should fail at store open or migrate because it's a directory.
	assert.Nil(t, env)
	assert.Error(t, pErr)
}

func TestInitPipeline_FailsOnSalesforce_ClosesStore(t *testing.T) {
	// Store succeeds (SQLite), SF returns nil gracefully, then fixture
	// loading fails because testdata/ doesn't exist in the temp dir.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_sf_close.db"),
		},
		Salesforce: config.SalesforceConfig{
			ClientID: "", // returns nil gracefully
		},
	}

	env, err := initPipeline(context.Background())
	assert.Nil(t, env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load question fixtures")
}

func TestInitPipeline_SFKeyNotFound_ClosesStore(t *testing.T) {
	// Store + migrate succeed, SF fails on missing key file.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver:      "sqlite",
			DatabaseURL: filepath.Join(tmpDir, "test_sf_key.db"),
		},
		Salesforce: config.SalesforceConfig{
			ClientID: "test-client-id",
			KeyPath:  "/nonexistent/key.pem",
		},
	}

	env, err := initPipeline(context.Background())
	assert.Nil(t, env)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read salesforce JWT private key")
}
