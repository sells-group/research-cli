//go:build !integration

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootCmd_PersistentPreRunE_WithValidConfig(t *testing.T) {
	// Create a temp dir with a minimal config.yaml.
	tmpDir := t.TempDir()
	configContent := `
store:
  driver: sqlite
log:
  level: info
  format: console
`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.yaml"), []byte(configContent), 0o644))

	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	// Reset cfg to nil so PersistentPreRunE repopulates it.
	oldCfg := cfg
	cfg = nil
	defer func() { cfg = oldCfg }()

	err := rootCmd.PersistentPreRunE(rootCmd, nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "sqlite", cfg.Store.Driver)
}

func TestRootCmd_PersistentPreRunE_NoConfigFile(t *testing.T) {
	// In a temp dir with no config.yaml, viper should use defaults + env.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	oldCfg := cfg
	cfg = nil
	defer func() { cfg = oldCfg }()

	err := rootCmd.PersistentPreRunE(rootCmd, nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	// Defaults should be applied.
	assert.Equal(t, "postgres", cfg.Store.Driver) // default is postgres
	assert.Equal(t, "info", cfg.Log.Level)
}

func TestRootCmd_PersistentPreRunE_BadLogLevel(t *testing.T) {
	// Create a config with an invalid log level.
	tmpDir := t.TempDir()
	configContent := `
log:
  level: NOT_A_LEVEL
  format: console
`
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.yaml"), []byte(configContent), 0o644))

	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	oldCfg := cfg
	cfg = nil
	defer func() { cfg = oldCfg }()

	err := rootCmd.PersistentPreRunE(rootCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "init logger")
}

func TestRootCmd_PersistentPostRun_DoesNotPanic(t *testing.T) {
	// PersistentPostRun calls zap.L().Sync() — should not panic.
	assert.NotPanics(t, func() {
		rootCmd.PersistentPostRun(rootCmd, nil)
	})
}

func TestRootCmd_PersistentPreRunE_InvalidYAML(t *testing.T) {
	// Create a config.yaml with invalid YAML syntax.
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "config.yaml"), []byte("invalid: [yaml: bad"), 0o644))

	origDir, _ := os.Getwd()
	require.NoError(t, os.Chdir(tmpDir))
	defer os.Chdir(origDir)

	oldCfg := cfg
	cfg = nil
	defer func() { cfg = oldCfg }()

	err := rootCmd.PersistentPreRunE(rootCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load config")
}
