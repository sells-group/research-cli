//go:build !integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func TestServeCmd_RunE_FailsOnValidation(t *testing.T) {
	// Config validation should fail fast with missing required fields.
	cfg = &config.Config{
		Store: config.StoreConfig{
			Driver: "postgres",
		},
	}

	serveCmd.SetContext(context.Background())
	defer serveCmd.SetContext(nil)

	err := serveCmd.RunE(serveCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config: validation failed")
	assert.Contains(t, err.Error(), "server.port must be > 0")
}
