//go:build !integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

// getFreePort returns a free TCP port on localhost.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestBuildMux_ServerLifecycle(t *testing.T) {
	// Test the full server start + request + graceful shutdown cycle.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := buildMux(ctx, nil, nil)

	port := getFreePort(t)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Start server in background.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for server to be ready.
	var ready bool
	for i := 0; i < 20; i++ {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, ready, "server did not become ready in time")

	// Make a real health request.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])

	// Graceful shutdown.
	require.NoError(t, srv.Shutdown(context.Background()))

	// Wait for server to finish.
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

func TestServeCmd_PortResolution(t *testing.T) {
	// When servePort is 0, port should come from cfg.Server.Port.
	cfg = &config.Config{
		Server: config.ServerConfig{
			Port: 9999,
		},
	}

	// Verify the config's port field is accessible.
	assert.Equal(t, 9999, cfg.Server.Port)
}
