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
)

func TestResolvePort_FlagSet(t *testing.T) {
	assert.Equal(t, 9090, resolvePort(9090, 8080))
}

func TestResolvePort_FlagZero(t *testing.T) {
	assert.Equal(t, 8080, resolvePort(0, 8080))
}

func TestResolvePort_BothZero(t *testing.T) {
	assert.Equal(t, 0, resolvePort(0, 0))
}

func TestStartServer_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mux := buildMux(ctx, nil, nil)

	// Find a free port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- startServer(ctx, mux, port)
	}()

	// Wait for server to be ready.
	var ready bool
	for i := 0; i < 30; i++ {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, ready, "server did not become ready in time")

	// Verify the server responds.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])

	// Trigger graceful shutdown.
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}
