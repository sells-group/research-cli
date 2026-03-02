package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func TestNewHandlers(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Port: 8080}}
	h := NewHandlers(cfg, nil, nil, nil)

	require.NotNil(t, h)
	assert.Equal(t, cfg, h.cfg)
	assert.Nil(t, h.store)
	assert.Nil(t, h.runner)
	assert.Nil(t, h.collector)
	assert.Equal(t, WebhookSemSize, cap(h.sem))
}

func TestDrain_ReturnsImmediately(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil)

	// Drain should return immediately when no in-flight jobs.
	done := make(chan struct{})
	go func() {
		h.Drain()
		close(done)
	}()

	select {
	case <-done:
		// OK — returned immediately.
	default:
		// Give it a moment for goroutine scheduling.
		select {
		case <-done:
		case <-t.Context().Done():
			t.Fatal("drain did not return")
		}
	}
}
