package monitoring

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/config"
)

func TestChecker_RunStopsOnCancel(t *testing.T) {
	st := &mockStore{}
	collector := NewCollector(st, nil)
	alerter := NewAlerter(config.MonitoringConfig{
		CheckIntervalSecs:    1,
		LookbackWindowHours:  24,
		FailureRateThreshold: 0.10,
	})
	checker := NewChecker(collector, alerter, config.MonitoringConfig{
		CheckIntervalSecs:   1,
		LookbackWindowHours: 24,
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		checker.Run(ctx)
		close(done)
	}()

	// Let it tick once then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Good — Run returned.
	case <-time.After(5 * time.Second):
		t.Fatal("Checker.Run did not stop after context cancellation")
	}
}

func TestChecker_DefaultInterval(t *testing.T) {
	st := &mockStore{}
	collector := NewCollector(st, nil)
	alerter := NewAlerter(config.MonitoringConfig{})

	// Zero interval should default to 5 minutes.
	checker := NewChecker(collector, alerter, config.MonitoringConfig{
		CheckIntervalSecs: 0,
	})
	assert.NotNil(t, checker)

	// Start and immediately cancel to verify it doesn't panic.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checker.Run(ctx)
}
