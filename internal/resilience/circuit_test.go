package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_ClosedState_PassesThrough(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	var calls int
	err := cb.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed state, got %s", cb.State())
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     1 * time.Minute,
	}
	cb := NewCircuitBreaker(cfg)

	// Fail 3 times to trip the breaker.
	for i := 0; i < 3; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("fail")
		})
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected open state after %d failures, got %s", cfg.FailureThreshold, cb.State())
	}

	// Next call should be rejected immediately.
	err := cb.Execute(context.Background(), func(_ context.Context) error {
		t.Error("should not be called when circuit is open")
		return nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_SuccessResetsClosed(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     1 * time.Minute,
	}
	cb := NewCircuitBreaker(cfg)

	// Fail twice (below threshold).
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("fail")
		})
	}

	failures, state := cb.Counters()
	if failures != 2 {
		t.Errorf("expected 2 consecutive failures, got %d", failures)
	}
	if state != CircuitClosed {
		t.Errorf("expected closed state, got %s", state)
	}

	// Success resets counter.
	_ = cb.Execute(context.Background(), func(_ context.Context) error {
		return nil
	})

	failures, _ = cb.Counters()
	if failures != 0 {
		t.Errorf("expected 0 consecutive failures after success, got %d", failures)
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	now := time.Now()
	cfg := CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)
	cb.nowFunc = func() time.Time { return now }

	// Trip the breaker.
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("fail")
		})
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open state, got %s", cb.State())
	}

	// Advance time past reset timeout.
	cb.nowFunc = func() time.Time { return now.Add(200 * time.Millisecond) }

	if cb.State() != CircuitHalfOpen {
		t.Errorf("expected half-open state after timeout, got %s", cb.State())
	}

	// Successful probe closes the circuit.
	err := cb.Execute(context.Background(), func(_ context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed state after successful probe, got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailure_Reopens(t *testing.T) {
	now := time.Now()
	cfg := CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(cfg)
	cb.nowFunc = func() time.Time { return now }

	// Trip the breaker.
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("fail")
		})
	}

	// Advance time past reset timeout.
	cb.nowFunc = func() time.Time { return now.Add(200 * time.Millisecond) }

	// Fail in half-open state → reopen.
	_ = cb.Execute(context.Background(), func(_ context.Context) error {
		return errors.New("still failing")
	})

	// State should be open again (need to prevent immediate half-open by resetting time).
	cb.nowFunc = func() time.Time { return now.Add(200 * time.Millisecond) }
	// The lastFailureTime was just updated, so circuit is open until another reset timeout passes.
	// Check internal state directly.
	failures, state := cb.Counters()
	if state != CircuitOpen {
		t.Errorf("expected open state after half-open failure, got %s", state)
	}
	if failures != 3 {
		t.Errorf("expected 3 total failures, got %d", failures)
	}
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	var transitions []struct{ from, to CircuitState }
	cfg := CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Minute,
		OnStateChange: func(from, to CircuitState) {
			transitions = append(transitions, struct{ from, to CircuitState }{from, to})
		},
	}
	cb := NewCircuitBreaker(cfg)

	// Trip the breaker.
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("fail")
		})
	}

	if len(transitions) != 1 {
		t.Fatalf("expected 1 transition, got %d", len(transitions))
	}
	if transitions[0].from != CircuitClosed || transitions[0].to != CircuitOpen {
		t.Errorf("expected closed→open, got %s→%s", transitions[0].from, transitions[0].to)
	}
}

func TestCircuitBreaker_ShouldTrip(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Minute,
		ShouldTrip: func(err error) bool {
			// Only trip on specific errors.
			return err.Error() == "tripworthy"
		},
	}
	cb := NewCircuitBreaker(cfg)

	// These shouldn't count toward the threshold.
	for i := 0; i < 5; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("non-tripworthy")
		})
	}
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed (non-tripworthy errors), got %s", cb.State())
	}

	// These should trip.
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("tripworthy")
		})
	}
	if cb.State() != CircuitOpen {
		t.Errorf("expected open after tripworthy errors, got %s", cb.State())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Hour,
	}
	cb := NewCircuitBreaker(cfg)

	// Trip it.
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return errors.New("fail")
		})
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open, got %s", cb.State())
	}

	// Manual reset.
	cb.Reset()
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed after reset, got %s", cb.State())
	}

	// Should work normally now.
	err := cb.Execute(context.Background(), func(_ context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error after reset: %v", err)
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	cfg := CircuitBreakerConfig{
		FailureThreshold: 100,
		ResetTimeout:     1 * time.Minute,
	}
	cb := NewCircuitBreaker(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cb.Execute(context.Background(), func(_ context.Context) error {
				if i%2 == 0 {
					return errors.New("fail")
				}
				return nil
			})
		}()
	}
	wg.Wait()
	// Just verifying no race/panic.
}

func TestExecuteVal_CircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	val, err := ExecuteVal(context.Background(), cb, func(_ context.Context) (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestExecuteVal_CircuitOpen(t *testing.T) {
	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     1 * time.Hour,
	}
	cb := NewCircuitBreaker(cfg)

	// Trip the breaker.
	_ = cb.Execute(context.Background(), func(_ context.Context) error {
		return errors.New("fail")
	})

	val, err := ExecuteVal(context.Background(), cb, func(_ context.Context) (int, error) {
		return 42, nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
	if val != 0 {
		t.Errorf("expected zero value, got %d", val)
	}
}

func TestServiceBreakers_GetOrCreate(t *testing.T) {
	sb := NewServiceBreakers(DefaultCircuitBreakerConfig())

	cb1 := sb.Get("anthropic")
	cb2 := sb.Get("anthropic")
	cb3 := sb.Get("salesforce")

	if cb1 != cb2 {
		t.Error("expected same breaker for same service")
	}
	if cb1 == cb3 {
		t.Error("expected different breakers for different services")
	}
}

func TestServiceBreakers_States(t *testing.T) {
	sb := NewServiceBreakers(CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     1 * time.Hour,
	})

	// Create a breaker and trip it.
	cb := sb.Get("anthropic")
	_ = cb.Execute(context.Background(), func(_ context.Context) error {
		return errors.New("fail")
	})

	// Keep salesforce healthy.
	_ = sb.Get("salesforce")

	states := sb.States()
	if states["anthropic"] != CircuitOpen {
		t.Errorf("expected anthropic=open, got %s", states["anthropic"])
	}
	if states["salesforce"] != CircuitClosed {
		t.Errorf("expected salesforce=closed, got %s", states["salesforce"])
	}
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state CircuitState
		want  string
	}{
		{CircuitClosed, "closed"},
		{CircuitOpen, "open"},
		{CircuitHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("CircuitState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}
