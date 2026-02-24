package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	var calls int
	err := Do(context.Background(), DefaultRetryConfig(), func(_ context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestDo_SuccessAfterRetry(t *testing.T) {
	var calls int
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}

	err := Do(context.Background(), cfg, func(_ context.Context) error {
		calls++
		if calls < 3 {
			return NewTransientError(errors.New("temporary"), 503)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestDo_ExhaustsRetries(t *testing.T) {
	var calls int
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2.0,
	}

	err := Do(context.Background(), cfg, func(_ context.Context) error {
		calls++
		return NewTransientError(errors.New("always fails"), 500)
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestDo_NonTransientError_NoRetry(t *testing.T) {
	var calls int
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
	}

	err := Do(context.Background(), cfg, func(_ context.Context) error {
		calls++
		return errors.New("permanent error: bad request")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry for non-transient), got %d", calls)
	}
}

func TestDo_ContextCancelled_StopsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	cfg := RetryConfig{
		MaxAttempts:    5,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
	}

	err := Do(ctx, cfg, func(_ context.Context) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return NewTransientError(errors.New("fail"), 500)
	})
	if err == nil {
		t.Fatal("expected error")
	}
	// Should stop after cancel (2 calls max).
	if calls > 3 {
		t.Errorf("expected <= 3 calls after cancel, got %d", calls)
	}
}

func TestDo_CustomShouldRetry(t *testing.T) {
	var calls int
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		ShouldRetry: func(err error) bool {
			return err.Error() == "retry me"
		},
	}

	err := Do(context.Background(), cfg, func(_ context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("retry me")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestDo_OnRetryCallback(t *testing.T) {
	var retryAttempts []int
	cfg := RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Millisecond,
		OnRetry: func(attempt int, _ error) {
			retryAttempts = append(retryAttempts, attempt)
		},
	}

	_ = Do(context.Background(), cfg, func(_ context.Context) error {
		return NewTransientError(errors.New("fail"), 500)
	})

	if len(retryAttempts) != 2 {
		t.Errorf("expected 2 OnRetry calls, got %d", len(retryAttempts))
	}
	if retryAttempts[0] != 1 || retryAttempts[1] != 2 {
		t.Errorf("expected attempts [1, 2], got %v", retryAttempts)
	}
}

func TestDoVal_ReturnsValueOnSuccess(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialBackoff = 1 * time.Millisecond

	var calls int
	val, err := DoVal(context.Background(), cfg, func(_ context.Context) (string, error) {
		calls++
		if calls < 2 {
			return "", NewTransientError(errors.New("fail"), 500)
		}
		return "hello", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Errorf("expected %q, got %q", "hello", val)
	}
}

func TestDoVal_ReturnsZeroOnFailure(t *testing.T) {
	cfg := RetryConfig{
		MaxAttempts:    2,
		InitialBackoff: 1 * time.Millisecond,
	}

	val, err := DoVal(context.Background(), cfg, func(_ context.Context) (int, error) {
		return 42, NewTransientError(errors.New("fail"), 500)
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if val != 0 {
		t.Errorf("expected zero value on failure, got %d", val)
	}
}

func TestDo_DefaultConfig(t *testing.T) {
	// Verify defaults are applied when zero config is given.
	var calls atomic.Int32
	cfg := RetryConfig{} // all zero values

	err := Do(context.Background(), cfg, func(_ context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 call, got %d", calls.Load())
	}
}

func TestComputeBackoff_ExponentialGrowth(t *testing.T) {
	cfg := RetryConfig{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0, // disable jitter for deterministic test
	}
	cfg = applyDefaults(cfg)

	delays := []time.Duration{
		computeBackoff(0, cfg), // 100ms
		computeBackoff(1, cfg), // 200ms
		computeBackoff(2, cfg), // 400ms
		computeBackoff(3, cfg), // 800ms
	}

	expected := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}
	for i, d := range delays {
		if d != expected[i] {
			t.Errorf("attempt %d: expected %v, got %v", i, expected[i], d)
		}
	}
}

func TestComputeBackoff_CapsAtMax(t *testing.T) {
	cfg := RetryConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     10.0,
		JitterFraction: 0,
	}
	cfg = applyDefaults(cfg)

	delay := computeBackoff(5, cfg)
	if delay > 5*time.Second {
		t.Errorf("expected delay capped at 5s, got %v", delay)
	}
}

func TestComputeBackoff_WithJitter(t *testing.T) {
	cfg := RetryConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.5,
	}
	cfg = applyDefaults(cfg)

	// Run many times to verify jitter produces varying results.
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := computeBackoff(0, cfg)
		seen[d] = true
		// With 50% jitter on 1s base, delay should be in [500ms, 1500ms].
		if d < 500*time.Millisecond || d > 1500*time.Millisecond {
			t.Errorf("delay %v outside expected range [500ms, 1500ms]", d)
		}
	}
	// Should have produced multiple different values due to jitter.
	if len(seen) < 2 {
		t.Error("expected jitter to produce varying delays")
	}
}

func TestRetryLogger(t *testing.T) {
	t.Parallel()
	// Just verify it doesn't panic.
	logger := RetryLogger("anthropic", "create_message")
	logger(1, errors.New("test error"))
}
