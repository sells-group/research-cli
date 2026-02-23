package resilience

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"
)

// RetryConfig controls retry behavior with exponential backoff and jitter.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts (including the first try).
	// A value of 1 means no retries. Default: 3.
	MaxAttempts int

	// InitialBackoff is the base delay before the first retry. Default: 500ms.
	InitialBackoff time.Duration

	// MaxBackoff caps the backoff duration. Default: 30s.
	MaxBackoff time.Duration

	// Multiplier scales the backoff after each attempt. Default: 2.0.
	Multiplier float64

	// JitterFraction adds random jitter as a fraction of the computed delay
	// (0.0 = no jitter, 0.5 = ±50%). Default: 0.25.
	JitterFraction float64

	// ShouldRetry optionally overrides the default transient-error check.
	// If nil, IsTransient is used.
	ShouldRetry func(err error) bool

	// OnRetry is called before each retry sleep with attempt number and error.
	OnRetry func(attempt int, err error)
}

// DefaultRetryConfig returns a sensible retry configuration for API calls.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.25,
	}
}

// Do executes fn with retry logic according to cfg. It retries only on
// errors deemed transient (via ShouldRetry or the default IsTransient check).
// Context cancellation stops retries immediately.
func Do(ctx context.Context, cfg RetryConfig, fn func(ctx context.Context) error) error {
	cfg = applyDefaults(cfg)

	shouldRetry := cfg.ShouldRetry
	if shouldRetry == nil {
		shouldRetry = IsTransient
	}

	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		// Don't retry on context cancellation.
		if ctx.Err() != nil {
			return lastErr
		}

		// Don't retry non-transient errors.
		if !shouldRetry(lastErr) {
			return lastErr
		}

		// Don't sleep after the last attempt.
		if attempt >= cfg.MaxAttempts-1 {
			break
		}

		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt+1, lastErr)
		}

		delay := computeBackoff(attempt, cfg)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}
	}

	return lastErr
}

// DoVal executes fn returning a value with retry logic. Same semantics as Do
// but preserves the return value from the successful call.
func DoVal[T any](ctx context.Context, cfg RetryConfig, fn func(ctx context.Context) (T, error)) (T, error) {
	cfg = applyDefaults(cfg)

	shouldRetry := cfg.ShouldRetry
	if shouldRetry == nil {
		shouldRetry = IsTransient
	}

	var zero T
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		val, err := fn(ctx)
		if err == nil {
			return val, nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return zero, lastErr
		}

		if !shouldRetry(lastErr) {
			return zero, lastErr
		}

		if attempt >= cfg.MaxAttempts-1 {
			break
		}

		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt+1, lastErr)
		}

		delay := computeBackoff(attempt, cfg)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, lastErr
		case <-timer.C:
		}
	}

	return zero, lastErr
}

func applyDefaults(cfg RetryConfig) RetryConfig {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.Multiplier <= 0 {
		cfg.Multiplier = 2.0
	}
	if cfg.JitterFraction < 0 {
		cfg.JitterFraction = 0
	}
	return cfg
}

func computeBackoff(attempt int, cfg RetryConfig) time.Duration {
	delay := float64(cfg.InitialBackoff) * math.Pow(cfg.Multiplier, float64(attempt))
	if delay > float64(cfg.MaxBackoff) {
		delay = float64(cfg.MaxBackoff)
	}

	// Apply jitter: ±JitterFraction of delay.
	if cfg.JitterFraction > 0 {
		jitterRange := delay * cfg.JitterFraction
		jitter := (rand.Float64()*2 - 1) * jitterRange // [-jitterRange, +jitterRange]
		delay += jitter
	}

	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}

// RetryLogger returns an OnRetry callback that logs each retry attempt.
func RetryLogger(service, operation string) func(int, error) {
	return func(attempt int, err error) {
		zap.L().Warn("retrying operation",
			zap.String("service", service),
			zap.String("operation", operation),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)
	}
}
