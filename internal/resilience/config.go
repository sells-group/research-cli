package resilience

import (
	"time"
)

// FromRetryConfig converts config values to a RetryConfig.
func FromRetryConfig(maxAttempts, initialBackoffMs, maxBackoffMs int, multiplier, jitterFraction float64) RetryConfig {
	cfg := DefaultRetryConfig()
	if maxAttempts > 0 {
		cfg.MaxAttempts = maxAttempts
	}
	if initialBackoffMs > 0 {
		cfg.InitialBackoff = time.Duration(initialBackoffMs) * time.Millisecond
	}
	if maxBackoffMs > 0 {
		cfg.MaxBackoff = time.Duration(maxBackoffMs) * time.Millisecond
	}
	if multiplier > 0 {
		cfg.Multiplier = multiplier
	}
	if jitterFraction >= 0 {
		cfg.JitterFraction = jitterFraction
	}
	return cfg
}

// FromCircuitConfig converts config values to a CircuitBreakerConfig.
func FromCircuitConfig(failureThreshold, resetTimeoutSecs int) CircuitBreakerConfig {
	cfg := DefaultCircuitBreakerConfig()
	if failureThreshold > 0 {
		cfg.FailureThreshold = failureThreshold
	}
	if resetTimeoutSecs > 0 {
		cfg.ResetTimeout = time.Duration(resetTimeoutSecs) * time.Second
	}
	return cfg
}
