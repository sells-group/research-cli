// Package resilience provides circuit breaker and retry patterns for external service calls.
package resilience

import (
	"context"
	"sync"
	"time"

	"github.com/rotisserie/eris"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	// CircuitClosed is the normal operating state — requests flow through.
	CircuitClosed CircuitState = iota
	// CircuitOpen means too many failures — requests are rejected immediately.
	CircuitOpen
	// CircuitHalfOpen allows a single probe request to test recovery.
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when a call is rejected because the circuit is open.
var ErrCircuitOpen = eris.New("circuit breaker is open")

// CircuitBreakerConfig controls circuit breaker behavior.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening
	// the circuit. Default: 5.
	FailureThreshold int

	// ResetTimeout is how long the circuit stays open before transitioning
	// to half-open. Default: 30s.
	ResetTimeout time.Duration

	// HalfOpenMaxProbes is the number of successful probes required in
	// half-open state before closing the circuit. Default: 1.
	HalfOpenMaxProbes int

	// ShouldTrip optionally overrides the default check. If nil, all errors
	// that pass IsTransient count toward the failure threshold.
	ShouldTrip func(err error) bool

	// OnStateChange is called when the circuit transitions between states.
	OnStateChange func(from, to CircuitState)
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold:  5,
		ResetTimeout:      30 * time.Second,
		HalfOpenMaxProbes: 1,
	}
}

// CircuitBreaker implements the circuit breaker pattern for a single service.
type CircuitBreaker struct {
	cfg   CircuitBreakerConfig
	mu    sync.Mutex
	state CircuitState

	consecutiveFailures int
	lastFailureTime     time.Time
	halfOpenSuccesses   int

	// nowFunc allows test injection of time.
	nowFunc func() time.Time
}

// NewCircuitBreaker creates a circuit breaker with the given config.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.ResetTimeout <= 0 {
		cfg.ResetTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMaxProbes <= 0 {
		cfg.HalfOpenMaxProbes = 1
	}
	return &CircuitBreaker{
		cfg:     cfg,
		state:   CircuitClosed,
		nowFunc: time.Now,
	}
}

// Execute runs fn through the circuit breaker. Returns ErrCircuitOpen if the
// circuit is open. On success, resets the failure counter. On failure (if the
// error should trip the breaker), increments the failure counter.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := cb.allowRequest(); err != nil {
		return err
	}

	err := fn(ctx)
	cb.recordResult(err)
	return err
}

// ExecuteVal is like Execute but preserves a return value.
func ExecuteVal[T any](ctx context.Context, cb *CircuitBreaker, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if err := cb.allowRequest(); err != nil {
		return zero, err
	}

	val, err := fn(ctx)
	cb.recordResult(err)
	return val, err
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check if open circuit should transition to half-open.
	if cb.state == CircuitOpen && cb.nowFunc().Sub(cb.lastFailureTime) >= cb.cfg.ResetTimeout {
		return CircuitHalfOpen
	}
	return cb.state
}

// Reset forces the circuit back to closed state. Useful for testing or
// manual recovery.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	old := cb.state
	cb.state = CircuitClosed
	cb.consecutiveFailures = 0
	cb.halfOpenSuccesses = 0
	if old != CircuitClosed && cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(old, CircuitClosed)
	}
}

// Counters returns the current failure count and state for observability.
func (cb *CircuitBreaker) Counters() (consecutiveFailures int, state CircuitState) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFailures, cb.state
}

func (cb *CircuitBreaker) allowRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		// Check if enough time has passed to try half-open.
		if cb.nowFunc().Sub(cb.lastFailureTime) >= cb.cfg.ResetTimeout {
			cb.transition(CircuitHalfOpen)
			return nil // Allow probe request.
		}
		return ErrCircuitOpen
	case CircuitHalfOpen:
		return nil // Allow probe request.
	default:
		return nil
	}
}

func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	shouldTrip := cb.cfg.ShouldTrip
	if shouldTrip == nil {
		shouldTrip = func(e error) bool { return e != nil }
	}

	if err == nil || !shouldTrip(err) {
		// Success.
		switch cb.state {
		case CircuitHalfOpen:
			cb.halfOpenSuccesses++
			if cb.halfOpenSuccesses >= cb.cfg.HalfOpenMaxProbes {
				cb.transition(CircuitClosed)
				cb.consecutiveFailures = 0
				cb.halfOpenSuccesses = 0
			}
		case CircuitClosed:
			cb.consecutiveFailures = 0
		}
		return
	}

	// Failure.
	cb.consecutiveFailures++
	cb.lastFailureTime = cb.nowFunc()

	switch cb.state {
	case CircuitClosed:
		if cb.consecutiveFailures >= cb.cfg.FailureThreshold {
			cb.transition(CircuitOpen)
		}
	case CircuitHalfOpen:
		// Any failure in half-open reopens the circuit.
		cb.transition(CircuitOpen)
		cb.halfOpenSuccesses = 0
	}
}

func (cb *CircuitBreaker) transition(to CircuitState) {
	from := cb.state
	cb.state = to
	if cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(from, to)
	}
}

// ServiceBreakers manages circuit breakers for multiple services.
type ServiceBreakers struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
	cfg      CircuitBreakerConfig
}

// NewServiceBreakers creates a registry of per-service circuit breakers.
func NewServiceBreakers(cfg CircuitBreakerConfig) *ServiceBreakers {
	return &ServiceBreakers{
		breakers: make(map[string]*CircuitBreaker),
		cfg:      cfg,
	}
}

// Get returns the circuit breaker for the named service, creating one if needed.
func (sb *ServiceBreakers) Get(service string) *CircuitBreaker {
	sb.mu.RLock()
	cb, ok := sb.breakers[service]
	sb.mu.RUnlock()
	if ok {
		return cb
	}

	sb.mu.Lock()
	defer sb.mu.Unlock()
	// Double-check after acquiring write lock.
	if cb, ok = sb.breakers[service]; ok {
		return cb
	}
	cb = NewCircuitBreaker(sb.cfg)
	sb.breakers[service] = cb
	return cb
}

// States returns a snapshot of all circuit breaker states.
func (sb *ServiceBreakers) States() map[string]CircuitState {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	states := make(map[string]CircuitState, len(sb.breakers))
	for name, cb := range sb.breakers {
		states[name] = cb.State()
	}
	return states
}
