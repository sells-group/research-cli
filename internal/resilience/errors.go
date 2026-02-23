package resilience

import (
	"errors"
	"net"
	"strings"
	"syscall"
)

// TransientError wraps an error that is safe to retry (e.g., 429, 5xx, network timeout).
type TransientError struct {
	Err        error
	StatusCode int
}

func (e *TransientError) Error() string {
	return e.Err.Error()
}

func (e *TransientError) Unwrap() error {
	return e.Err
}

// NewTransientError wraps an error as transient with an optional HTTP status code.
func NewTransientError(err error, statusCode int) *TransientError {
	return &TransientError{Err: err, StatusCode: statusCode}
}

// IsTransient returns true if the error (or any error in its chain) is a
// TransientError, or if it matches common transient error patterns (network
// timeouts, connection resets, DNS failures).
func IsTransient(err error) bool {
	if err == nil {
		return false
	}

	// Check for explicit TransientError in chain.
	var te *TransientError
	if errors.As(err, &te) {
		return true
	}

	// Check for network-level transient errors.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Connection reset / refused / DNS.
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNABORTED) {
		return true
	}

	// String-based heuristics for wrapped errors from HTTP clients.
	msg := strings.ToLower(err.Error())
	transientPatterns := []string{
		"connection reset by peer",
		"broken pipe",
		"temporary failure in name resolution",
		"no such host",
		"tls handshake timeout",
		"i/o timeout",
		"server closed idle connection",
		"transport connection broken",
	}
	for _, p := range transientPatterns {
		if strings.Contains(msg, p) {
			return true
		}
	}

	return false
}

// IsTransientHTTPStatus returns true if the HTTP status code indicates a
// transient server-side issue that is safe to retry.
func IsTransientHTTPStatus(statusCode int) bool {
	switch statusCode {
	case 408, // Request Timeout
		429, // Too Many Requests
		500, // Internal Server Error
		502, // Bad Gateway
		503, // Service Unavailable
		504: // Gateway Timeout
		return true
	default:
		return false
	}
}
