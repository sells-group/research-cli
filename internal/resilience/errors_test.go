package resilience

import (
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
)

func TestIsTransient_ExplicitTransientError(t *testing.T) {
	err := NewTransientError(errors.New("server overloaded"), 503)
	if !IsTransient(err) {
		t.Error("expected TransientError to be transient")
	}
}

func TestIsTransient_WrappedTransientError(t *testing.T) {
	inner := NewTransientError(errors.New("rate limited"), 429)
	wrapped := fmt.Errorf("api call failed: %w", inner)
	if !IsTransient(wrapped) {
		t.Error("expected wrapped TransientError to be transient")
	}
}

func TestIsTransient_NilError(t *testing.T) {
	if IsTransient(nil) {
		t.Error("nil error should not be transient")
	}
}

func TestIsTransient_RegularError(t *testing.T) {
	err := errors.New("invalid input: missing field")
	if IsTransient(err) {
		t.Error("regular error should not be transient")
	}
}

func TestIsTransient_ConnectionReset(t *testing.T) {
	err := fmt.Errorf("write tcp: %w", syscall.ECONNRESET)
	if !IsTransient(err) {
		t.Error("ECONNRESET should be transient")
	}
}

func TestIsTransient_ConnectionRefused(t *testing.T) {
	err := fmt.Errorf("dial tcp: %w", syscall.ECONNREFUSED)
	if !IsTransient(err) {
		t.Error("ECONNREFUSED should be transient")
	}
}

func TestIsTransient_NetworkTimeout(t *testing.T) {
	err := &net.DNSError{IsTimeout: true, Err: "timeout"}
	if !IsTransient(err) {
		t.Error("network timeout should be transient")
	}
}

func TestIsTransient_StringPatterns(t *testing.T) {
	patterns := []string{
		"connection reset by peer",
		"broken pipe",
		"TLS handshake timeout",
		"i/o timeout",
		"server closed idle connection",
	}
	for _, p := range patterns {
		err := errors.New(p)
		if !IsTransient(err) {
			t.Errorf("expected %q to be transient", p)
		}
	}
}

func TestIsTransientHTTPStatus(t *testing.T) {
	transient := []int{408, 429, 500, 502, 503, 504}
	for _, code := range transient {
		if !IsTransientHTTPStatus(code) {
			t.Errorf("expected HTTP %d to be transient", code)
		}
	}

	permanent := []int{200, 201, 400, 401, 403, 404, 405, 409, 422}
	for _, code := range permanent {
		if IsTransientHTTPStatus(code) {
			t.Errorf("expected HTTP %d to NOT be transient", code)
		}
	}
}

func TestTransientError_Unwrap(t *testing.T) {
	inner := errors.New("root cause")
	te := NewTransientError(inner, 500)

	if !errors.Is(te, inner) {
		t.Error("TransientError.Unwrap should return the inner error")
	}

	if te.StatusCode != 500 {
		t.Errorf("expected StatusCode 500, got %d", te.StatusCode)
	}
}

func TestTransientError_ErrorMessage(t *testing.T) {
	inner := errors.New("something went wrong")
	te := NewTransientError(inner, 503)

	if te.Error() != "something went wrong" {
		t.Errorf("expected error message %q, got %q", inner.Error(), te.Error())
	}
}
