package resilience

import (
	"errors"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
)

func TestDLQEntry_CanRetry(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		maxRetries int
		want       bool
	}{
		{"below max", 0, 3, true},
		{"at max", 3, 3, false},
		{"above max", 5, 3, false},
		{"one below max", 2, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := DLQEntry{
				RetryCount: tt.retryCount,
				MaxRetries: tt.maxRetries,
			}
			if got := e.CanRetry(); got != tt.want {
				t.Errorf("CanRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"transient error", NewTransientError(errors.New("503"), 503), "transient"},
		{"permanent error", errors.New("invalid input"), "permanent"},
		{"connection reset", errors.New("connection reset by peer"), "transient"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != tt.want {
				t.Errorf("ClassifyError() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDLQEntry_CompanyURL(t *testing.T) {
	e := DLQEntry{
		Company: model.Company{URL: "https://example.com", Name: "Test Corp"},
	}
	if e.Company.URL != "https://example.com" {
		t.Errorf("expected company URL, got %q", e.Company.URL)
	}
}
