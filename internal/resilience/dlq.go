package resilience

import (
	"time"

	"github.com/sells-group/research-cli/internal/model"
)

// DLQEntry represents a failed company that can be retried later.
type DLQEntry struct {
	ID           string        `json:"id"`
	Company      model.Company `json:"company"`
	Error        string        `json:"error"`
	ErrorType    string        `json:"error_type"` // "transient" or "permanent"
	FailedPhase  string        `json:"failed_phase,omitempty"`
	RetryCount   int           `json:"retry_count"`
	MaxRetries   int           `json:"max_retries"`
	NextRetryAt  time.Time     `json:"next_retry_at"`
	CreatedAt    time.Time     `json:"created_at"`
	LastFailedAt time.Time     `json:"last_failed_at"`
}

// DLQFilter specifies criteria for querying the dead letter queue.
type DLQFilter struct {
	ErrorType string `json:"error_type,omitempty"` // "transient", "permanent", or "" for all
	Limit     int    `json:"limit,omitempty"`
}

// CanRetry returns true if this entry hasn't exceeded its max retry count.
func (e *DLQEntry) CanRetry() bool {
	return e.RetryCount < e.MaxRetries
}

// ClassifyError categorizes an error as "transient" or "permanent".
func ClassifyError(err error) string {
	if IsTransient(err) {
		return "transient"
	}
	return "permanent"
}
