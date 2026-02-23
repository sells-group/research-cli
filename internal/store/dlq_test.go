package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/resilience"
)

func TestSQLite_DLQ_EnqueueAndDequeue(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	entry := resilience.DLQEntry{
		ID:           "dlq-1",
		Company:      model.Company{URL: "https://acme.com", Name: "Acme Corp"},
		Error:        "503 Service Unavailable",
		ErrorType:    "transient",
		RetryCount:   0,
		MaxRetries:   3,
		NextRetryAt:  time.Now().Add(-1 * time.Minute), // already past → eligible
		CreatedAt:    time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "dlq-1", entries[0].ID)
	assert.Equal(t, "https://acme.com", entries[0].Company.URL)
	assert.Equal(t, "Acme Corp", entries[0].Company.Name)
	assert.Equal(t, "transient", entries[0].ErrorType)
	assert.Equal(t, 0, entries[0].RetryCount)
}

func TestSQLite_DLQ_DequeueFiltersErrorType(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Enqueue transient and permanent entries.
	transient := resilience.DLQEntry{
		ID:          "dlq-t",
		Company:     model.Company{URL: "https://t.com"},
		Error:       "timeout",
		ErrorType:   "transient",
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	permanent := resilience.DLQEntry{
		ID:          "dlq-p",
		Company:     model.Company{URL: "https://p.com"},
		Error:       "404 Not Found",
		ErrorType:   "permanent",
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, transient))
	require.NoError(t, st.EnqueueDLQ(ctx, permanent))

	// Query transient only.
	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{ErrorType: "transient"})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "dlq-t", entries[0].ID)
}

func TestSQLite_DLQ_DequeueRespectsNextRetryAt(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Entry with future next_retry_at — should NOT be dequeued.
	entry := resilience.DLQEntry{
		ID:          "dlq-future",
		Company:     model.Company{URL: "https://future.com"},
		Error:       "timeout",
		ErrorType:   "transient",
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(1 * time.Hour), // future
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestSQLite_DLQ_DequeueRespectsMaxRetries(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Entry that has exhausted retries.
	entry := resilience.DLQEntry{
		ID:          "dlq-exhausted",
		Company:     model.Company{URL: "https://exhausted.com"},
		Error:       "always fails",
		ErrorType:   "transient",
		RetryCount:  3,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestSQLite_DLQ_IncrementRetry(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	entry := resilience.DLQEntry{
		ID:          "dlq-inc",
		Company:     model.Company{URL: "https://inc.com"},
		Error:       "first error",
		ErrorType:   "transient",
		MaxRetries:  5,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	// Increment retry.
	nextRetry := time.Now().Add(5 * time.Minute)
	require.NoError(t, st.IncrementDLQRetry(ctx, "dlq-inc", nextRetry, "second error"))

	// Dequeue should return nothing (next_retry_at is in future).
	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, entries, "entry should not be eligible yet")
}

func TestSQLite_DLQ_IncrementRetry_NotFound(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	err := st.IncrementDLQRetry(ctx, "nonexistent", time.Now(), "error")
	assert.Error(t, err)
}

func TestSQLite_DLQ_Remove(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	entry := resilience.DLQEntry{
		ID:          "dlq-rm",
		Company:     model.Company{URL: "https://rm.com"},
		Error:       "error",
		ErrorType:   "transient",
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	// Verify it's there.
	count, err := st.CountDLQ(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Remove it.
	require.NoError(t, st.RemoveDLQ(ctx, "dlq-rm"))

	count, err = st.CountDLQ(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestSQLite_DLQ_Count(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	// Initially empty.
	count, err := st.CountDLQ(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Add entries.
	for i := 0; i < 3; i++ {
		entry := resilience.DLQEntry{
			ID:          "dlq-count-" + string(rune('a'+i)),
			Company:     model.Company{URL: "https://count.com"},
			Error:       "error",
			ErrorType:   "transient",
			MaxRetries:  3,
			NextRetryAt: time.Now(),
			CreatedAt:   time.Now(),
			LastFailedAt: time.Now(),
		}
		require.NoError(t, st.EnqueueDLQ(ctx, entry))
	}

	count, err = st.CountDLQ(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestSQLite_DLQ_EnqueueReplace(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	entry := resilience.DLQEntry{
		ID:          "dlq-replace",
		Company:     model.Company{URL: "https://replace.com"},
		Error:       "first error",
		ErrorType:   "transient",
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	// Re-enqueue with same ID but updated error.
	entry.Error = "second error"
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	// Should still be one entry.
	count, err := st.CountDLQ(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "second error", entries[0].Error)
}

func TestSQLite_DLQ_DequeueOrdersByNextRetry(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	now := time.Now()
	// Enqueue entries with different next_retry_at times.
	for i, id := range []string{"dlq-c", "dlq-a", "dlq-b"} {
		entry := resilience.DLQEntry{
			ID:          id,
			Company:     model.Company{URL: "https://" + id + ".com"},
			Error:       "error",
			ErrorType:   "transient",
			MaxRetries:  3,
			NextRetryAt: now.Add(time.Duration(-3+i) * time.Minute),
			CreatedAt:   now,
			LastFailedAt: now,
		}
		require.NoError(t, st.EnqueueDLQ(ctx, entry))
	}

	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 3)
	// Should be ordered by next_retry_at ascending.
	assert.Equal(t, "dlq-c", entries[0].ID) // earliest
	assert.Equal(t, "dlq-a", entries[1].ID)
	assert.Equal(t, "dlq-b", entries[2].ID)
}

func TestSQLite_DLQ_DequeueWithFailedPhase(t *testing.T) {
	st := newTestSQLiteStore(t)
	ctx := context.Background()

	entry := resilience.DLQEntry{
		ID:          "dlq-phase",
		Company:     model.Company{URL: "https://phase.com"},
		Error:       "anthropic 503",
		ErrorType:   "transient",
		FailedPhase: "4_extract_t1",
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
		CreatedAt:   time.Now(),
		LastFailedAt: time.Now(),
	}
	require.NoError(t, st.EnqueueDLQ(ctx, entry))

	entries, err := st.DequeueDLQ(ctx, resilience.DLQFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "4_extract_t1", entries[0].FailedPhase)
}
