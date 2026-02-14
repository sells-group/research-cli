//go:build !integration

package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/fedsync"
)

func TestFormatStatusEntries_Empty(t *testing.T) {
	var buf bytes.Buffer
	formatStatusEntries(&buf, nil)

	output := buf.String()
	// Should still have the header even if entries is nil.
	assert.Contains(t, output, "DATASET")
	assert.Contains(t, output, "STATUS")
	assert.Contains(t, output, "STARTED")
}

func TestFormatStatusEntries_SingleEntry(t *testing.T) {
	started := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	completed := started.Add(5 * time.Minute)

	entries := []fedsync.SyncEntry{
		{
			ID:          1,
			Dataset:     "cbp",
			Status:      "complete",
			StartedAt:   started,
			CompletedAt: &completed,
			RowsSynced:  50000,
		},
	}

	var buf bytes.Buffer
	formatStatusEntries(&buf, entries)

	output := buf.String()
	assert.Contains(t, output, "cbp")
	assert.Contains(t, output, "complete")
	assert.Contains(t, output, "2025-01-15 10:30")
	assert.Contains(t, output, "5m0s")
	assert.Contains(t, output, "50000")
}

func TestFormatStatusEntries_NoCompletedAt(t *testing.T) {
	started := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	entries := []fedsync.SyncEntry{
		{
			ID:          2,
			Dataset:     "fpds",
			Status:      "running",
			StartedAt:   started,
			CompletedAt: nil,
			RowsSynced:  0,
		},
	}

	var buf bytes.Buffer
	formatStatusEntries(&buf, entries)

	output := buf.String()
	assert.Contains(t, output, "fpds")
	assert.Contains(t, output, "running")
	assert.Contains(t, output, "-") // duration should be "-"
}

func TestFormatStatusEntries_WithError(t *testing.T) {
	started := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	entries := []fedsync.SyncEntry{
		{
			ID:        3,
			Dataset:   "oews",
			Status:    "failed",
			StartedAt: started,
			Error:     "connection timeout after 30 seconds",
		},
	}

	var buf bytes.Buffer
	formatStatusEntries(&buf, entries)

	output := buf.String()
	assert.Contains(t, output, "oews")
	assert.Contains(t, output, "failed")
	assert.Contains(t, output, "connection timeout")
}

func TestFormatStatusEntries_WithLongError(t *testing.T) {
	started := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	longErr := "this is a very long error message that should be truncated when it exceeds the sixty character limit set in the truncate function"

	entries := []fedsync.SyncEntry{
		{
			ID:        4,
			Dataset:   "fred",
			Status:    "failed",
			StartedAt: started,
			Error:     longErr,
		},
	}

	var buf bytes.Buffer
	formatStatusEntries(&buf, entries)

	output := buf.String()
	assert.Contains(t, output, "fred")
	assert.Contains(t, output, "...")
	// The truncated error should NOT contain the full message.
	assert.NotContains(t, output, longErr)
}

func TestFormatStatusEntries_MultipleEntries(t *testing.T) {
	started1 := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	completed1 := started1.Add(2 * time.Minute)
	started2 := time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC)
	completed2 := started2.Add(30 * time.Second)

	entries := []fedsync.SyncEntry{
		{
			ID:          1,
			Dataset:     "cbp",
			Status:      "complete",
			StartedAt:   started1,
			CompletedAt: &completed1,
			RowsSynced:  10000,
		},
		{
			ID:          2,
			Dataset:     "fpds",
			Status:      "complete",
			StartedAt:   started2,
			CompletedAt: &completed2,
			RowsSynced:  500,
		},
	}

	var buf bytes.Buffer
	formatStatusEntries(&buf, entries)

	output := buf.String()
	assert.Contains(t, output, "cbp")
	assert.Contains(t, output, "fpds")
	assert.Contains(t, output, "10000")
	assert.Contains(t, output, "500")
}
