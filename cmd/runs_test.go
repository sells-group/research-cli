//go:build !integration

package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
)

func TestFormatRunsList(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	runs := []model.Run{
		{
			ID:        "abc12345-6789-0000-0000-000000000000",
			Company:   model.Company{URL: "https://acme.com", Name: "Acme Corp"},
			Status:    model.RunStatusComplete,
			CreatedAt: now,
			UpdatedAt: now.Add(2 * time.Minute),
		},
		{
			ID:        "def12345-6789-0000-0000-000000000000",
			Company:   model.Company{URL: "https://beta.com", Name: "Beta Inc"},
			Status:    model.RunStatusCrawling,
			CreatedAt: now.Add(-1 * time.Hour),
			UpdatedAt: now.Add(-30 * time.Minute),
		},
	}

	var buf bytes.Buffer
	formatRunsList(&buf, runs)

	output := buf.String()
	assert.Contains(t, output, "ID")
	assert.Contains(t, output, "COMPANY")
	assert.Contains(t, output, "STATUS")
	assert.Contains(t, output, "Acme Corp")
	assert.Contains(t, output, "complete")
	assert.Contains(t, output, "Beta Inc")
	assert.Contains(t, output, "crawling")
	assert.Contains(t, output, "2025-06-15 10:30")
	assert.Contains(t, output, "abc12345")
}

func TestFormatRunsList_FailedRun(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	runs := []model.Run{
		{
			ID:      "abc12345-6789-0000-0000-000000000000",
			Company: model.Company{URL: "https://fail.com", Name: "FailCo"},
			Status:  model.RunStatusFailed,
			Error: &model.RunError{
				Message:     "all Phase 1 data sources failed",
				Category:    model.ErrorCategoryTransient,
				FailedPhase: "1_data_collection",
			},
			CreatedAt: now,
			UpdatedAt: now.Add(30 * time.Second),
		},
	}

	var buf bytes.Buffer
	formatRunsList(&buf, runs)

	output := buf.String()
	assert.Contains(t, output, "FailCo")
	assert.Contains(t, output, "failed")
	assert.Contains(t, output, "transient")
}

func TestRunsStats(t *testing.T) {
	now := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)

	runs := []model.Run{
		{
			ID:        "1",
			Status:    model.RunStatusComplete,
			CreatedAt: now,
			UpdatedAt: now.Add(2 * time.Minute),
		},
		{
			ID:        "2",
			Status:    model.RunStatusComplete,
			CreatedAt: now.Add(5 * time.Minute),
			UpdatedAt: now.Add(8 * time.Minute),
		},
		{
			ID:     "3",
			Status: model.RunStatusFailed,
			Error: &model.RunError{
				Message:  "timeout",
				Category: model.ErrorCategoryTransient,
			},
			CreatedAt: now.Add(10 * time.Minute),
			UpdatedAt: now.Add(10*time.Minute + 30*time.Second),
		},
		{
			ID:     "4",
			Status: model.RunStatusFailed,
			Error: &model.RunError{
				Message:  "invalid url",
				Category: model.ErrorCategoryPermanent,
			},
			CreatedAt: now.Add(15 * time.Minute),
			UpdatedAt: now.Add(15*time.Minute + 10*time.Second),
		},
	}

	stats := computeRunStats(runs)
	assert.Equal(t, 4, stats.Total)
	assert.Equal(t, 2, stats.Complete)
	assert.Equal(t, 2, stats.Failed)
	assert.Equal(t, 1, stats.Transient)
	assert.Equal(t, 1, stats.Permanent)
	// Average duration of the 2 complete runs: (120s + 180s) / 2 = 150s.
	assert.InDelta(t, 150.0, stats.AvgDurSecs, 0.1)

	var buf bytes.Buffer
	formatRunStats(&buf, stats)

	output := buf.String()
	assert.Contains(t, output, "Total runs:")
	assert.Contains(t, output, "4")
	assert.Contains(t, output, "Complete:")
	assert.Contains(t, output, "2")
	assert.Contains(t, output, "Failed:")
	assert.Contains(t, output, "Transient:")
	assert.Contains(t, output, "Permanent:")
	assert.Contains(t, output, "150.0s")
}

func TestTruncateID(t *testing.T) {
	assert.Equal(t, "abc12345", truncateID("abc12345-6789-0000-0000-000000000000"))
	assert.Equal(t, "short", truncateID("short"))
}
