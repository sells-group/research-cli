package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunStatusValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status RunStatus
		want   string
	}{
		{RunStatusQueued, "queued"},
		{RunStatusCrawling, "crawling"},
		{RunStatusClassifying, "classifying"},
		{RunStatusExtracting, "extracting"},
		{RunStatusAggregating, "aggregating"},
		{RunStatusWritingSF, "writing_sf"},
		{RunStatusComplete, "complete"},
		{RunStatusFailed, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, string(tt.status))
		})
	}
}

func TestPhaseStatusValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status PhaseStatus
		want   string
	}{
		{PhaseStatusRunning, "running"},
		{PhaseStatusComplete, "complete"},
		{PhaseStatusFailed, "failed"},
		{PhaseStatusSkipped, "skipped"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, string(tt.status))
		})
	}
}
