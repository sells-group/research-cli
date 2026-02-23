package monitoring

import (
	"context"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
)

// MetricsSnapshot holds a point-in-time view of system health.
type MetricsSnapshot struct {
	// Pipeline metrics (within lookback window).
	PipelineTotal     int     `json:"pipeline_total"`
	PipelineComplete  int     `json:"pipeline_complete"`
	PipelineFailed    int     `json:"pipeline_failed"`
	PipelineQueued    int     `json:"pipeline_queued"`
	PipelineFailRate  float64 `json:"pipeline_fail_rate"`
	PipelineCostUSD   float64 `json:"pipeline_cost_usd"`
	PipelineAvgScore  float64 `json:"pipeline_avg_score"`
	PipelineAvgTokens int     `json:"pipeline_avg_tokens"`

	// Fedsync metrics (within lookback window).
	FedsyncTotal    int `json:"fedsync_total"`
	FedsyncComplete int `json:"fedsync_complete"`
	FedsyncFailed   int `json:"fedsync_failed"`
	FedsyncRunning  int `json:"fedsync_running"`

	// DLQ depth.
	DLQDepth int `json:"dlq_depth"`

	// Metadata.
	LookbackHours int       `json:"lookback_hours"`
	CollectedAt   time.Time `json:"collected_at"`
}

// SyncLogQuerier abstracts the fedsync SyncLog methods needed by the collector.
type SyncLogQuerier interface {
	ListAll(ctx context.Context) ([]fedsync.SyncEntry, error)
}

// Collector gathers metrics from the store and sync log.
type Collector struct {
	store   store.Store
	syncLog SyncLogQuerier
}

// NewCollector creates a new metrics collector.
func NewCollector(st store.Store, syncLog SyncLogQuerier) *Collector {
	return &Collector{store: st, syncLog: syncLog}
}

// Collect gathers a snapshot of system metrics over the given lookback window.
func (c *Collector) Collect(ctx context.Context, lookbackHours int) (*MetricsSnapshot, error) {
	snap := &MetricsSnapshot{
		LookbackHours: lookbackHours,
		CollectedAt:   time.Now().UTC(),
	}

	cutoff := time.Now().UTC().Add(-time.Duration(lookbackHours) * time.Hour)

	// Fetch pipeline runs within the window.
	runs, err := c.store.ListRuns(ctx, store.RunFilter{
		CreatedAfter: cutoff,
		Limit:        10000,
	})
	if err != nil {
		return nil, eris.Wrap(err, "monitoring: list runs")
	}

	snap.PipelineTotal = len(runs)
	var totalCost float64
	var totalScore float64
	var totalTokens int
	var scoredRuns int

	for _, r := range runs {
		switch r.Status {
		case model.RunStatusComplete:
			snap.PipelineComplete++
		case model.RunStatusFailed:
			snap.PipelineFailed++
		case model.RunStatusQueued:
			snap.PipelineQueued++
		}
		if r.Result != nil {
			totalCost += r.Result.TotalCost
			totalTokens += r.Result.TotalTokens
			if r.Result.Score > 0 {
				totalScore += r.Result.Score
				scoredRuns++
			}
		}
	}

	snap.PipelineCostUSD = totalCost
	if snap.PipelineTotal > 0 {
		finished := snap.PipelineComplete + snap.PipelineFailed
		if finished > 0 {
			snap.PipelineFailRate = float64(snap.PipelineFailed) / float64(finished)
		}
		snap.PipelineAvgTokens = totalTokens / snap.PipelineTotal
	}
	if scoredRuns > 0 {
		snap.PipelineAvgScore = totalScore / float64(scoredRuns)
	}

	// Fedsync metrics.
	if c.syncLog != nil {
		entries, err := c.syncLog.ListAll(ctx)
		if err != nil {
			return nil, eris.Wrap(err, "monitoring: list sync entries")
		}
		for _, e := range entries {
			if e.StartedAt.Before(cutoff) {
				continue
			}
			snap.FedsyncTotal++
			switch e.Status {
			case "complete":
				snap.FedsyncComplete++
			case "failed":
				snap.FedsyncFailed++
			case "running":
				snap.FedsyncRunning++
			}
		}
	}

	// DLQ depth.
	dlqCount, err := c.store.CountDLQ(ctx)
	if err != nil {
		return nil, eris.Wrap(err, "monitoring: count dlq")
	}
	snap.DLQDepth = dlqCount

	return snap, nil
}
