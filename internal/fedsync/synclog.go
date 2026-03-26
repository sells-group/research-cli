// Package fedsync provides federal data sync tracking and orchestration.
package fedsync

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/db"
)

// SyncEntry represents a row in fed_data.sync_log.
type SyncEntry struct {
	ID          int64          `json:"id"`
	Dataset     string         `json:"dataset"`
	Status      string         `json:"status"`
	StartedAt   time.Time      `json:"started_at"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	RowsSynced  int64          `json:"rows_synced"`
	Error       string         `json:"error,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// SyncResult holds the outcome of a dataset sync, passed to Complete().
type SyncResult struct {
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// SyncSummary aggregates sync log entries over a time window.
type SyncSummary struct {
	Total    int `json:"total"`
	Complete int `json:"complete"`
	Failed   int `json:"failed"`
	Running  int `json:"running"`
}

// SyncLog provides read/write access to the fed_data.sync_log table.
type SyncLog struct {
	pool  db.Pool
	cache apicache.Cache
}

// NewSyncLog creates a new SyncLog backed by the given connection pool.
func NewSyncLog(pool db.Pool) *SyncLog {
	return &SyncLog{pool: pool}
}

// SetCache configures shared cache invalidation for sync-log writes.
func (s *SyncLog) SetCache(cache apicache.Cache) {
	s.cache = cache
}

func (s *SyncLog) invalidateStatuses() {
	if s.cache == nil {
		return
	}
	_ = s.cache.DeleteDomains(apicache.DomainFedsync)
}

func (s *SyncLog) refreshLatestStatusView(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `REFRESH MATERIALIZED VIEW fed_data.mv_dataset_status_latest`)
}

func (s *SyncLog) refreshDailyTrendsView(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `REFRESH MATERIALIZED VIEW fed_data.mv_sync_daily_trends`)
}

// LastSuccess returns the started_at time of the most recent successful sync for a dataset.
// Returns nil if the dataset has never been synced successfully.
func (s *SyncLog) LastSuccess(ctx context.Context, dataset string) (*time.Time, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT started_at FROM fed_data.sync_log
		 WHERE dataset = $1 AND status = 'complete'
		 ORDER BY started_at DESC LIMIT 1`,
		dataset,
	).Scan(&t)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "synclog: last success for %s", dataset)
	}
	return &t, nil
}

// LastSuccessWithMeta returns the started_at time AND metadata from the last successful sync.
// Returns nil time if the dataset has never been synced successfully.
func (s *SyncLog) LastSuccessWithMeta(ctx context.Context, dataset string) (*time.Time, map[string]any, error) {
	var t time.Time
	var metaJSON []byte
	err := s.pool.QueryRow(ctx,
		`SELECT started_at, metadata FROM fed_data.sync_log
		 WHERE dataset = $1 AND status = 'complete'
		 ORDER BY started_at DESC LIMIT 1`,
		dataset,
	).Scan(&t, &metaJSON)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil, nil
		}
		return nil, nil, eris.Wrapf(err, "synclog: last success with meta for %s", dataset)
	}
	var meta map[string]any
	if metaJSON != nil {
		_ = json.Unmarshal(metaJSON, &meta)
	}
	return &t, meta, nil
}

// Start records the beginning of a sync run and returns its ID.
func (s *SyncLog) Start(ctx context.Context, dataset string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO fed_data.sync_log (dataset, status, started_at)
		 VALUES ($1, 'running', now()) RETURNING id`,
		dataset,
	).Scan(&id)
	if err != nil {
		return 0, eris.Wrapf(err, "synclog: start sync for %s", dataset)
	}
	s.refreshLatestStatusView(ctx)
	s.invalidateStatuses()
	return id, nil
}

// Complete marks a sync run as successfully completed.
func (s *SyncLog) Complete(ctx context.Context, syncID int64, result *SyncResult) error {
	var metaJSON []byte
	if result != nil && result.Metadata != nil {
		var err error
		metaJSON, err = json.Marshal(result.Metadata)
		if err != nil {
			return eris.Wrap(err, "synclog: marshal metadata")
		}
	}

	rowsSynced := int64(0)
	if result != nil {
		rowsSynced = result.RowsSynced
	}

	_, err := s.pool.Exec(ctx,
		`UPDATE fed_data.sync_log
		 SET status = 'complete', completed_at = now(), rows_synced = $1, metadata = $2
		 WHERE id = $3`,
		rowsSynced, metaJSON, syncID,
	)
	if err != nil {
		return eris.Wrapf(err, "synclog: complete sync %d", syncID)
	}
	s.refreshLatestStatusView(ctx)
	s.refreshDailyTrendsView(ctx)
	s.invalidateStatuses()
	return nil
}

// Fail marks a sync run as failed with an error message.
func (s *SyncLog) Fail(ctx context.Context, syncID int64, errMsg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE fed_data.sync_log
		 SET status = 'failed', completed_at = now(), error = $1
		 WHERE id = $2`,
		errMsg, syncID,
	)
	if err != nil {
		return eris.Wrapf(err, "synclog: fail sync %d", syncID)
	}
	s.refreshLatestStatusView(ctx)
	s.refreshDailyTrendsView(ctx)
	s.invalidateStatuses()
	return nil
}

// ListAll returns all sync log entries ordered by most recent first.
func (s *SyncLog) ListAll(ctx context.Context) ([]SyncEntry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, dataset, status, started_at, completed_at, rows_synced, error, metadata
		 FROM fed_data.sync_log ORDER BY started_at DESC`,
	)
	if err != nil {
		return nil, eris.Wrap(err, "synclog: list all")
	}
	defer rows.Close()

	var entries []SyncEntry
	for rows.Next() {
		var e SyncEntry
		var completedAt *time.Time
		var errStr *string
		var metaJSON []byte
		if err := rows.Scan(&e.ID, &e.Dataset, &e.Status, &e.StartedAt, &completedAt, &e.RowsSynced, &errStr, &metaJSON); err != nil {
			return nil, eris.Wrap(err, "synclog: scan entry")
		}
		e.CompletedAt = completedAt
		if errStr != nil {
			e.Error = *errStr
		}
		if metaJSON != nil {
			_ = json.Unmarshal(metaJSON, &e.Metadata)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// SummarizeSince returns aggregate sync counts within the given time window.
func (s *SyncLog) SummarizeSince(ctx context.Context, since time.Time) (*SyncSummary, error) {
	summary := &SyncSummary{}
	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'complete'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'running')
		FROM fed_data.sync_log
		WHERE started_at >= $1`,
		since,
	).Scan(&summary.Total, &summary.Complete, &summary.Failed, &summary.Running)
	if err != nil {
		return nil, eris.Wrap(err, "synclog: summarize since")
	}
	return summary, nil
}
