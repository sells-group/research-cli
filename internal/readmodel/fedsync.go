package readmodel

import (
	"context"
	"strings"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

type postgresFedsync struct {
	pool     db.Pool
	registry *dataset.Registry
	syncLog  *fedsync.SyncLog
}

type syncLogEntry struct {
	status     string
	rowsSynced int64
	startedAt  time.Time
	metadata   map[string]any
}

// ListDatasetStatuses implements FedsyncReader.
func (p *postgresFedsync) ListDatasetStatuses(ctx context.Context) ([]DatasetStatus, error) {
	logMap := make(map[string]syncLogEntry)
	rows, err := p.pool.Query(ctx, `
		SELECT dataset, status, rows_synced, started_at, metadata
		FROM fed_data.mv_dataset_status_latest`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: latest fedsync log entries")
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name       string
			status     string
			rowsSynced int64
			startedAt  time.Time
			metadata   map[string]any
		)
		if err := rows.Scan(&name, &status, &rowsSynced, &startedAt, &metadata); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan latest fedsync log entry")
		}
		logMap[name] = syncLogEntry{
			status:     status,
			rowsSynced: rowsSynced,
			startedAt:  startedAt,
			metadata:   metadata,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate latest fedsync log entries")
	}

	rowCounts := make(map[string]int64)
	countRows, err := p.pool.Query(ctx, `
		SELECT relname, COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = 'fed_data'`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: fedsync table row counts")
	}
	defer countRows.Close()

	for countRows.Next() {
		var name string
		var count int64
		if err := countRows.Scan(&name, &count); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan fedsync row count")
		}
		rowCounts[name] = count
	}
	if err := countRows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate fedsync row counts")
	}

	successMap := make(map[string]time.Time)
	successRows, err := p.pool.Query(ctx, `
		SELECT DISTINCT ON (dataset) dataset, started_at
		FROM fed_data.sync_log
		WHERE status = 'complete'
		ORDER BY dataset, started_at DESC`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: last successful syncs")
	}
	defer successRows.Close()

	for successRows.Next() {
		var name string
		var startedAt time.Time
		if err := successRows.Scan(&name, &startedAt); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan successful sync")
		}
		successMap[name] = startedAt
	}
	if err := successRows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate successful syncs")
	}

	datasets := p.registry.All()
	statuses := make([]DatasetStatus, 0, len(datasets))
	for _, ds := range datasets {
		entry := logMap[ds.Name()]

		tableName := ds.Table()
		if idx := strings.Index(tableName, "."); idx >= 0 {
			tableName = tableName[idx+1:]
		}

		cadence := string(ds.Cadence())
		var lastSync *time.Time
		if t, ok := successMap[ds.Name()]; ok {
			lastSync = &t
		}

		statuses = append(statuses, DatasetStatus{
			Name:       ds.Name(),
			Table:      ds.Table(),
			Phase:      ds.Phase().String(),
			Cadence:    cadence,
			LastSync:   lastSync,
			LastStatus: entry.status,
			RowsSynced: entry.rowsSynced,
			RowCount:   rowCounts[tableName],
			NextDue:    computeNextDue(cadence, lastSync),
			Metadata:   entry.metadata,
		})
	}

	return statuses, nil
}

// ListSyncEntries implements FedsyncReader.
func (p *postgresFedsync) ListSyncEntries(ctx context.Context) ([]fedsync.SyncEntry, error) {
	return p.syncLog.ListAll(ctx)
}

func newRegistry(cfg *config.Config) *dataset.Registry {
	return dataset.NewRegistry(cfg)
}

func computeNextDue(cadence string, lastSync *time.Time) *time.Time {
	if lastSync == nil {
		return nil
	}

	var next time.Time
	switch cadence {
	case "daily":
		next = lastSync.AddDate(0, 0, 1)
	case "weekly":
		next = lastSync.AddDate(0, 0, 7)
	case "monthly":
		next = lastSync.AddDate(0, 1, 0)
	case "quarterly":
		next = lastSync.AddDate(0, 3, 0)
	case "annual":
		next = lastSync.AddDate(1, 0, 0)
	default:
		return nil
	}
	return &next
}
