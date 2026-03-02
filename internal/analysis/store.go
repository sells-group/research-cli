package analysis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// LogEntry represents a row in geo.analysis_log.
type LogEntry struct {
	ID           int64          `json:"id"`
	Analyzer     string         `json:"analyzer"`
	Status       string         `json:"status"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
	RowsAffected int64          `json:"rows_affected"`
	Error        string         `json:"error,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// Log provides read/write access to the geo.analysis_log table.
type Log struct {
	pool db.Pool
}

// NewLog creates a new Log backed by the given connection pool.
func NewLog(pool db.Pool) *Log {
	return &Log{pool: pool}
}

// Start records the beginning of an analysis run and returns its ID.
func (l *Log) Start(ctx context.Context, analyzer string) (int64, error) {
	var id int64
	err := l.pool.QueryRow(ctx,
		`INSERT INTO geo.analysis_log (analyzer, status, started_at)
		 VALUES ($1, 'running', now()) RETURNING id`,
		analyzer,
	).Scan(&id)
	if err != nil {
		return 0, eris.Wrapf(err, "analysis log: start for %s", analyzer)
	}
	return id, nil
}

// Complete marks an analysis run as successfully completed.
func (l *Log) Complete(ctx context.Context, runID int64, result *RunResult) error {
	var metaJSON []byte
	if result != nil && result.Metadata != nil {
		var err error
		metaJSON, err = json.Marshal(result.Metadata)
		if err != nil {
			return eris.Wrap(err, "analysis log: marshal metadata")
		}
	}

	rowsAffected := int64(0)
	if result != nil {
		rowsAffected = result.RowsAffected
	}

	_, err := l.pool.Exec(ctx,
		`UPDATE geo.analysis_log
		 SET status = 'complete', completed_at = now(), rows_affected = $1, metadata = $2
		 WHERE id = $3`,
		rowsAffected, metaJSON, runID,
	)
	if err != nil {
		return eris.Wrapf(err, "analysis log: complete run %d", runID)
	}
	return nil
}

// Fail marks an analysis run as failed with an error message.
func (l *Log) Fail(ctx context.Context, runID int64, errMsg string) error {
	_, err := l.pool.Exec(ctx,
		`UPDATE geo.analysis_log
		 SET status = 'failed', completed_at = now(), error = $1
		 WHERE id = $2`,
		errMsg, runID,
	)
	if err != nil {
		return eris.Wrapf(err, "analysis log: fail run %d", runID)
	}
	return nil
}

// LastSuccess returns the started_at time of the most recent successful run
// for an analyzer. Returns nil if the analyzer has never run successfully.
func (l *Log) LastSuccess(ctx context.Context, analyzer string) (*time.Time, error) {
	var t time.Time
	err := l.pool.QueryRow(ctx,
		`SELECT started_at FROM geo.analysis_log
		 WHERE analyzer = $1 AND status = 'complete'
		 ORDER BY started_at DESC LIMIT 1`,
		analyzer,
	).Scan(&t)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "analysis log: last success for %s", analyzer)
	}
	return &t, nil
}

// ListAll returns all analysis log entries ordered by most recent first.
func (l *Log) ListAll(ctx context.Context) ([]LogEntry, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT id, analyzer, status, started_at, completed_at, rows_affected, error, metadata
		 FROM geo.analysis_log ORDER BY started_at DESC`,
	)
	if err != nil {
		return nil, eris.Wrap(err, "analysis log: list all")
	}
	defer rows.Close()

	var entries []LogEntry
	for rows.Next() {
		var e LogEntry
		var completedAt *time.Time
		var errStr *string
		var metaJSON []byte
		if err := rows.Scan(&e.ID, &e.Analyzer, &e.Status, &e.StartedAt,
			&completedAt, &e.RowsAffected, &errStr, &metaJSON); err != nil {
			return nil, eris.Wrap(err, "analysis log: scan entry")
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
