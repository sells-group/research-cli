package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rotisserie/eris"
	_ "modernc.org/sqlite" // Register the pure-Go SQLite driver.

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/resilience"
)

// SQLiteStore implements Store using modernc.org/sqlite.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLite opens a SQLite database at the given path and configures WAL mode.
func NewSQLite(dsn string) (*SQLiteStore, error) {
	// Embed pragmas in DSN so every pooled connection gets them.
	if !strings.Contains(dsn, "?") {
		dsn += "?"
	} else {
		dsn += "&"
	}
	dsn += "_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: open")
	}
	// Allow enough connections for parallel pipelines + their fan-out phases.
	db.SetMaxOpenConns(10)

	// Verify the connection is usable (sql.Open is lazy).
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, eris.Wrap(err, "sqlite: ping")
	}

	return &SQLiteStore{db: db}, nil
}

const sqliteMigration = `
CREATE TABLE IF NOT EXISTS runs (
	id         TEXT PRIMARY KEY,
	company    TEXT NOT NULL,
	status     TEXT NOT NULL DEFAULT 'queued',
	result     TEXT,
	created_at DATETIME NOT NULL DEFAULT (datetime('now')),
	updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS run_phases (
	id         TEXT PRIMARY KEY,
	run_id     TEXT NOT NULL REFERENCES runs(id),
	name       TEXT NOT NULL,
	status     TEXT NOT NULL DEFAULT 'running',
	result     TEXT,
	started_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS crawl_cache (
	id          TEXT PRIMARY KEY,
	company_url TEXT NOT NULL,
	pages       TEXT NOT NULL,
	crawled_at  DATETIME NOT NULL DEFAULT (datetime('now')),
	expires_at  DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_company ON runs(company);
CREATE INDEX IF NOT EXISTS idx_run_phases_run_id ON run_phases(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_crawl_cache_company_url ON crawl_cache(company_url);
CREATE INDEX IF NOT EXISTS idx_crawl_cache_expires_at ON crawl_cache(expires_at);

CREATE TABLE IF NOT EXISTS linkedin_cache (
	id         TEXT PRIMARY KEY,
	domain     TEXT NOT NULL,
	data       TEXT NOT NULL,
	cached_at  DATETIME NOT NULL DEFAULT (datetime('now')),
	expires_at DATETIME NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_linkedin_cache_domain ON linkedin_cache(domain);
CREATE INDEX IF NOT EXISTS idx_linkedin_cache_expires_at ON linkedin_cache(expires_at);

CREATE TABLE IF NOT EXISTS scrape_cache (
	id         TEXT PRIMARY KEY,
	url_hash   TEXT NOT NULL,
	content    TEXT NOT NULL,
	cached_at  DATETIME NOT NULL DEFAULT (datetime('now')),
	expires_at DATETIME NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_cache_url_hash ON scrape_cache(url_hash);
CREATE INDEX IF NOT EXISTS idx_scrape_cache_expires_at ON scrape_cache(expires_at);

CREATE TABLE IF NOT EXISTS checkpoints (
	company_id TEXT PRIMARY KEY,
	phase      TEXT NOT NULL,
	data       TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
	id            TEXT PRIMARY KEY,
	company       TEXT NOT NULL,
	error         TEXT NOT NULL,
	error_type    TEXT NOT NULL DEFAULT 'transient',
	failed_phase  TEXT,
	retry_count   INTEGER NOT NULL DEFAULT 0,
	max_retries   INTEGER NOT NULL DEFAULT 3,
	next_retry_at DATETIME NOT NULL,
	created_at    DATETIME NOT NULL DEFAULT (datetime('now')),
	last_failed_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_type ON dead_letter_queue(error_type);
CREATE INDEX IF NOT EXISTS idx_dlq_next_retry ON dead_letter_queue(next_retry_at);
`

// Ping implements Store.
func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Migrate implements Store.
func (s *SQLiteStore) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, sqliteMigration); err != nil {
		return eris.Wrap(err, "sqlite: migrate")
	}
	// v2: add error column (ignore duplicate-column error).
	_, _ = s.db.ExecContext(ctx, `ALTER TABLE runs ADD COLUMN error TEXT`)
	return nil
}

// Close implements Store.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// CreateRun implements Store.
func (s *SQLiteStore) CreateRun(ctx context.Context, company model.Company) (*model.Run, error) {
	id := uuid.New().String()
	now := time.Now().UTC()

	companyJSON, err := json.Marshal(company)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: marshal company")
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO runs (id, company, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		id, string(companyJSON), string(model.RunStatusQueued), now, now,
	)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: insert run")
	}

	return &model.Run{
		ID:        id,
		Company:   company,
		Status:    model.RunStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// UpdateRunStatus implements Store.
func (s *SQLiteStore) UpdateRunStatus(ctx context.Context, runID string, status model.RunStatus) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = ?, updated_at = ? WHERE id = ?`,
		string(status), time.Now().UTC(), runID,
	)
	if err != nil {
		return eris.Wrapf(err, "sqlite: update run status %s", runID)
	}
	return checkRowsAffected(res, "run", runID)
}

// UpdateRunResult implements Store.
func (s *SQLiteStore) UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return eris.Wrap(err, "sqlite: marshal result")
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE runs SET result = ?, status = ?, updated_at = ? WHERE id = ?`,
		string(resultJSON), string(model.RunStatusComplete), time.Now().UTC(), runID,
	)
	if err != nil {
		return eris.Wrapf(err, "sqlite: update run result %s", runID)
	}
	return checkRowsAffected(res, "run", runID)
}

// FailRun implements Store.
func (s *SQLiteStore) FailRun(ctx context.Context, runID string, runErr *model.RunError) error {
	errJSON, err := json.Marshal(runErr)
	if err != nil {
		return eris.Wrap(err, "sqlite: marshal run error")
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = ?, error = ?, updated_at = ? WHERE id = ?`,
		string(model.RunStatusFailed), string(errJSON), time.Now().UTC(), runID,
	)
	if err != nil {
		return eris.Wrapf(err, "sqlite: fail run %s", runID)
	}
	return checkRowsAffected(res, "run", runID)
}

// GetRun implements Store.
func (s *SQLiteStore) GetRun(ctx context.Context, runID string) (*model.Run, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, company, status, result, error, created_at, updated_at FROM runs WHERE id = ?`,
		runID,
	)
	return scanRun(row)
}

// ListRuns implements Store.
func (s *SQLiteStore) ListRuns(ctx context.Context, filter RunFilter) ([]model.Run, error) {
	query := `SELECT id, company, status, result, error, created_at, updated_at FROM runs WHERE 1=1`
	var args []any

	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, string(filter.Status))
	}
	if filter.CompanyURL != "" {
		query += ` AND json_extract(company, '$.url') = ?`
		args = append(args, filter.CompanyURL)
	}
	if filter.ErrorCategory != "" {
		query += ` AND json_extract(error, '$.category') = ?`
		args = append(args, string(filter.ErrorCategory))
	}
	if !filter.CreatedAfter.IsZero() {
		query += ` AND created_at >= ?`
		args = append(args, filter.CreatedAfter)
	}
	query += ` ORDER BY created_at DESC`

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += ` LIMIT ?`
	args = append(args, limit)

	if filter.Offset > 0 {
		query += ` OFFSET ?`
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: list runs")
	}
	defer rows.Close() //nolint:errcheck

	var runs []model.Run
	for rows.Next() {
		r, err := scanRunFromRows(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, *r)
	}
	return runs, eris.Wrap(rows.Err(), "sqlite: list runs iterate")
}

// CreatePhase implements Store.
func (s *SQLiteStore) CreatePhase(ctx context.Context, runID string, name string) (*model.RunPhase, error) {
	id := uuid.New().String()
	now := time.Now().UTC()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO run_phases (id, run_id, name, status, started_at) VALUES (?, ?, ?, ?, ?)`,
		id, runID, name, string(model.PhaseStatusRunning), now,
	)
	if err != nil {
		return nil, eris.Wrapf(err, "sqlite: insert phase for run %s", runID)
	}

	return &model.RunPhase{
		ID:        id,
		RunID:     runID,
		Name:      name,
		Status:    model.PhaseStatusRunning,
		StartedAt: now,
	}, nil
}

// CompletePhase implements Store.
func (s *SQLiteStore) CompletePhase(ctx context.Context, phaseID string, result *model.PhaseResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return eris.Wrap(err, "sqlite: marshal phase result")
	}

	res, err := s.db.ExecContext(ctx,
		`UPDATE run_phases SET status = ?, result = ? WHERE id = ?`,
		string(result.Status), string(resultJSON), phaseID,
	)
	if err != nil {
		return eris.Wrapf(err, "sqlite: complete phase %s", phaseID)
	}
	return checkRowsAffected(res, "phase", phaseID)
}

// GetCachedCrawl implements Store.
func (s *SQLiteStore) GetCachedCrawl(ctx context.Context, companyURL string) (*model.CrawlCache, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, company_url, pages, crawled_at, expires_at FROM crawl_cache
		 WHERE company_url = ? AND expires_at > datetime('now')
		 ORDER BY crawled_at DESC LIMIT 1`,
		companyURL,
	)

	var cc model.CrawlCache
	var pagesJSON string
	err := row.Scan(&cc.ID, &cc.CompanyURL, &pagesJSON, &cc.CrawledAt, &cc.ExpiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: get cached crawl")
	}
	if err := json.Unmarshal([]byte(pagesJSON), &cc.Pages); err != nil {
		return nil, eris.Wrap(err, "sqlite: unmarshal cached pages")
	}
	return &cc, nil
}

// SetCachedCrawl implements Store.
func (s *SQLiteStore) SetCachedCrawl(ctx context.Context, companyURL string, pages []model.CrawledPage, ttl time.Duration) error {
	id := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	pagesJSON, err := json.Marshal(pages)
	if err != nil {
		return eris.Wrap(err, "sqlite: marshal pages")
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO crawl_cache (id, company_url, pages, crawled_at, expires_at) VALUES (?, ?, ?, ?, ?)`,
		id, companyURL, string(pagesJSON), now, expiresAt,
	)
	return eris.Wrap(err, "sqlite: set cached crawl")
}

// GetCachedLinkedIn implements Store.
func (s *SQLiteStore) GetCachedLinkedIn(ctx context.Context, domain string) ([]byte, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT data FROM linkedin_cache
		 WHERE domain = ? AND expires_at > datetime('now')
		 ORDER BY cached_at DESC LIMIT 1`,
		domain,
	)
	var data string
	err := row.Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: get cached linkedin")
	}
	return []byte(data), nil
}

// SetCachedLinkedIn implements Store.
func (s *SQLiteStore) SetCachedLinkedIn(ctx context.Context, domain string, data []byte, ttl time.Duration) error {
	id := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO linkedin_cache (id, domain, data, cached_at, expires_at) VALUES (?, ?, ?, ?, ?)`,
		id, domain, string(data), now, expiresAt,
	)
	return eris.Wrap(err, "sqlite: set cached linkedin")
}

// DeleteExpiredCrawls implements Store.
func (s *SQLiteStore) DeleteExpiredCrawls(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM crawl_cache WHERE expires_at <= datetime('now')`,
	)
	if err != nil {
		return 0, eris.Wrap(err, "sqlite: delete expired crawls")
	}
	n, err := res.RowsAffected()
	return int(n), eris.Wrap(err, "sqlite: rows affected")
}

// DeleteExpiredLinkedIn implements Store.
func (s *SQLiteStore) DeleteExpiredLinkedIn(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM linkedin_cache WHERE expires_at <= datetime('now')`,
	)
	if err != nil {
		return 0, eris.Wrap(err, "sqlite: delete expired linkedin")
	}
	n, err := res.RowsAffected()
	return int(n), eris.Wrap(err, "sqlite: rows affected")
}

// DeleteExpiredScrapes implements Store.
func (s *SQLiteStore) DeleteExpiredScrapes(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM scrape_cache WHERE expires_at <= datetime('now')`,
	)
	if err != nil {
		return 0, eris.Wrap(err, "sqlite: delete expired scrapes")
	}
	n, err := res.RowsAffected()
	return int(n), eris.Wrap(err, "sqlite: rows affected")
}

// GetCachedScrape implements Store.
func (s *SQLiteStore) GetCachedScrape(ctx context.Context, urlHash string) ([]byte, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT content FROM scrape_cache
		 WHERE url_hash = ? AND expires_at > datetime('now')`,
		urlHash,
	)
	var content string
	err := row.Scan(&content)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: get cached scrape")
	}
	return []byte(content), nil
}

// SetCachedScrape implements Store.
func (s *SQLiteStore) SetCachedScrape(ctx context.Context, urlHash string, content []byte, ttl time.Duration) error {
	id := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO scrape_cache (id, url_hash, content, cached_at, expires_at) VALUES (?, ?, ?, ?, ?)`,
		id, urlHash, string(content), now, expiresAt,
	)
	return eris.Wrap(err, "sqlite: set cached scrape")
}

// GetHighConfidenceAnswers implements Store.
func (s *SQLiteStore) GetHighConfidenceAnswers(ctx context.Context, companyURL string, minConfidence float64) ([]model.ExtractionAnswer, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT result FROM runs
		 WHERE json_extract(company, '$.url') = ? AND status = 'complete' AND result IS NOT NULL
		 ORDER BY created_at DESC LIMIT 1`,
		companyURL,
	)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: get high confidence answers")
	}
	defer rows.Close() //nolint:errcheck

	if !rows.Next() {
		return nil, nil
	}

	var resultJSON string
	if err := rows.Scan(&resultJSON); err != nil {
		return nil, eris.Wrap(err, "sqlite: scan result")
	}

	var result model.RunResult
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, eris.Wrap(err, "sqlite: unmarshal result")
	}

	var highConf []model.ExtractionAnswer
	for _, a := range result.Answers {
		if a.Confidence >= minConfidence {
			highConf = append(highConf, a)
		}
	}
	return highConf, nil
}

// SaveCheckpoint implements Store.
func (s *SQLiteStore) SaveCheckpoint(ctx context.Context, companyID string, phase string, data []byte) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO checkpoints (company_id, phase, data, created_at) VALUES (?, ?, ?, ?)`,
		companyID, phase, string(data), now,
	)
	return eris.Wrap(err, "sqlite: save checkpoint")
}

// LoadCheckpoint implements Store.
func (s *SQLiteStore) LoadCheckpoint(ctx context.Context, companyID string) (*model.Checkpoint, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT company_id, phase, data, created_at FROM checkpoints WHERE company_id = ?`,
		companyID,
	)
	var cp model.Checkpoint
	var data string
	err := row.Scan(&cp.CompanyID, &cp.Phase, &data, &cp.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: load checkpoint")
	}
	cp.Data = []byte(data)
	return &cp, nil
}

// DeleteCheckpoint implements Store.
func (s *SQLiteStore) DeleteCheckpoint(ctx context.Context, companyID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM checkpoints WHERE company_id = ?`,
		companyID,
	)
	return eris.Wrap(err, "sqlite: delete checkpoint")
}

// helpers

func checkRowsAffected(res sql.Result, entity, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return eris.Wrap(err, "rows affected")
	}
	if n == 0 {
		return eris.Errorf("%s not found: %s", entity, id)
	}
	return nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanRun(row scannable) (*model.Run, error) {
	var r model.Run
	var companyJSON string
	var resultJSON sql.NullString
	var errorJSON sql.NullString

	err := row.Scan(&r.ID, &companyJSON, &r.Status, &resultJSON, &errorJSON, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, eris.New("run not found")
	}
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: scan run")
	}

	if err := json.Unmarshal([]byte(companyJSON), &r.Company); err != nil {
		return nil, eris.Wrap(err, "sqlite: unmarshal company")
	}
	if resultJSON.Valid {
		r.Result = &model.RunResult{}
		if err := json.Unmarshal([]byte(resultJSON.String), r.Result); err != nil {
			return nil, eris.Wrap(err, "sqlite: unmarshal result")
		}
	}
	if errorJSON.Valid {
		r.Error = &model.RunError{}
		if err := json.Unmarshal([]byte(errorJSON.String), r.Error); err != nil {
			return nil, eris.Wrap(err, "sqlite: unmarshal error")
		}
	}
	return &r, nil
}

func scanRunFromRows(rows *sql.Rows) (*model.Run, error) {
	return scanRun(rows)
}

// EnqueueDLQ implements Store.
func (s *SQLiteStore) EnqueueDLQ(ctx context.Context, entry resilience.DLQEntry) error {
	companyJSON, err := json.Marshal(entry.Company)
	if err != nil {
		return eris.Wrap(err, "sqlite: marshal dlq company")
	}

	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO dead_letter_queue
		 (id, company, error, error_type, failed_phase, retry_count, max_retries, next_retry_at, created_at, last_failed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID, string(companyJSON), entry.Error, entry.ErrorType,
		entry.FailedPhase, entry.RetryCount, entry.MaxRetries,
		entry.NextRetryAt.UTC(), entry.CreatedAt.UTC(), entry.LastFailedAt.UTC(),
	)
	return eris.Wrap(err, "sqlite: enqueue dlq")
}

// DequeueDLQ implements Store.
func (s *SQLiteStore) DequeueDLQ(ctx context.Context, filter resilience.DLQFilter) ([]resilience.DLQEntry, error) {
	now := time.Now().UTC()
	query := `SELECT id, company, error, error_type, failed_phase, retry_count, max_retries, next_retry_at, created_at, last_failed_at
	          FROM dead_letter_queue
	          WHERE next_retry_at <= ? AND retry_count < max_retries`
	args := []any{now}

	if filter.ErrorType != "" {
		query += ` AND error_type = ?`
		args = append(args, filter.ErrorType)
	}

	query += ` ORDER BY next_retry_at ASC`

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += ` LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: dequeue dlq")
	}
	defer rows.Close() //nolint:errcheck

	var entries []resilience.DLQEntry
	for rows.Next() {
		var e resilience.DLQEntry
		var companyJSON string
		var failedPhase sql.NullString
		if err := rows.Scan(&e.ID, &companyJSON, &e.Error, &e.ErrorType,
			&failedPhase, &e.RetryCount, &e.MaxRetries,
			&e.NextRetryAt, &e.CreatedAt, &e.LastFailedAt); err != nil {
			return nil, eris.Wrap(err, "sqlite: scan dlq entry")
		}
		if failedPhase.Valid {
			e.FailedPhase = failedPhase.String
		}
		if err := json.Unmarshal([]byte(companyJSON), &e.Company); err != nil {
			return nil, eris.Wrap(err, "sqlite: unmarshal dlq company")
		}
		entries = append(entries, e)
	}
	return entries, eris.Wrap(rows.Err(), "sqlite: dequeue dlq iterate")
}

// IncrementDLQRetry implements Store.
func (s *SQLiteStore) IncrementDLQRetry(ctx context.Context, id string, nextRetryAt time.Time, lastErr string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE dead_letter_queue
		 SET retry_count = retry_count + 1, next_retry_at = ?, error = ?, last_failed_at = ?
		 WHERE id = ?`,
		nextRetryAt.UTC(), lastErr, time.Now().UTC(), id,
	)
	if err != nil {
		return eris.Wrapf(err, "sqlite: increment dlq retry %s", id)
	}
	return checkRowsAffected(res, "dlq_entry", id)
}

// RemoveDLQ implements Store.
func (s *SQLiteStore) RemoveDLQ(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dead_letter_queue WHERE id = ?`, id)
	return eris.Wrap(err, "sqlite: remove dlq")
}

// CountDLQ implements Store.
func (s *SQLiteStore) CountDLQ(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letter_queue`).Scan(&count)
	return count, eris.Wrap(err, "sqlite: count dlq")
}
