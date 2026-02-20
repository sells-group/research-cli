package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/rotisserie/eris"
	_ "modernc.org/sqlite"

	"github.com/sells-group/research-cli/internal/model"
)

// SQLiteStore implements Store using modernc.org/sqlite.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLite opens a SQLite database at the given path and configures WAL mode.
func NewSQLite(dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, eris.Wrap(err, "sqlite: open")
	}
	// WAL mode supports concurrent readers with a single writer.
	// Allow multiple connections for parallel Phase 1 fan-out (1A/1B/1C/1D).
	// busy_timeout handles writer contention.
	db.SetMaxOpenConns(4)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, eris.Wrapf(err, "sqlite: exec %s", pragma)
		}
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
`

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, sqliteMigration)
	return eris.Wrap(err, "sqlite: migrate")
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

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

func (s *SQLiteStore) GetRun(ctx context.Context, runID string) (*model.Run, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, company, status, result, created_at, updated_at FROM runs WHERE id = ?`,
		runID,
	)
	return scanRun(row)
}

func (s *SQLiteStore) ListRuns(ctx context.Context, filter RunFilter) ([]model.Run, error) {
	query := `SELECT id, company, status, result, created_at, updated_at FROM runs WHERE 1=1`
	var args []any

	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, string(filter.Status))
	}
	if filter.CompanyURL != "" {
		query += ` AND json_extract(company, '$.url') = ?`
		args = append(args, filter.CompanyURL)
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
	defer rows.Close()

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
	defer rows.Close()

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

func (s *SQLiteStore) SaveCheckpoint(ctx context.Context, companyID string, phase string, data []byte) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO checkpoints (company_id, phase, data, created_at) VALUES (?, ?, ?, ?)`,
		companyID, phase, string(data), now,
	)
	return eris.Wrap(err, "sqlite: save checkpoint")
}

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

	err := row.Scan(&r.ID, &companyJSON, &r.Status, &resultJSON, &r.CreatedAt, &r.UpdatedAt)
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
	return &r, nil
}

func scanRunFromRows(rows *sql.Rows) (*model.Run, error) {
	return scanRun(rows)
}
