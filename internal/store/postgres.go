package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/resilience"
)

// PostgresStore implements Store using pgxpool.
type PostgresStore struct {
	pool    db.Pool
	closeFn func()
}

// PoolConfig holds optional connection pool tuning parameters.
type PoolConfig struct {
	MaxConns int32 `yaml:"max_conns" mapstructure:"max_conns"`
	MinConns int32 `yaml:"min_conns" mapstructure:"min_conns"`
}

// preparedStatements lists queries to prepare on each new connection for
// faster execution of the most frequently used store operations.
var preparedStatements = map[string]string{
	"insert_run":         `INSERT INTO runs (id, company, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)`,
	"update_run_status":  `UPDATE runs SET status = $1, updated_at = $2 WHERE id = $3`,
	"update_run_result":  `UPDATE runs SET result = $1, status = $2, updated_at = $3 WHERE id = $4`,
	"get_run":            `SELECT id, company, status, result, created_at, updated_at FROM runs WHERE id = $1`,
	"insert_phase":       `INSERT INTO run_phases (id, run_id, name, status, started_at) VALUES ($1, $2, $3, $4, $5)`,
	"complete_phase":     `UPDATE run_phases SET status = $1, result = $2 WHERE id = $3`,
	"get_cached_crawl":   `SELECT id, company_url, pages, crawled_at, expires_at FROM crawl_cache WHERE company_url = $1 AND expires_at > now() ORDER BY crawled_at DESC LIMIT 1`,
	"set_cached_crawl":   `INSERT INTO crawl_cache (id, company_url, pages, crawled_at, expires_at) VALUES ($1, $2, $3, $4, $5)`,
	"get_cached_linkedin": `SELECT data FROM linkedin_cache WHERE domain = $1 AND expires_at > now() ORDER BY cached_at DESC LIMIT 1`,
	"set_cached_linkedin": `INSERT INTO linkedin_cache (id, domain, data, cached_at, expires_at) VALUES ($1, $2, $3, $4, $5)`,
	"delete_expired_crawls": `DELETE FROM crawl_cache WHERE expires_at <= now()`,
}

// NewPostgres creates a PostgresStore with a connection pool.
func NewPostgres(ctx context.Context, connString string, poolCfg *PoolConfig) (*PostgresStore, error) {
	pgxCfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: parse config")
	}

	// Apply pool sizing from config with sensible defaults.
	maxConns := int32(10)
	minConns := int32(2)
	if poolCfg != nil {
		if poolCfg.MaxConns > 0 {
			maxConns = poolCfg.MaxConns
		}
		if poolCfg.MinConns > 0 {
			minConns = poolCfg.MinConns
		}
	}
	pgxCfg.MaxConns = maxConns
	pgxCfg.MinConns = minConns
	pgxCfg.MaxConnLifetime = 30 * time.Minute
	pgxCfg.MaxConnIdleTime = 5 * time.Minute

	// Prepare frequently-used statements on each new connection.
	pgxCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		for name, sql := range preparedStatements {
			if _, err := conn.Prepare(ctx, name, sql); err != nil {
				return eris.Wrapf(err, "postgres: prepare %s", name)
			}
		}
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: create pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrap(err, "postgres: ping")
	}
	return &PostgresStore{pool: pool, closeFn: pool.Close}, nil
}

// Pool returns the underlying database pool for use by subsystems
// that need direct query access (e.g., revenue estimation).
func (s *PostgresStore) Pool() db.Pool {
	return s.pool
}

const postgresMigration = `
CREATE TABLE IF NOT EXISTS runs (
	id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
	company    JSONB NOT NULL,
	status     TEXT NOT NULL DEFAULT 'queued',
	result     JSONB,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS run_phases (
	id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
	run_id     TEXT NOT NULL REFERENCES runs(id),
	name       TEXT NOT NULL,
	status     TEXT NOT NULL DEFAULT 'running',
	result     JSONB,
	started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawl_cache (
	id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
	company_url TEXT NOT NULL UNIQUE,
	pages       JSONB NOT NULL,
	crawled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	expires_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_run_phases_run_id ON run_phases(run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_cache_company_url ON crawl_cache(company_url);
CREATE INDEX IF NOT EXISTS idx_crawl_cache_expires_at ON crawl_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_crawl_cache_url_expires ON crawl_cache(company_url, expires_at DESC);

CREATE TABLE IF NOT EXISTS linkedin_cache (
	id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
	domain     TEXT NOT NULL UNIQUE,
	data       JSONB NOT NULL,
	cached_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_linkedin_cache_domain ON linkedin_cache(domain);
CREATE INDEX IF NOT EXISTS idx_linkedin_cache_expires_at ON linkedin_cache(expires_at);

CREATE TABLE IF NOT EXISTS scrape_cache (
	id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
	url_hash   TEXT NOT NULL UNIQUE,
	content    JSONB NOT NULL,
	cached_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scrape_cache_url_hash ON scrape_cache(url_hash);
CREATE INDEX IF NOT EXISTS idx_scrape_cache_expires_at ON scrape_cache(expires_at);

CREATE TABLE IF NOT EXISTS checkpoints (
	company_id TEXT PRIMARY KEY,
	phase      TEXT NOT NULL,
	data       JSONB NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
	id             TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
	company        JSONB NOT NULL,
	error          TEXT NOT NULL,
	error_type     TEXT NOT NULL DEFAULT 'transient',
	failed_phase   TEXT,
	retry_count    INTEGER NOT NULL DEFAULT 0,
	max_retries    INTEGER NOT NULL DEFAULT 3,
	next_retry_at  TIMESTAMPTZ NOT NULL,
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	last_failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_type ON dead_letter_queue(error_type);
CREATE INDEX IF NOT EXISTS idx_dlq_next_retry ON dead_letter_queue(next_retry_at);
`

func (s *PostgresStore) Ping(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "SELECT 1")
	return eris.Wrap(err, "postgres: ping")
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, postgresMigration)
	return eris.Wrap(err, "postgres: migrate")
}

func (s *PostgresStore) Close() error {
	if s.closeFn != nil {
		s.closeFn()
	}
	return nil
}

func (s *PostgresStore) CreateRun(ctx context.Context, company model.Company) (*model.Run, error) {
	id := uuid.New().String()
	now := time.Now().UTC()

	companyJSON, err := json.Marshal(company)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: marshal company")
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO runs (id, company, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)`,
		id, companyJSON, string(model.RunStatusQueued), now, now,
	)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: insert run")
	}

	return &model.Run{
		ID:        id,
		Company:   company,
		Status:    model.RunStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func (s *PostgresStore) UpdateRunStatus(ctx context.Context, runID string, status model.RunStatus) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE runs SET status = $1, updated_at = $2 WHERE id = $3`,
		string(status), time.Now().UTC(), runID,
	)
	if err != nil {
		return eris.Wrapf(err, "postgres: update run status %s", runID)
	}
	if tag.RowsAffected() == 0 {
		return eris.Errorf("run not found: %s", runID)
	}
	return nil
}

func (s *PostgresStore) UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return eris.Wrap(err, "postgres: marshal result")
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE runs SET result = $1, status = $2, updated_at = $3 WHERE id = $4`,
		resultJSON, string(model.RunStatusComplete), time.Now().UTC(), runID,
	)
	if err != nil {
		return eris.Wrapf(err, "postgres: update run result %s", runID)
	}
	if tag.RowsAffected() == 0 {
		return eris.Errorf("run not found: %s", runID)
	}
	return nil
}

func (s *PostgresStore) GetRun(ctx context.Context, runID string) (*model.Run, error) {
	var r model.Run
	var companyJSON, resultJSON []byte
	var resultNull *[]byte

	err := s.pool.QueryRow(ctx,
		`SELECT id, company, status, result, created_at, updated_at FROM runs WHERE id = $1`,
		runID,
	).Scan(&r.ID, &companyJSON, &r.Status, &resultNull, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, eris.Wrapf(err, "postgres: get run %s", runID)
	}

	if err := json.Unmarshal(companyJSON, &r.Company); err != nil {
		return nil, eris.Wrap(err, "postgres: unmarshal company")
	}
	if resultNull != nil {
		resultJSON = *resultNull
		r.Result = &model.RunResult{}
		if err := json.Unmarshal(resultJSON, r.Result); err != nil {
			return nil, eris.Wrap(err, "postgres: unmarshal result")
		}
	}
	return &r, nil
}

func (s *PostgresStore) ListRuns(ctx context.Context, filter RunFilter) ([]model.Run, error) {
	query := `SELECT id, company, status, result, created_at, updated_at FROM runs WHERE true`
	args := []any{}
	argIdx := 1

	if filter.Status != "" {
		query += fmt.Sprintf(` AND status = $%d`, argIdx)
		args = append(args, string(filter.Status))
		argIdx++
	}
	if filter.CompanyURL != "" {
		query += fmt.Sprintf(` AND company->>'url' = $%d`, argIdx)
		args = append(args, filter.CompanyURL)
		argIdx++
	}
	query += ` ORDER BY created_at DESC`

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += fmt.Sprintf(` LIMIT $%d`, argIdx)
	args = append(args, limit)
	argIdx++

	if filter.Offset > 0 {
		query += fmt.Sprintf(` OFFSET $%d`, argIdx)
		args = append(args, filter.Offset)
		argIdx++
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: list runs")
	}
	defer rows.Close()

	var runs []model.Run
	for rows.Next() {
		var r model.Run
		var companyJSON, resultJSON []byte
		var resultNull *[]byte

		if err := rows.Scan(&r.ID, &companyJSON, &r.Status, &resultNull, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, eris.Wrap(err, "postgres: scan run")
		}
		if err := json.Unmarshal(companyJSON, &r.Company); err != nil {
			return nil, eris.Wrap(err, "postgres: unmarshal company")
		}
		if resultNull != nil {
			resultJSON = *resultNull
			r.Result = &model.RunResult{}
			if err := json.Unmarshal(resultJSON, r.Result); err != nil {
				return nil, eris.Wrap(err, "postgres: unmarshal result")
			}
		}
		runs = append(runs, r)
	}
	return runs, eris.Wrap(rows.Err(), "postgres: list runs iterate")
}

func (s *PostgresStore) CreatePhase(ctx context.Context, runID string, name string) (*model.RunPhase, error) {
	id := uuid.New().String()
	now := time.Now().UTC()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO run_phases (id, run_id, name, status, started_at) VALUES ($1, $2, $3, $4, $5)`,
		id, runID, name, string(model.PhaseStatusRunning), now,
	)
	if err != nil {
		return nil, eris.Wrapf(err, "postgres: insert phase for run %s", runID)
	}

	return &model.RunPhase{
		ID:        id,
		RunID:     runID,
		Name:      name,
		Status:    model.PhaseStatusRunning,
		StartedAt: now,
	}, nil
}

func (s *PostgresStore) CompletePhase(ctx context.Context, phaseID string, result *model.PhaseResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return eris.Wrap(err, "postgres: marshal phase result")
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE run_phases SET status = $1, result = $2 WHERE id = $3`,
		string(result.Status), resultJSON, phaseID,
	)
	if err != nil {
		return eris.Wrapf(err, "postgres: complete phase %s", phaseID)
	}
	if tag.RowsAffected() == 0 {
		return eris.Errorf("phase not found: %s", phaseID)
	}
	return nil
}

func (s *PostgresStore) GetCachedCrawl(ctx context.Context, companyURL string) (*model.CrawlCache, error) {
	var cc model.CrawlCache
	var pagesJSON []byte

	err := s.pool.QueryRow(ctx,
		`SELECT id, company_url, pages, crawled_at, expires_at FROM crawl_cache
		 WHERE company_url = $1 AND expires_at > now()
		 ORDER BY crawled_at DESC LIMIT 1`,
		companyURL,
	).Scan(&cc.ID, &cc.CompanyURL, &pagesJSON, &cc.CrawledAt, &cc.ExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "postgres: get cached crawl")
	}
	if err := json.Unmarshal(pagesJSON, &cc.Pages); err != nil {
		return nil, eris.Wrap(err, "postgres: unmarshal cached pages")
	}
	return &cc, nil
}

func (s *PostgresStore) SetCachedCrawl(ctx context.Context, companyURL string, pages []model.CrawledPage, ttl time.Duration) error {
	id := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	pagesJSON, err := json.Marshal(pages)
	if err != nil {
		return eris.Wrap(err, "postgres: marshal pages")
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO crawl_cache (id, company_url, pages, crawled_at, expires_at) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (company_url) DO UPDATE SET pages = $3, crawled_at = $4, expires_at = $5`,
		id, companyURL, pagesJSON, now, expiresAt,
	)
	return eris.Wrap(err, "postgres: set cached crawl")
}

func (s *PostgresStore) GetCachedLinkedIn(ctx context.Context, domain string) ([]byte, error) {
	var data []byte
	err := s.pool.QueryRow(ctx,
		`SELECT data FROM linkedin_cache
		 WHERE domain = $1 AND expires_at > now()
		 ORDER BY cached_at DESC LIMIT 1`,
		domain,
	).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "postgres: get cached linkedin")
	}
	return data, nil
}

func (s *PostgresStore) SetCachedLinkedIn(ctx context.Context, domain string, data []byte, ttl time.Duration) error {
	id := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	_, err := s.pool.Exec(ctx,
		`INSERT INTO linkedin_cache (id, domain, data, cached_at, expires_at) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (domain) DO UPDATE SET data = $3, cached_at = $4, expires_at = $5`,
		id, domain, data, now, expiresAt,
	)
	return eris.Wrap(err, "postgres: set cached linkedin")
}

func (s *PostgresStore) DeleteExpiredCrawls(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM crawl_cache WHERE expires_at <= now()`,
	)
	if err != nil {
		return 0, eris.Wrap(err, "postgres: delete expired crawls")
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) DeleteExpiredLinkedIn(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM linkedin_cache WHERE expires_at <= now()`,
	)
	if err != nil {
		return 0, eris.Wrap(err, "postgres: delete expired linkedin")
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) DeleteExpiredScrapes(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM scrape_cache WHERE expires_at <= now()`,
	)
	if err != nil {
		return 0, eris.Wrap(err, "postgres: delete expired scrapes")
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) GetCachedScrape(ctx context.Context, urlHash string) ([]byte, error) {
	var content []byte
	err := s.pool.QueryRow(ctx,
		`SELECT content FROM scrape_cache
		 WHERE url_hash = $1 AND expires_at > now()`,
		urlHash,
	).Scan(&content)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "postgres: get cached scrape")
	}
	return content, nil
}

func (s *PostgresStore) SetCachedScrape(ctx context.Context, urlHash string, content []byte, ttl time.Duration) error {
	id := uuid.New().String()
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	_, err := s.pool.Exec(ctx,
		`INSERT INTO scrape_cache (id, url_hash, content, cached_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (url_hash) DO UPDATE SET content = $3, cached_at = $4, expires_at = $5`,
		id, urlHash, content, now, expiresAt,
	)
	return eris.Wrap(err, "postgres: set cached scrape")
}

func (s *PostgresStore) GetHighConfidenceAnswers(ctx context.Context, companyURL string, minConfidence float64) ([]model.ExtractionAnswer, error) {
	var resultJSON []byte
	err := s.pool.QueryRow(ctx,
		`SELECT result FROM runs
		 WHERE company->>'url' = $1 AND status = 'complete' AND result IS NOT NULL
		 ORDER BY created_at DESC LIMIT 1`,
		companyURL,
	).Scan(&resultJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "postgres: get high confidence answers")
	}

	var result model.RunResult
	if err := json.Unmarshal(resultJSON, &result); err != nil {
		return nil, eris.Wrap(err, "postgres: unmarshal result")
	}

	var highConf []model.ExtractionAnswer
	for _, a := range result.Answers {
		if a.Confidence >= minConfidence {
			highConf = append(highConf, a)
		}
	}
	return highConf, nil
}

func (s *PostgresStore) SaveCheckpoint(ctx context.Context, companyID string, phase string, data []byte) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO checkpoints (company_id, phase, data, created_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (company_id) DO UPDATE SET phase = $2, data = $3, created_at = $4`,
		companyID, phase, data, now,
	)
	return eris.Wrap(err, "postgres: save checkpoint")
}

func (s *PostgresStore) LoadCheckpoint(ctx context.Context, companyID string) (*model.Checkpoint, error) {
	var cp model.Checkpoint
	err := s.pool.QueryRow(ctx,
		`SELECT company_id, phase, data, created_at FROM checkpoints WHERE company_id = $1`,
		companyID,
	).Scan(&cp.CompanyID, &cp.Phase, &cp.Data, &cp.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, eris.Wrap(err, "postgres: load checkpoint")
	}
	return &cp, nil
}

func (s *PostgresStore) DeleteCheckpoint(ctx context.Context, companyID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM checkpoints WHERE company_id = $1`,
		companyID,
	)
	return eris.Wrap(err, "postgres: delete checkpoint")
}

// Dead letter queue methods

func (s *PostgresStore) EnqueueDLQ(ctx context.Context, entry resilience.DLQEntry) error {
	companyJSON, err := json.Marshal(entry.Company)
	if err != nil {
		return eris.Wrap(err, "postgres: marshal dlq company")
	}

	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO dead_letter_queue
		 (id, company, error, error_type, failed_phase, retry_count, max_retries, next_retry_at, created_at, last_failed_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (id) DO UPDATE SET
		   error = $3, error_type = $4, failed_phase = $5, retry_count = $6,
		   next_retry_at = $8, last_failed_at = $10`,
		entry.ID, companyJSON, entry.Error, entry.ErrorType,
		entry.FailedPhase, entry.RetryCount, entry.MaxRetries,
		entry.NextRetryAt, entry.CreatedAt, entry.LastFailedAt,
	)
	return eris.Wrap(err, "postgres: enqueue dlq")
}

func (s *PostgresStore) DequeueDLQ(ctx context.Context, filter resilience.DLQFilter) ([]resilience.DLQEntry, error) {
	query := `SELECT id, company, error, error_type, failed_phase, retry_count, max_retries, next_retry_at, created_at, last_failed_at
	          FROM dead_letter_queue
	          WHERE next_retry_at <= now() AND retry_count < max_retries`
	args := []any{}
	argIdx := 1

	if filter.ErrorType != "" {
		query += fmt.Sprintf(` AND error_type = $%d`, argIdx)
		args = append(args, filter.ErrorType)
		argIdx++
	}

	query += ` ORDER BY next_retry_at ASC`

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query += fmt.Sprintf(` LIMIT $%d`, argIdx)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: dequeue dlq")
	}
	defer rows.Close()

	var entries []resilience.DLQEntry
	for rows.Next() {
		var e resilience.DLQEntry
		var companyJSON []byte
		var failedPhase *string
		if err := rows.Scan(&e.ID, &companyJSON, &e.Error, &e.ErrorType,
			&failedPhase, &e.RetryCount, &e.MaxRetries,
			&e.NextRetryAt, &e.CreatedAt, &e.LastFailedAt); err != nil {
			return nil, eris.Wrap(err, "postgres: scan dlq entry")
		}
		if failedPhase != nil {
			e.FailedPhase = *failedPhase
		}
		if err := json.Unmarshal(companyJSON, &e.Company); err != nil {
			return nil, eris.Wrap(err, "postgres: unmarshal dlq company")
		}
		entries = append(entries, e)
	}
	return entries, eris.Wrap(rows.Err(), "postgres: dequeue dlq iterate")
}

func (s *PostgresStore) IncrementDLQRetry(ctx context.Context, id string, nextRetryAt time.Time, lastErr string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE dead_letter_queue
		 SET retry_count = retry_count + 1, next_retry_at = $1, error = $2, last_failed_at = now()
		 WHERE id = $3`,
		nextRetryAt, lastErr, id,
	)
	if err != nil {
		return eris.Wrapf(err, "postgres: increment dlq retry %s", id)
	}
	if tag.RowsAffected() == 0 {
		return eris.Errorf("dlq_entry not found: %s", id)
	}
	return nil
}

func (s *PostgresStore) RemoveDLQ(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM dead_letter_queue WHERE id = $1`, id)
	return eris.Wrap(err, "postgres: remove dlq")
}

func (s *PostgresStore) CountDLQ(ctx context.Context) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dead_letter_queue`).Scan(&count)
	return count, eris.Wrap(err, "postgres: count dlq")
}
