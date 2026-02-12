//go:build integration

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// PostgresStore implements Store using pgxpool.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgres creates a PostgresStore with a connection pool.
func NewPostgres(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, eris.Wrap(err, "postgres: create pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrap(err, "postgres: ping")
	}
	return &PostgresStore{pool: pool}, nil
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
	company_url TEXT NOT NULL,
	pages       JSONB NOT NULL,
	crawled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	expires_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_run_phases_run_id ON run_phases(run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_cache_company_url ON crawl_cache(company_url);
CREATE INDEX IF NOT EXISTS idx_crawl_cache_expires_at ON crawl_cache(expires_at);
`

func (s *PostgresStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, postgresMigration)
	return eris.Wrap(err, "postgres: migrate")
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
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
		if err.Error() == "no rows in result set" {
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
		`INSERT INTO crawl_cache (id, company_url, pages, crawled_at, expires_at) VALUES ($1, $2, $3, $4, $5)`,
		id, companyURL, pagesJSON, now, expiresAt,
	)
	return eris.Wrap(err, "postgres: set cached crawl")
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
