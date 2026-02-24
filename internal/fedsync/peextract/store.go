package peextract

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// Store provides DB read/write operations for PE extraction.
type Store struct {
	pool db.Pool
}

// NewStore creates a new PE extraction store.
func NewStore(pool db.Pool) *Store {
	return &Store{pool: pool}
}

// PEFirmRow represents a PE firm from the pe_firms table.
type PEFirmRow struct {
	PEFirmID   int64
	FirmName   string
	FirmType   *string
	WebsiteURL *string
	HQCity     *string
	HQState    *string
}

// LoadFirm loads a single PE firm by ID.
func (s *Store) LoadFirm(ctx context.Context, firmID int64) (*PEFirmRow, error) {
	query := `SELECT pe_firm_id, firm_name, firm_type, website_url, hq_city, hq_state
		FROM fed_data.pe_firms WHERE pe_firm_id = $1`

	row := &PEFirmRow{}
	err := s.pool.QueryRow(ctx, query, firmID).Scan(
		&row.PEFirmID, &row.FirmName, &row.FirmType, &row.WebsiteURL,
		&row.HQCity, &row.HQState,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, eris.Errorf("peextract: PE firm %d not found", firmID)
		}
		return nil, eris.Wrapf(err, "peextract: load firm %d", firmID)
	}
	return row, nil
}

// LoadFirmByName loads a PE firm by exact name match.
func (s *Store) LoadFirmByName(ctx context.Context, name string) (*PEFirmRow, error) {
	query := `SELECT pe_firm_id, firm_name, firm_type, website_url, hq_city, hq_state
		FROM fed_data.pe_firms WHERE firm_name = $1`

	row := &PEFirmRow{}
	err := s.pool.QueryRow(ctx, query, name).Scan(
		&row.PEFirmID, &row.FirmName, &row.FirmType, &row.WebsiteURL,
		&row.HQCity, &row.HQState,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "peextract: load firm by name %q", name)
	}
	return row, nil
}

// UpsertFirm inserts or updates a PE firm, returning the firm ID.
func (s *Store) UpsertFirm(ctx context.Context, c PEFirmCandidate) (int64, error) {
	query := `INSERT INTO fed_data.pe_firms (
			firm_name, firm_type, website_url, website_source,
			linkedin_url, twitter_url, facebook_url, instagram_url, youtube_url, crunchbase_url,
			updated_at
		) VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''),
			NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''),
			now())
		ON CONFLICT (firm_name) DO UPDATE SET
			firm_type = COALESCE(EXCLUDED.firm_type, fed_data.pe_firms.firm_type),
			website_url = COALESCE(EXCLUDED.website_url, fed_data.pe_firms.website_url),
			website_source = COALESCE(EXCLUDED.website_source, fed_data.pe_firms.website_source),
			linkedin_url = COALESCE(EXCLUDED.linkedin_url, fed_data.pe_firms.linkedin_url),
			twitter_url = COALESCE(EXCLUDED.twitter_url, fed_data.pe_firms.twitter_url),
			facebook_url = COALESCE(EXCLUDED.facebook_url, fed_data.pe_firms.facebook_url),
			instagram_url = COALESCE(EXCLUDED.instagram_url, fed_data.pe_firms.instagram_url),
			youtube_url = COALESCE(EXCLUDED.youtube_url, fed_data.pe_firms.youtube_url),
			crunchbase_url = COALESCE(EXCLUDED.crunchbase_url, fed_data.pe_firms.crunchbase_url),
			updated_at = now()
		RETURNING pe_firm_id`

	var id int64
	err := s.pool.QueryRow(ctx, query,
		c.OwnerName, c.OwnerType, c.WebsiteURL, c.Source,
		c.LinkedInURL, c.TwitterURL, c.FacebookURL, c.InstagramURL, c.YouTubeURL, c.CrunchbaseURL,
	).Scan(&id)
	if err != nil {
		return 0, eris.Wrapf(err, "peextract: upsert firm %q", c.OwnerName)
	}
	return id, nil
}

// LinkFirmRIAs writes PE firm ↔ RIA ownership links.
func (s *Store) LinkFirmRIAs(ctx context.Context, firmID int64, c PEFirmCandidate) error {
	if len(c.OwnedCRDs) == 0 {
		return nil
	}

	cols := []string{"pe_firm_id", "crd_number", "ownership_pct", "is_control", "owner_type"}
	conflictKeys := []string{"pe_firm_id", "crd_number"}

	rows := make([][]any, len(c.OwnedCRDs))
	for i, crd := range c.OwnedCRDs {
		rows[i] = []any{firmID, crd, c.MaxOwnership, c.HasControl, c.OwnerType}
	}

	_, err := db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "fed_data.pe_firm_rias",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return eris.Wrapf(err, "peextract: link RIAs for firm %d", firmID)
}

// LoadOverrideURL checks pe_firm_overrides for a manual URL override.
func (s *Store) LoadOverrideURL(ctx context.Context, firmName string) (*string, error) {
	query := `SELECT o.website_url_override
		FROM fed_data.pe_firm_overrides o
		JOIN fed_data.pe_firms pf ON pf.pe_firm_id = o.pe_firm_id
		WHERE pf.firm_name = $1`

	var url string
	err := s.pool.QueryRow(ctx, query, firmName).Scan(&url)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, eris.Wrapf(err, "peextract: load override URL for %q", firmName)
	}
	return &url, nil
}

// CrawlCacheRow represents a cached crawl page.
type CrawlCacheRow struct {
	URL       string
	PageType  string
	Title     string
	Markdown  string
	CrawledAt time.Time
}

// LoadCrawlCache loads cached crawl pages for a firm.
func (s *Store) LoadCrawlCache(ctx context.Context, firmID int64) ([]CrawlCacheRow, error) {
	query := `SELECT url, page_type, title, markdown, crawled_at
		FROM fed_data.pe_crawl_cache
		WHERE pe_firm_id = $1
		ORDER BY page_type, url`

	rows, err := s.pool.Query(ctx, query, firmID)
	if err != nil {
		return nil, eris.Wrapf(err, "peextract: load crawl cache for firm %d", firmID)
	}
	defer rows.Close()

	var result []CrawlCacheRow
	for rows.Next() {
		var c CrawlCacheRow
		if err := rows.Scan(&c.URL, &c.PageType, &c.Title, &c.Markdown, &c.CrawledAt); err != nil {
			return nil, eris.Wrap(err, "peextract: scan crawl cache row")
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// IsCrawlCacheFresh returns true if the crawl cache for a firm is less than maxAge old.
func (s *Store) IsCrawlCacheFresh(ctx context.Context, firmID int64, maxAge time.Duration) (bool, error) {
	query := `SELECT count(*) FROM fed_data.pe_crawl_cache
		WHERE pe_firm_id = $1 AND crawled_at > $2`

	var count int
	err := s.pool.QueryRow(ctx, query, firmID, time.Now().Add(-maxAge)).Scan(&count)
	if err != nil {
		return false, eris.Wrapf(err, "peextract: check crawl cache freshness for firm %d", firmID)
	}
	return count > 0, nil
}

// WriteCrawlCache stores crawled pages for a firm.
func (s *Store) WriteCrawlCache(ctx context.Context, firmID int64, pages []ClassifiedPage) error {
	if len(pages) == 0 {
		return nil
	}

	cols := []string{"pe_firm_id", "url", "page_type", "title", "markdown", "status_code", "crawled_at"}
	conflictKeys := []string{"pe_firm_id", "url"}

	rows := make([][]any, len(pages))
	now := time.Now()
	for i, p := range pages {
		rows[i] = []any{firmID, p.URL, string(p.PageType), p.Title, p.Markdown, p.StatusCode, now}
	}

	_, err := db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "fed_data.pe_crawl_cache",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return eris.Wrapf(err, "peextract: write crawl cache for firm %d", firmID)
}

// SkipRun records a skipped extraction run (e.g., no pages available).
func (s *Store) SkipRun(ctx context.Context, firmID int64, reason string) error {
	query := `INSERT INTO fed_data.pe_extraction_runs
		(pe_firm_id, status, error_message, started_at, completed_at)
		VALUES ($1, 'skipped', $2, now(), now())`
	_, err := s.pool.Exec(ctx, query, firmID, reason)
	return eris.Wrapf(err, "peextract: skip run for firm %d", firmID)
}

// CreateRun inserts a new extraction run and returns its ID.
func (s *Store) CreateRun(ctx context.Context, firmID int64) (int64, error) {
	query := `INSERT INTO fed_data.pe_extraction_runs
		(pe_firm_id, status, started_at) VALUES ($1, 'running', now())
		RETURNING id`

	var id int64
	err := s.pool.QueryRow(ctx, query, firmID).Scan(&id)
	if err != nil {
		return 0, eris.Wrapf(err, "peextract: create run for firm %d", firmID)
	}
	return id, nil
}

// RunStats holds final stats for a completed run.
type RunStats struct {
	TierCompleted  int
	TotalQuestions int
	Answered       int
	PagesCrawled   int
	InputTokens    int
	OutputTokens   int
	CostUSD        float64
}

// CompleteRun marks a run as complete with final stats.
func (s *Store) CompleteRun(ctx context.Context, runID int64, stats RunStats) error {
	query := `UPDATE fed_data.pe_extraction_runs SET
		status = 'complete',
		tier_completed = $2,
		total_questions = $3,
		answered = $4,
		pages_crawled = $5,
		input_tokens = $6,
		output_tokens = $7,
		cost_usd = $8,
		completed_at = now()
		WHERE id = $1`

	_, err := s.pool.Exec(ctx, query, runID,
		stats.TierCompleted, stats.TotalQuestions, stats.Answered, stats.PagesCrawled,
		stats.InputTokens, stats.OutputTokens, stats.CostUSD)
	return eris.Wrapf(err, "peextract: complete run %d", runID)
}

// FailRun marks a run as failed.
func (s *Store) FailRun(ctx context.Context, runID int64, errMsg string) error {
	query := `UPDATE fed_data.pe_extraction_runs SET
		status = 'failed', error_message = $2, completed_at = now()
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, query, runID, errMsg)
	return eris.Wrapf(err, "peextract: fail run %d", runID)
}

// Answer represents a single extracted answer ready for DB storage.
type Answer struct {
	PEFirmID       int64
	QuestionKey    string
	Value          any
	Confidence     float64
	Tier           int
	Reasoning      string
	SourceURL      string
	SourcePageType string
	Model          string
	InputTokens    int
	OutputTokens   int
	RunID          int64
}

func (a Answer) toRow() []any {
	return []any{
		a.PEFirmID, a.QuestionKey,
		jsonValue(a.Value), a.Confidence, a.Tier,
		a.Reasoning, a.SourceURL, a.SourcePageType, a.Model,
		a.InputTokens, a.OutputTokens, a.RunID, time.Now(),
	}
}

// WriteAnswers bulk-upserts PE extraction answers.
func (s *Store) WriteAnswers(ctx context.Context, answers []Answer) error {
	if len(answers) == 0 {
		return nil
	}

	cols := []string{
		"pe_firm_id", "question_key", "value", "confidence", "tier",
		"reasoning", "source_url", "source_page_type", "model",
		"input_tokens", "output_tokens", "run_id", "extracted_at",
	}
	conflictKeys := []string{"pe_firm_id", "question_key"}

	rows := make([][]any, len(answers))
	for i, a := range answers {
		rows[i] = a.toRow()
	}

	_, err := db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "fed_data.pe_answers",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return eris.Wrap(err, "peextract: write answers")
}

// ListOpts controls PE firm listing filters.
type ListOpts struct {
	Limit            int
	MinRIAs          int
	IncludeExtracted bool
}

// ListPEFirms returns PE firm IDs matching the given filters.
func (s *Store) ListPEFirms(ctx context.Context, opts ListOpts) ([]int64, error) {
	query := `SELECT pf.pe_firm_id FROM fed_data.pe_firms pf`

	var conditions []string
	var args []any
	argIdx := 1

	if opts.MinRIAs > 0 {
		query += ` JOIN LATERAL (
			SELECT count(*) AS cnt FROM fed_data.pe_firm_rias pr
			WHERE pr.pe_firm_id = pf.pe_firm_id
		) rc ON true`
		conditions = append(conditions, "rc.cnt >= $"+itoa(argIdx))
		args = append(args, opts.MinRIAs)
		argIdx++
	}

	if !opts.IncludeExtracted {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM fed_data.pe_extraction_runs r
			WHERE r.pe_firm_id = pf.pe_firm_id AND r.status = 'complete'
		)`)
	}

	if len(conditions) > 0 {
		query += " WHERE "
		for i, c := range conditions {
			if i > 0 {
				query += " AND "
			}
			query += c
		}
	}

	query += " ORDER BY pf.pe_firm_id"
	if opts.Limit > 0 {
		query += " LIMIT $" + itoa(argIdx)
		args = append(args, opts.Limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "peextract: list PE firms")
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, eris.Wrap(err, "peextract: scan PE firm ID")
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RefreshMaterializedView refreshes the PE intelligence materialized view.
// Falls back to non-concurrent refresh if the view has never been populated.
func (s *Store) RefreshMaterializedView(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY fed_data.mv_pe_intelligence")
	if err != nil && strings.Contains(err.Error(), "CONCURRENTLY cannot be used when the materialized view is not populated") {
		_, err = s.pool.Exec(ctx, "REFRESH MATERIALIZED VIEW fed_data.mv_pe_intelligence")
	}
	return eris.Wrap(err, "peextract: refresh materialized view")
}

// jsonValue converts a value to JSONB-ready json.RawMessage.
func jsonValue(v any) json.RawMessage {
	if v == nil {
		return json.RawMessage("null")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return json.RawMessage(b)
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
