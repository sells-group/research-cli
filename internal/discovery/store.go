package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// Store defines persistence operations for the discovery subsystem.
type Store interface {
	CreateRun(ctx context.Context, cfg RunConfig) (string, error)
	CompleteRun(ctx context.Context, runID string, result *RunResult) error
	FailRun(ctx context.Context, runID string, errMsg string) error
	BulkInsertCandidates(ctx context.Context, candidates []Candidate) (int64, error)
	UpdateCandidateScore(ctx context.Context, id int64, tier string, score float64) error
	DisqualifyCandidate(ctx context.Context, id int64, reason string) error
	ListCandidates(ctx context.Context, runID string, opts ListOpts) ([]Candidate, error)
	MarkPromoted(ctx context.Context, ids []int64) error
	FindNewPPPBorrowers(ctx context.Context, naics []string, states []string, minApproval float64, limit int) ([]PPPBorrower, error)
	PlaceIDExists(ctx context.Context, placeID string) (bool, error)
	DomainExists(ctx context.Context, domain string) (bool, error)
	GetUnsearchedCells(ctx context.Context, cbsaCode string, limit int) ([]GridCell, error)
	UpdateCellSearched(ctx context.Context, cellID int64, resultCount int) error
}

// PostgresStore implements Store using pgx.
type PostgresStore struct {
	pool db.Pool
}

// NewPostgresStore creates a new PostgresStore.
func NewPostgresStore(pool db.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// CreateRun inserts a new discovery run and returns its UUID.
func (s *PostgresStore) CreateRun(ctx context.Context, cfg RunConfig) (string, error) {
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return "", eris.Wrap(err, "discovery: marshal run config")
	}

	var id string
	err = s.pool.QueryRow(ctx,
		`INSERT INTO discovery_runs (strategy, config) VALUES ($1, $2) RETURNING id`,
		cfg.Strategy, cfgJSON,
	).Scan(&id)
	if err != nil {
		return "", eris.Wrap(err, "discovery: create run")
	}
	return id, nil
}

// CompleteRun marks a discovery run as completed with result statistics.
func (s *PostgresStore) CompleteRun(ctx context.Context, runID string, result *RunResult) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE discovery_runs SET
			status = 'completed',
			candidates_found = $2,
			candidates_qualified = $3,
			cost_usd = $4,
			completed_at = now()
		WHERE id = $1`,
		runID, result.CandidatesFound, result.CandidatesQualified, result.CostUSD,
	)
	if err != nil {
		return eris.Wrapf(err, "discovery: complete run %s", runID)
	}
	return nil
}

// FailRun marks a discovery run as failed with an error message.
func (s *PostgresStore) FailRun(ctx context.Context, runID string, errMsg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE discovery_runs SET status = 'failed', error = $2, completed_at = now() WHERE id = $1`,
		runID, errMsg,
	)
	if err != nil {
		return eris.Wrapf(err, "discovery: fail run %s", runID)
	}
	return nil
}

// BulkInsertCandidates inserts candidates using BulkUpsert with ON CONFLICT DO NOTHING.
func (s *PostgresStore) BulkInsertCandidates(ctx context.Context, candidates []Candidate) (int64, error) {
	if len(candidates) == 0 {
		return 0, nil
	}

	rows := make([][]any, len(candidates))
	for i, c := range candidates {
		rows[i] = []any{
			c.RunID, c.GooglePlaceID, c.Name, c.Domain, c.Website,
			c.Street, c.City, c.State, c.ZipCode, c.NAICSCode,
			c.Source, c.SourceRecord,
		}
	}

	cfg := db.UpsertConfig{
		Table: "discovery_candidates",
		Columns: []string{
			"run_id", "google_place_id", "name", "domain", "website",
			"street", "city", "state", "zip_code", "naics_code",
			"source", "source_record",
		},
		ConflictKeys: []string{"run_id", "google_place_id"},
		// Empty UpdateCols = DO NOTHING behavior by not updating anything.
		// We use a direct INSERT approach since BulkUpsert always does DO UPDATE.
	}

	return db.BulkUpsert(ctx, s.pool, cfg, rows)
}

// UpdateCandidateScore updates the score for a specific tier.
func (s *PostgresStore) UpdateCandidateScore(ctx context.Context, id int64, tier string, score float64) error {
	col, err := tierColumn(tier)
	if err != nil {
		return err
	}

	_, err = s.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE discovery_candidates SET %s = $2 WHERE id = $1`, col),
		id, score,
	)
	if err != nil {
		return eris.Wrapf(err, "discovery: update score %s for candidate %d", tier, id)
	}
	return nil
}

// DisqualifyCandidate marks a candidate as disqualified with the given reason.
func (s *PostgresStore) DisqualifyCandidate(ctx context.Context, id int64, reason string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE discovery_candidates SET disqualified = true, disqualify_reason = $2 WHERE id = $1`,
		id, reason,
	)
	if err != nil {
		return eris.Wrapf(err, "discovery: disqualify candidate %d", id)
	}
	return nil
}

// ListCandidates returns candidates for a run with optional filtering.
func (s *PostgresStore) ListCandidates(ctx context.Context, runID string, opts ListOpts) ([]Candidate, error) {
	var conditions []string
	var args []any
	argIdx := 1

	conditions = append(conditions, fmt.Sprintf("run_id = $%d", argIdx))
	args = append(args, runID)
	argIdx++

	if opts.Disqualified != nil {
		conditions = append(conditions, fmt.Sprintf("disqualified = $%d", argIdx))
		args = append(args, *opts.Disqualified)
		argIdx++
	}

	if opts.MinScore != nil {
		conditions = append(conditions, fmt.Sprintf("COALESCE(score_t2, score_t1, score_t0, 0) >= $%d", argIdx))
		args = append(args, *opts.MinScore)
		argIdx++
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	query := fmt.Sprintf(
		`SELECT %s FROM discovery_candidates WHERE %s ORDER BY id LIMIT $%d OFFSET $%d`,
		candidateColumns,
		strings.Join(conditions, " AND "),
		argIdx,
		argIdx+1,
	)
	args = append(args, limit, opts.Offset)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "discovery: list candidates")
	}
	defer rows.Close()

	return scanCandidates(rows)
}

// MarkPromoted sets promoted_at to now for the given candidate IDs.
func (s *PostgresStore) MarkPromoted(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	// Build placeholder list: $1, $2, ...
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(
		`UPDATE discovery_candidates SET promoted_at = now() WHERE id IN (%s)`,
		strings.Join(placeholders, ", "),
	)

	_, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return eris.Wrap(err, "discovery: mark promoted")
	}
	return nil
}

// FindNewPPPBorrowers finds PPP borrowers that are not yet in the companies table.
func (s *PostgresStore) FindNewPPPBorrowers(ctx context.Context, naics []string, states []string, minApproval float64, limit int) ([]PPPBorrower, error) {
	if limit <= 0 {
		limit = 1000
	}

	var conditions []string
	var args []any
	argIdx := 1

	conditions = append(conditions, fmt.Sprintf("p.current_approval_amount >= $%d", argIdx))
	args = append(args, minApproval)
	argIdx++

	if len(naics) > 0 {
		conditions = append(conditions, fmt.Sprintf("p.naics_code = ANY($%d)", argIdx))
		args = append(args, naics)
		argIdx++
	}

	if len(states) > 0 {
		conditions = append(conditions, fmt.Sprintf("p.state = ANY($%d)", argIdx))
		args = append(args, states)
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT p.borrower_name, p.street, p.city, p.state, p.zip, p.naics_code,
			p.current_approval_amount, p.loan_number
		FROM fed_data.ppp_loans p
		LEFT JOIN companies c ON LOWER(c.name) = LOWER(p.borrower_name)
		WHERE c.id IS NULL AND %s
		ORDER BY p.current_approval_amount DESC
		LIMIT $%d`,
		strings.Join(conditions, " AND "),
		argIdx,
	)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "discovery: find new PPP borrowers")
	}
	defer rows.Close()

	var borrowers []PPPBorrower
	for rows.Next() {
		var b PPPBorrower
		if err := rows.Scan(&b.BorrowerName, &b.Street, &b.City, &b.State, &b.Zip,
			&b.NAICSCode, &b.Approval, &b.LoanNumber); err != nil {
			return nil, eris.Wrap(err, "discovery: scan PPP borrower")
		}
		borrowers = append(borrowers, b)
	}
	return borrowers, rows.Err()
}

// PlaceIDExists checks if a Google Place ID already exists in discovery_candidates.
func (s *PostgresStore) PlaceIDExists(ctx context.Context, placeID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM discovery_candidates WHERE google_place_id = $1)`,
		placeID,
	).Scan(&exists)
	if err != nil {
		return false, eris.Wrap(err, "discovery: check place ID exists")
	}
	return exists, nil
}

// DomainExists checks if a domain already exists in the companies table.
func (s *PostgresStore) DomainExists(ctx context.Context, domain string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM companies WHERE domain = $1)`,
		domain,
	).Scan(&exists)
	if err != nil {
		return false, eris.Wrap(err, "discovery: check domain exists")
	}
	return exists, nil
}

// GetUnsearchedCells returns grid cells that have not been searched yet.
func (s *PostgresStore) GetUnsearchedCells(ctx context.Context, cbsaCode string, limit int) ([]GridCell, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, cbsa_code, cell_km, sw_lat, sw_lon, ne_lat, ne_lon
		FROM msa_grid_cells
		WHERE cbsa_code = $1 AND searched_at IS NULL
		ORDER BY id
		LIMIT $2`, cbsaCode, limit)
	if err != nil {
		return nil, eris.Wrap(err, "discovery: get unsearched cells")
	}
	defer rows.Close()

	var cells []GridCell
	for rows.Next() {
		var c GridCell
		if err := rows.Scan(&c.ID, &c.CBSACode, &c.CellKM, &c.SWLat, &c.SWLon, &c.NELat, &c.NELon); err != nil {
			return nil, eris.Wrap(err, "discovery: scan grid cell")
		}
		cells = append(cells, c)
	}
	return cells, rows.Err()
}

// UpdateCellSearched marks a grid cell as searched with the result count.
func (s *PostgresStore) UpdateCellSearched(ctx context.Context, cellID int64, resultCount int) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE msa_grid_cells SET searched_at = $2, result_count = $3 WHERE id = $1`,
		cellID, time.Now(), resultCount,
	)
	if err != nil {
		return eris.Wrapf(err, "discovery: update cell searched %d", cellID)
	}
	return nil
}

// tierColumn maps a tier name to the corresponding score column.
func tierColumn(tier string) (string, error) {
	switch tier {
	case "t0":
		return "score_t0", nil
	case "t1":
		return "score_t1", nil
	case "t2":
		return "score_t2", nil
	default:
		return "", eris.Errorf("discovery: unknown tier %q", tier)
	}
}

const candidateColumns = `id, run_id, google_place_id, name, domain, website,
	street, city, state, zip_code, naics_code,
	source, source_record, disqualified, disqualify_reason,
	score_t0, score_t1, score_t2, promoted_at, created_at`

func scanCandidates(rows pgx.Rows) ([]Candidate, error) {
	var candidates []Candidate
	for rows.Next() {
		var c Candidate
		if err := rows.Scan(
			&c.ID, &c.RunID, &c.GooglePlaceID, &c.Name, &c.Domain, &c.Website,
			&c.Street, &c.City, &c.State, &c.ZipCode, &c.NAICSCode,
			&c.Source, &c.SourceRecord, &c.Disqualified, &c.DisqualifyReason,
			&c.ScoreT0, &c.ScoreT1, &c.ScoreT2, &c.PromotedAt, &c.CreatedAt,
		); err != nil {
			return nil, eris.Wrap(err, "discovery: scan candidate")
		}
		candidates = append(candidates, c)
	}
	return candidates, rows.Err()
}
