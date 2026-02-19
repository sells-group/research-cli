package ppp

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
)

// Config configures the PPP loan lookup client.
type Config struct {
	URL                 string  `mapstructure:"url"`
	SimilarityThreshold float64 `mapstructure:"similarity_threshold"`
	MaxCandidates       int     `mapstructure:"max_candidates"`
}

// LoanMatch represents a matched PPP loan record.
type LoanMatch struct {
	LoanNumber        int64     `json:"loan_number"`
	BorrowerName      string    `json:"borrower_name"`
	BorrowerAddress   string    `json:"borrower_address"`
	BorrowerCity      string    `json:"borrower_city"`
	BorrowerState     string    `json:"borrower_state"`
	BorrowerZip       string    `json:"borrower_zip"`
	CurrentApproval   float64   `json:"current_approval_amount"`
	ForgivenessAmount float64   `json:"forgiveness_amount"`
	JobsReported      int       `json:"jobs_reported"`
	DateApproved      time.Time `json:"date_approved"`
	LoanStatus        string    `json:"loan_status"`
	BusinessType      string    `json:"business_type"`
	NAICSCode         string    `json:"naics_code"`
	BusinessAge       string    `json:"business_age"`
	MatchTier         int       `json:"match_tier"`
	MatchScore        float64   `json:"match_score"`
}

// Querier abstracts database query operations for testing.
type Querier interface {
	FindLoans(ctx context.Context, name, state, city string) ([]LoanMatch, error)
	Close()
}

// pool defines the minimal database pool interface used by Client.
type pool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Ping(ctx context.Context) error
	Close()
}

// Client queries the PPP loan database.
type Client struct {
	pool pool
	cfg  Config
}

// Ensure Client implements Querier.
var _ Querier = (*Client)(nil)

// New creates a new PPP client connected to the loan database.
func New(ctx context.Context, cfg Config) (*Client, error) {
	pool, err := pgxpool.New(ctx, cfg.URL)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: connect")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrap(err, "ppp: ping")
	}
	return &Client{pool: pool, cfg: cfg}, nil
}

// NewFromPool creates a PPP client from an existing pgxpool.Pool.
// The client does NOT own the pool — Close() is a no-op.
// Use this when PPP data lives in fed_data.ppp_loans alongside other fedsync tables.
func NewFromPool(p *pgxpool.Pool, cfg Config) *Client {
	return &Client{pool: &sharedPool{Pool: p}, cfg: cfg}
}

// sharedPool wraps pgxpool.Pool with a no-op Close so we don't close a shared connection.
type sharedPool struct {
	*pgxpool.Pool
}

func (s *sharedPool) Close() {} // no-op: we don't own this pool

// Close releases the connection pool.
func (c *Client) Close() { c.pool.Close() }

// FindLoans tries 3 tiers in order, returns on first non-empty result.
func (c *Client) FindLoans(ctx context.Context, name, state, city string) ([]LoanMatch, error) {
	// Tier 1: Exact match on uppercased, trimmed name.
	upperName := strings.ToUpper(strings.TrimSpace(name))
	matches, err := c.queryTier1(ctx, upperName, state)
	if err != nil {
		return nil, err
	}
	if len(matches) > 0 {
		return matches, nil
	}

	// Tier 2: Normalized match (strip entity suffixes).
	normName := Normalize(name)
	matches, err = c.queryTier2(ctx, normName, state)
	if err != nil {
		return nil, err
	}
	if len(matches) > 0 {
		return matches, nil
	}

	// Tier 3: Trigram fuzzy match.
	matches, err = c.queryTier3(ctx, upperName, state, city)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

const tier1SQL = `
SELECT loannumber, borrowername, borroweraddress, borrowercity, borrowerstate, borrowerzip,
       currentapprovalamount, forgivenessamount, jobsreported, dateapproved, loanstatus,
       businesstype, naicscode, businessagedescription
FROM fed_data.ppp_loans
WHERE borrowerstate = $1 AND UPPER(TRIM(borrowername)) = $2
ORDER BY currentapprovalamount DESC`

func (c *Client) queryTier1(ctx context.Context, upperName, state string) ([]LoanMatch, error) {
	rows, err := c.pool.Query(ctx, tier1SQL, state, upperName)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: tier1 query")
	}
	defer rows.Close()

	return scanLoanMatches(rows, 1, 1.0)
}

const tier2SQL = `
SELECT loannumber, borrowername, borroweraddress, borrowercity, borrowerstate, borrowerzip,
       currentapprovalamount, forgivenessamount, jobsreported, dateapproved, loanstatus,
       businesstype, naicscode, businessagedescription
FROM fed_data.ppp_loans
WHERE borrowerstate = $1
  AND UPPER(REGEXP_REPLACE(TRIM(borrowername),
      '\s*,?\s*(LLC|L\.?L\.?C\.?|INC\.?|INCORPORATED|CORP\.?|CORPORATION|CO\.?|COMPANY|LTD\.?|LIMITED|L\.?P\.?|LLP|L\.?L\.?P\.?|PLLC|P\.?L\.?L\.?C\.?|P\.?C\.?|DBA|D/B/A)\s*\.?\s*$',
      '', 'i')) = $2
ORDER BY currentapprovalamount DESC`

func (c *Client) queryTier2(ctx context.Context, normName, state string) ([]LoanMatch, error) {
	rows, err := c.pool.Query(ctx, tier2SQL, state, normName)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: tier2 query")
	}
	defer rows.Close()

	return scanLoanMatches(rows, 2, 0.8)
}

const tier3SQL = `
SELECT loannumber, borrowername, borroweraddress, borrowercity, borrowerstate, borrowerzip,
       currentapprovalamount, forgivenessamount, jobsreported, dateapproved, loanstatus,
       businesstype, naicscode, businessagedescription,
       similarity(UPPER(borrowername), $2) AS sim_score
FROM fed_data.ppp_loans
WHERE borrowerstate = $1
  AND borrowername %% $2
  AND ($3::text IS NULL OR borrowercity ILIKE $3)
ORDER BY sim_score DESC
LIMIT $4`

func (c *Client) queryTier3(ctx context.Context, upperName, state, city string) ([]LoanMatch, error) {
	var cityParam *string
	if city != "" {
		cityParam = &city
	}

	rows, err := c.pool.Query(ctx, tier3SQL, state, upperName, cityParam, c.cfg.MaxCandidates)
	if err != nil {
		return nil, eris.Wrap(err, "ppp: tier3 query")
	}
	defer rows.Close()

	return scanLoanMatchesWithScore(rows, 3)
}

// scanLoanMatches scans rows into LoanMatch structs with a fixed tier and score.
func scanLoanMatches(rows pgx.Rows, tier int, score float64) ([]LoanMatch, error) {
	var matches []LoanMatch
	for rows.Next() {
		m, err := scanLoanMatch(rows)
		if err != nil {
			return nil, err
		}
		m.MatchTier = tier
		m.MatchScore = score
		matches = append(matches, m)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "ppp: rows iteration")
	}
	return matches, nil
}

// scanLoanMatchesWithScore scans rows that include a sim_score column.
func scanLoanMatchesWithScore(rows pgx.Rows, tier int) ([]LoanMatch, error) {
	var matches []LoanMatch
	for rows.Next() {
		var m LoanMatch
		var simScore float64
		err := rows.Scan(
			&m.LoanNumber, &m.BorrowerName, &m.BorrowerAddress, &m.BorrowerCity,
			&m.BorrowerState, &m.BorrowerZip, &m.CurrentApproval, &m.ForgivenessAmount,
			&m.JobsReported, &m.DateApproved, &m.LoanStatus, &m.BusinessType,
			&m.NAICSCode, &m.BusinessAge, &simScore,
		)
		if err != nil {
			return nil, eris.Wrap(err, "ppp: scan tier3 row")
		}
		m.MatchTier = tier
		m.MatchScore = simScore
		matches = append(matches, m)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "ppp: rows iteration")
	}
	return matches, nil
}

// scanLoanMatch scans a single row (without sim_score) into a LoanMatch.
func scanLoanMatch(rows pgx.Rows) (LoanMatch, error) {
	var m LoanMatch
	err := rows.Scan(
		&m.LoanNumber, &m.BorrowerName, &m.BorrowerAddress, &m.BorrowerCity,
		&m.BorrowerState, &m.BorrowerZip, &m.CurrentApproval, &m.ForgivenessAmount,
		&m.JobsReported, &m.DateApproved, &m.LoanStatus, &m.BusinessType,
		&m.NAICSCode, &m.BusinessAge,
	)
	if err != nil {
		return m, eris.Wrap(err, "ppp: scan row")
	}
	return m, nil
}
