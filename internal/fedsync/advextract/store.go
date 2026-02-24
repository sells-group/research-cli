package advextract

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// Store provides DB read/write operations for ADV extraction.
type Store struct {
	pool db.Pool
}

// NewStore creates a new extraction store.
func NewStore(pool db.Pool) *Store {
	return &Store{pool: pool}
}

// AdvisorRow represents an advisor from adv_firms + latest filing.
type AdvisorRow struct {
	CRDNumber int
	FirmName  string
	State     string
	City      string
	Website   string

	// Latest filing data
	AUMTotal            *int64
	AUMDiscretionary    *int64
	AUMNonDiscretionary *int64
	NumAccounts         *int
	TotalEmployees      *int
	FilingDate          *time.Time
	ClientTypes         json.RawMessage // JSONB from adv_filings

	// Structured Part 1 data (full filing row for bypass)
	Filing map[string]any
}

// BrochureRow represents an ADV Part 2 brochure.
type BrochureRow struct {
	CRDNumber   int
	BrochureID  string
	TextContent string
	FilingDate  string
}

// CRSRow represents an ADV Part 3 CRS document.
type CRSRow struct {
	CRDNumber   int
	CRSID       string
	TextContent string
	FilingDate  string
}

// OwnerRow represents a schedule A/B owner.
type OwnerRow struct {
	CRDNumber    int
	OwnerName    string
	OwnerType    string
	OwnershipPct *float64
	IsControl    bool
}

// FundRow represents a private fund from adv_private_funds.
type FundRow struct {
	CRDNumber       int
	FundID          string
	FundName        string
	FundType        string
	GrossAssetValue *int64
	NetAssetValue   *int64
}

// LoadAdvisor loads a single advisor with their latest filing data.
func (s *Store) LoadAdvisor(ctx context.Context, crd int) (*AdvisorRow, error) {
	query := `SELECT
		f.crd_number, f.firm_name, f.state, f.city, f.website,
		fi.aum_total, fi.aum_discretionary, fi.aum_non_discretionary,
		fi.num_accounts, fi.total_employees, fi.filing_date, fi.client_types
	FROM fed_data.adv_firms f
	LEFT JOIN LATERAL (
		SELECT * FROM fed_data.adv_filings fi2
		WHERE fi2.crd_number = f.crd_number
		ORDER BY fi2.filing_date DESC
		LIMIT 1
	) fi ON true
	WHERE f.crd_number = $1`

	row := &AdvisorRow{}
	err := s.pool.QueryRow(ctx, query, crd).Scan(
		&row.CRDNumber, &row.FirmName, &row.State, &row.City, &row.Website,
		&row.AUMTotal, &row.AUMDiscretionary, &row.AUMNonDiscretionary,
		&row.NumAccounts, &row.TotalEmployees, &row.FilingDate, &row.ClientTypes,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, eris.Errorf("advextract: advisor CRD %d not found", crd)
		}
		return nil, eris.Wrapf(err, "advextract: load advisor %d", crd)
	}

	// Load full filing row for structured bypass.
	filing, err := s.loadFilingMap(ctx, crd)
	if err == nil {
		row.Filing = filing
	}

	return row, nil
}

// loadFilingMap loads all columns from the latest filing as a map for structured bypass.
func (s *Store) loadFilingMap(ctx context.Context, crd int) (map[string]any, error) {
	query := `SELECT row_to_json(fi.*) FROM fed_data.adv_filings fi
		WHERE fi.crd_number = $1 ORDER BY fi.filing_date DESC LIMIT 1`

	var raw json.RawMessage
	err := s.pool.QueryRow(ctx, query, crd).Scan(&raw)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// LoadBrochures loads all brochures for an advisor.
func (s *Store) LoadBrochures(ctx context.Context, crd int) ([]BrochureRow, error) {
	query := `SELECT crd_number, brochure_id, text_content, filing_date
		FROM fed_data.adv_brochures
		WHERE crd_number = $1 AND text_content IS NOT NULL
		ORDER BY filing_date DESC`

	rows, err := s.pool.Query(ctx, query, crd)
	if err != nil {
		return nil, eris.Wrapf(err, "advextract: load brochures for CRD %d", crd)
	}
	defer rows.Close()

	var result []BrochureRow
	for rows.Next() {
		var b BrochureRow
		if err := rows.Scan(&b.CRDNumber, &b.BrochureID, &b.TextContent, &b.FilingDate); err != nil {
			return nil, eris.Wrap(err, "advextract: scan brochure")
		}
		result = append(result, b)
	}
	return result, rows.Err()
}

// LoadCRS loads all CRS documents for an advisor.
func (s *Store) LoadCRS(ctx context.Context, crd int) ([]CRSRow, error) {
	query := `SELECT crd_number, crs_id, text_content, filing_date
		FROM fed_data.adv_crs
		WHERE crd_number = $1 AND text_content IS NOT NULL
		ORDER BY filing_date DESC`

	rows, err := s.pool.Query(ctx, query, crd)
	if err != nil {
		return nil, eris.Wrapf(err, "advextract: load CRS for CRD %d", crd)
	}
	defer rows.Close()

	var result []CRSRow
	for rows.Next() {
		var c CRSRow
		if err := rows.Scan(&c.CRDNumber, &c.CRSID, &c.TextContent, &c.FilingDate); err != nil {
			return nil, eris.Wrap(err, "advextract: scan CRS")
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// LoadOwners loads schedule A/B owners for an advisor.
func (s *Store) LoadOwners(ctx context.Context, crd int) ([]OwnerRow, error) {
	query := `SELECT crd_number, owner_name, owner_type, ownership_pct, is_control
		FROM fed_data.adv_owners
		WHERE crd_number = $1
		ORDER BY COALESCE(ownership_pct, 0) DESC`

	rows, err := s.pool.Query(ctx, query, crd)
	if err != nil {
		return nil, eris.Wrapf(err, "advextract: load owners for CRD %d", crd)
	}
	defer rows.Close()

	var result []OwnerRow
	for rows.Next() {
		var o OwnerRow
		if err := rows.Scan(&o.CRDNumber, &o.OwnerName, &o.OwnerType, &o.OwnershipPct, &o.IsControl); err != nil {
			return nil, eris.Wrap(err, "advextract: scan owner")
		}
		result = append(result, o)
	}
	return result, rows.Err()
}

// LoadFunds loads all private funds for an advisor.
func (s *Store) LoadFunds(ctx context.Context, crd int) ([]FundRow, error) {
	query := `SELECT crd_number, fund_id, fund_name, fund_type, gross_asset_value, net_asset_value
		FROM fed_data.adv_private_funds
		WHERE crd_number = $1
		ORDER BY COALESCE(gross_asset_value, 0) DESC`

	rows, err := s.pool.Query(ctx, query, crd)
	if err != nil {
		return nil, eris.Wrapf(err, "advextract: load funds for CRD %d", crd)
	}
	defer rows.Close()

	var result []FundRow
	for rows.Next() {
		var f FundRow
		if err := rows.Scan(&f.CRDNumber, &f.FundID, &f.FundName, &f.FundType, &f.GrossAssetValue, &f.NetAssetValue); err != nil {
			return nil, eris.Wrap(err, "advextract: scan fund")
		}
		result = append(result, f)
	}
	return result, rows.Err()
}

// ListAdvisors returns CRD numbers matching the given filters.
func (s *Store) ListAdvisors(ctx context.Context, opts ListOpts) ([]int, error) {
	query := `SELECT DISTINCT f.crd_number FROM fed_data.adv_firms f`
	var conditions []string
	var args []any
	argIdx := 1

	if opts.MinAUM > 0 {
		query += ` LEFT JOIN LATERAL (
			SELECT aum_total FROM fed_data.adv_filings fi2
			WHERE fi2.crd_number = f.crd_number ORDER BY fi2.filing_date DESC LIMIT 1
		) fi ON true`
	}

	if opts.State != "" {
		conditions = append(conditions, "f.state = $"+itoa(argIdx))
		args = append(args, opts.State)
		argIdx++
	}
	if opts.MinAUM > 0 {
		conditions = append(conditions, "fi.aum_total >= $"+itoa(argIdx))
		args = append(args, opts.MinAUM)
		argIdx++
	}
	if !opts.IncludeExtracted {
		conditions = append(conditions, `NOT EXISTS (
			SELECT 1 FROM fed_data.adv_extraction_runs r
			WHERE r.crd_number = f.crd_number AND r.status = 'complete'
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

	query += " ORDER BY f.crd_number"
	if opts.Limit > 0 {
		query += " LIMIT $" + itoa(argIdx)
		args = append(args, opts.Limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "advextract: list advisors")
	}
	defer rows.Close()

	var crds []int
	for rows.Next() {
		var crd int
		if err := rows.Scan(&crd); err != nil {
			return nil, eris.Wrap(err, "advextract: scan CRD")
		}
		crds = append(crds, crd)
	}
	return crds, rows.Err()
}

// ListOpts controls advisor listing filters.
type ListOpts struct {
	Limit            int
	State            string
	MinAUM           int64
	IncludeExtracted bool // if false, skip already-extracted advisors
}

// CreateRun inserts a new extraction run and returns its ID.
func (s *Store) CreateRun(ctx context.Context, crd int, scope string, fundID string) (int64, error) {
	query := `INSERT INTO fed_data.adv_extraction_runs
		(crd_number, scope, fund_id, status, started_at)
		VALUES ($1, $2, NULLIF($3, ''), 'running', now())
		RETURNING id`

	var id int64
	err := s.pool.QueryRow(ctx, query, crd, scope, fundID).Scan(&id)
	if err != nil {
		return 0, eris.Wrap(err, "advextract: create run")
	}
	return id, nil
}

// CompleteRun marks a run as complete with final stats.
func (s *Store) CompleteRun(ctx context.Context, runID int64, stats RunStats) error {
	query := `UPDATE fed_data.adv_extraction_runs SET
		status = 'complete',
		tier_completed = $2,
		total_questions = $3,
		answered = $4,
		input_tokens = $5,
		output_tokens = $6,
		cost_usd = $7,
		completed_at = now()
		WHERE id = $1`

	_, err := s.pool.Exec(ctx, query, runID,
		stats.TierCompleted, stats.TotalQuestions, stats.Answered,
		stats.InputTokens, stats.OutputTokens, stats.CostUSD)
	return eris.Wrapf(err, "advextract: complete run %d", runID)
}

// FailRun marks a run as failed.
func (s *Store) FailRun(ctx context.Context, runID int64, errMsg string) error {
	query := `UPDATE fed_data.adv_extraction_runs SET
		status = 'failed', error_message = $2, completed_at = now()
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, query, runID, errMsg)
	return eris.Wrapf(err, "advextract: fail run %d", runID)
}

// RunStats holds final stats for a completed run.
type RunStats struct {
	TierCompleted  int
	TotalQuestions int
	Answered       int
	InputTokens    int
	OutputTokens   int
	CostUSD        float64
}

// WriteAdvisorAnswers bulk-upserts advisor-level answers.
func (s *Store) WriteAdvisorAnswers(ctx context.Context, answers []Answer) error {
	if len(answers) == 0 {
		return nil
	}

	cols := []string{
		"crd_number", "question_key", "value", "confidence", "tier",
		"reasoning", "source_doc", "source_section", "model",
		"input_tokens", "output_tokens", "run_id", "extracted_at",
	}
	conflictKeys := []string{"crd_number", "question_key"}

	rows := make([][]any, len(answers))
	for i, a := range answers {
		rows[i] = a.toRow()
	}

	_, err := db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "fed_data.adv_advisor_answers",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return eris.Wrap(err, "advextract: write advisor answers")
}

// WriteFundAnswers bulk-upserts fund-level answers.
func (s *Store) WriteFundAnswers(ctx context.Context, answers []Answer) error {
	if len(answers) == 0 {
		return nil
	}

	cols := []string{
		"crd_number", "fund_id", "question_key", "value", "confidence", "tier",
		"reasoning", "source_doc", "source_section", "model",
		"input_tokens", "output_tokens", "run_id", "extracted_at",
	}
	conflictKeys := []string{"crd_number", "fund_id", "question_key"}

	rows := make([][]any, len(answers))
	for i, a := range answers {
		rows[i] = a.toFundRow()
	}

	_, err := db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "fed_data.adv_fund_answers",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return eris.Wrap(err, "advextract: write fund answers")
}

// Answer represents a single extracted answer ready for DB storage.
type Answer struct {
	CRDNumber     int
	FundID        string // empty for advisor-level
	QuestionKey   string
	Value         any
	Confidence    float64
	Tier          int
	Reasoning     string
	SourceDoc     string
	SourceSection string
	Model         string
	InputTokens   int
	OutputTokens  int
	RunID         int64
}

func (a Answer) toRow() []any {
	return []any{
		a.CRDNumber, a.QuestionKey,
		jsonValue(a.Value), a.Confidence, a.Tier,
		a.Reasoning, a.SourceDoc, a.SourceSection, a.Model,
		a.InputTokens, a.OutputTokens, a.RunID, time.Now(),
	}
}

func (a Answer) toFundRow() []any {
	return []any{
		a.CRDNumber, a.FundID, a.QuestionKey,
		jsonValue(a.Value), a.Confidence, a.Tier,
		a.Reasoning, a.SourceDoc, a.SourceSection, a.Model,
		a.InputTokens, a.OutputTokens, a.RunID, time.Now(),
	}
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

// ArchiveExistingAnswers copies current answers to history before re-extraction.
func (s *Store) ArchiveExistingAnswers(ctx context.Context, crd int, runID int64) error {
	query := `INSERT INTO fed_data.adv_answer_history
		(crd_number, fund_id, question_key, value, confidence, tier, reasoning,
		 source_doc, source_section, model, run_id, superseded_by)
		SELECT crd_number, NULL, question_key, value, confidence, tier, reasoning,
		       source_doc, source_section, model, run_id, $2
		FROM fed_data.adv_advisor_answers
		WHERE crd_number = $1`
	_, err := s.pool.Exec(ctx, query, crd, runID)
	if err != nil {
		return eris.Wrapf(err, "advextract: archive advisor answers for CRD %d", crd)
	}

	// Also archive fund answers.
	fundQuery := `INSERT INTO fed_data.adv_answer_history
		(crd_number, fund_id, question_key, value, confidence, tier, reasoning,
		 source_doc, source_section, model, run_id, superseded_by)
		SELECT crd_number, fund_id, question_key, value, confidence, tier, reasoning,
		       source_doc, source_section, model, run_id, $2
		FROM fed_data.adv_fund_answers
		WHERE crd_number = $1`
	_, err = s.pool.Exec(ctx, fundQuery, crd, runID)
	return eris.Wrapf(err, "advextract: archive fund answers for CRD %d", crd)
}

// SectionIndexEntry represents a document section for indexing.
type SectionIndexEntry struct {
	CRDNumber     int
	DocType       string
	DocID         string
	SectionKey    string
	SectionTitle  string
	CharLength    int
	TokenEstimate int
}

// WriteSectionIndex records what document sections are available for an advisor.
func (s *Store) WriteSectionIndex(ctx context.Context, entries []SectionIndexEntry) error {
	if len(entries) == 0 {
		return nil
	}

	cols := []string{"crd_number", "doc_type", "doc_id", "section_key", "section_title", "char_length", "token_estimate"}
	conflictKeys := []string{"crd_number", "doc_type", "doc_id", "section_key"}

	rows := make([][]any, len(entries))
	for i, e := range entries {
		rows[i] = []any{e.CRDNumber, e.DocType, e.DocID, e.SectionKey, e.SectionTitle, e.CharLength, e.TokenEstimate}
	}

	_, err := db.BulkUpsert(ctx, s.pool, db.UpsertConfig{
		Table:        "fed_data.adv_document_sections",
		Columns:      cols,
		ConflictKeys: conflictKeys,
	}, rows)
	return eris.Wrap(err, "advextract: write section index")
}

// WriteComputedMetrics upserts computed metrics for an advisor.
func (s *Store) WriteComputedMetrics(ctx context.Context, m *ComputedMetrics) error {
	query := `INSERT INTO fed_data.adv_computed_metrics
		(crd_number, revenue_estimate, blended_fee_rate_bps, revenue_per_client,
		 aum_growth_cagr_pct, client_growth_rate_pct, employee_growth_rate_pct,
		 hnw_revenue_pct, institutional_revenue_pct, fund_aum_pct_total,
		 compensation_diversity, business_complexity, drp_severity, acquisition_readiness,
		 aum_1yr_growth_pct, aum_3yr_cagr_pct, aum_5yr_cagr_pct,
		 client_3yr_cagr_pct, employee_3yr_cagr_pct,
		 concentration_risk_score, key_person_dependency_score,
		 hybrid_revenue_estimate, estimated_expense_ratio, estimated_operating_margin,
		 revenue_per_employee, benchmark_aum_per_employee_pctile, benchmark_fee_rate_pctile,
		 amendments_last_year, amendments_per_year_avg, has_frequent_amendments,
		 regulatory_risk_score,
		 computed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
		        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27,
		        $28, $29, $30, $31, now())
		ON CONFLICT (crd_number) DO UPDATE SET
		 revenue_estimate = EXCLUDED.revenue_estimate,
		 blended_fee_rate_bps = EXCLUDED.blended_fee_rate_bps,
		 revenue_per_client = EXCLUDED.revenue_per_client,
		 aum_growth_cagr_pct = EXCLUDED.aum_growth_cagr_pct,
		 client_growth_rate_pct = EXCLUDED.client_growth_rate_pct,
		 employee_growth_rate_pct = EXCLUDED.employee_growth_rate_pct,
		 hnw_revenue_pct = EXCLUDED.hnw_revenue_pct,
		 institutional_revenue_pct = EXCLUDED.institutional_revenue_pct,
		 fund_aum_pct_total = EXCLUDED.fund_aum_pct_total,
		 compensation_diversity = EXCLUDED.compensation_diversity,
		 business_complexity = EXCLUDED.business_complexity,
		 drp_severity = EXCLUDED.drp_severity,
		 acquisition_readiness = EXCLUDED.acquisition_readiness,
		 aum_1yr_growth_pct = EXCLUDED.aum_1yr_growth_pct,
		 aum_3yr_cagr_pct = EXCLUDED.aum_3yr_cagr_pct,
		 aum_5yr_cagr_pct = EXCLUDED.aum_5yr_cagr_pct,
		 client_3yr_cagr_pct = EXCLUDED.client_3yr_cagr_pct,
		 employee_3yr_cagr_pct = EXCLUDED.employee_3yr_cagr_pct,
		 concentration_risk_score = EXCLUDED.concentration_risk_score,
		 key_person_dependency_score = EXCLUDED.key_person_dependency_score,
		 hybrid_revenue_estimate = EXCLUDED.hybrid_revenue_estimate,
		 estimated_expense_ratio = EXCLUDED.estimated_expense_ratio,
		 estimated_operating_margin = EXCLUDED.estimated_operating_margin,
		 revenue_per_employee = EXCLUDED.revenue_per_employee,
		 benchmark_aum_per_employee_pctile = EXCLUDED.benchmark_aum_per_employee_pctile,
		 benchmark_fee_rate_pctile = EXCLUDED.benchmark_fee_rate_pctile,
		 amendments_last_year = EXCLUDED.amendments_last_year,
		 amendments_per_year_avg = EXCLUDED.amendments_per_year_avg,
		 has_frequent_amendments = EXCLUDED.has_frequent_amendments,
		 regulatory_risk_score = EXCLUDED.regulatory_risk_score,
		 computed_at = now()`

	_, err := s.pool.Exec(ctx, query, m.CRDNumber,
		m.RevenueEstimate, m.BlendedFeeRateBPS, m.RevenuePerClient,
		m.AUMGrowthCAGR, m.ClientGrowthRate, m.EmployeeGrowthRate,
		m.HNWRevenuePct, m.InstitutionalRevenuePct, m.FundAUMPctTotal,
		m.CompensationDiversity, m.BusinessComplexity, m.DRPSeverity, m.AcquisitionReadiness,
		m.AUM1YrGrowth, m.AUM3YrCAGR, m.AUM5YrCAGR,
		m.Client3YrCAGR, m.Employee3YrCAGR,
		m.ConcentrationRiskScore, m.KeyPersonDependencyScore,
		m.HybridRevenueEstimate, m.EstimatedExpenseRatio, m.EstimatedOperatingMargin,
		m.RevenuePerEmployee, m.BenchmarkAUMPerEmployeePctile, m.BenchmarkFeeRatePctile,
		m.AmendmentsLastYear, m.AmendmentsPerYearAvg, m.HasFrequentAmendments,
		m.RegulatoryRiskScore)
	return eris.Wrapf(err, "advextract: write computed metrics for CRD %d", m.CRDNumber)
}

// RefreshMaterializedView refreshes the M&A intelligence materialized view.
func (s *Store) RefreshMaterializedView(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY fed_data.mv_adv_intelligence")
	return eris.Wrap(err, "advextract: refresh materialized view")
}

func itoa(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
