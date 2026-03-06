package advextract

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStore(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := NewStore(mock)
	require.NotNil(t, s)
}

// ---------------------------------------------------------------------------
// LoadAdvisor
// ---------------------------------------------------------------------------

func TestLoadAdvisor_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()
	aum := int64(1_000_000)
	aumD := int64(800_000)
	aumND := int64(200_000)
	numAcct := 50
	numEmp := 10

	// Main advisor query.
	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{
			"crd_number", "firm_name", "state", "city", "website",
			"aum_total", "aum_discretionary", "aum_non_discretionary",
			"num_accounts", "total_employees", "filing_date", "client_types",
		}).AddRow(
			123, "Test Advisors", "CA", "Los Angeles", "https://test.com",
			&aum, &aumD, &aumND,
			&numAcct, &numEmp, &now, json.RawMessage(`{"individual":true}`),
		),
	)

	// loadFilingMap query.
	mock.ExpectQuery("SELECT row_to_json").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"row_to_json"}).AddRow(
			json.RawMessage(`{"crd_number":123,"firm_name":"Test Advisors"}`),
		),
	)

	s := NewStore(mock)
	advisor, err := s.LoadAdvisor(context.Background(), 123)
	require.NoError(t, err)
	require.NotNil(t, advisor)

	assert.Equal(t, 123, advisor.CRDNumber)
	assert.Equal(t, "Test Advisors", advisor.FirmName)
	assert.Equal(t, "CA", advisor.State)
	assert.Equal(t, "Los Angeles", advisor.City)
	assert.Equal(t, "https://test.com", advisor.Website)
	assert.Equal(t, int64(1_000_000), *advisor.AUMTotal)
	assert.Equal(t, int64(800_000), *advisor.AUMDiscretionary)
	assert.Equal(t, int64(200_000), *advisor.AUMNonDiscretionary)
	assert.Equal(t, 50, *advisor.NumAccounts)
	assert.Equal(t, 10, *advisor.TotalEmployees)
	assert.NotNil(t, advisor.Filing)
	assert.Equal(t, float64(123), advisor.Filing["crd_number"])

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadAdvisor_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(999).WillReturnError(pgx.ErrNoRows)

	s := NewStore(mock)
	advisor, err := s.LoadAdvisor(context.Background(), 999)
	require.Error(t, err)
	assert.Nil(t, advisor)
	assert.Contains(t, err.Error(), "not found")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadAdvisor_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnError(fmt.Errorf("connection refused"))

	s := NewStore(mock)
	advisor, err := s.LoadAdvisor(context.Background(), 123)
	require.Error(t, err)
	assert.Nil(t, advisor)
	assert.Contains(t, err.Error(), "load advisor")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// LoadBrochures
// ---------------------------------------------------------------------------

func TestLoadBrochures_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "brochure_id", "text_content", "filing_date"}).
			AddRow(123, "B001", "Brochure text 1", "2025-01-01").
			AddRow(123, "B002", "Brochure text 2", "2024-06-01"),
	)

	s := NewStore(mock)
	brochures, err := s.LoadBrochures(context.Background(), 123)
	require.NoError(t, err)
	require.Len(t, brochures, 2)
	assert.Equal(t, "B001", brochures[0].BrochureID)
	assert.Equal(t, "Brochure text 1", brochures[0].TextContent)
	assert.Equal(t, "B002", brochures[1].BrochureID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadBrochures_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "brochure_id", "text_content", "filing_date"}),
	)

	s := NewStore(mock)
	brochures, err := s.LoadBrochures(context.Background(), 123)
	require.NoError(t, err)
	assert.Empty(t, brochures)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadBrochures_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnError(fmt.Errorf("timeout"))

	s := NewStore(mock)
	brochures, err := s.LoadBrochures(context.Background(), 123)
	require.Error(t, err)
	assert.Nil(t, brochures)
	assert.Contains(t, err.Error(), "load brochures")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// LoadCRS
// ---------------------------------------------------------------------------

func TestLoadCRS_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(456).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "crs_id", "text_content", "filing_date"}).
			AddRow(456, "C001", "CRS content", "2025-03-01"),
	)

	s := NewStore(mock)
	docs, err := s.LoadCRS(context.Background(), 456)
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, "C001", docs[0].CRSID)
	assert.Equal(t, "CRS content", docs[0].TextContent)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadCRS_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(456).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "crs_id", "text_content", "filing_date"}),
	)

	s := NewStore(mock)
	docs, err := s.LoadCRS(context.Background(), 456)
	require.NoError(t, err)
	assert.Empty(t, docs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadCRS_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(456).WillReturnError(fmt.Errorf("broken pipe"))

	s := NewStore(mock)
	docs, err := s.LoadCRS(context.Background(), 456)
	require.Error(t, err)
	assert.Nil(t, docs)
	assert.Contains(t, err.Error(), "load CRS")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// LoadOwners
// ---------------------------------------------------------------------------

func TestLoadOwners_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	pct1 := 75.0
	pct2 := 25.0
	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "owner_name", "owner_type", "ownership_pct", "is_control"}).
			AddRow(123, "Alice Smith", "individual", &pct1, true).
			AddRow(123, "Bob Jones", "individual", &pct2, false),
	)

	s := NewStore(mock)
	owners, err := s.LoadOwners(context.Background(), 123)
	require.NoError(t, err)
	require.Len(t, owners, 2)
	assert.Equal(t, "Alice Smith", owners[0].OwnerName)
	assert.Equal(t, 75.0, *owners[0].OwnershipPct)
	assert.True(t, owners[0].IsControl)
	assert.Equal(t, "Bob Jones", owners[1].OwnerName)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadOwners_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "owner_name", "owner_type", "ownership_pct", "is_control"}),
	)

	s := NewStore(mock)
	owners, err := s.LoadOwners(context.Background(), 123)
	require.NoError(t, err)
	assert.Empty(t, owners)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadOwners_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnError(fmt.Errorf("timeout"))

	s := NewStore(mock)
	owners, err := s.LoadOwners(context.Background(), 123)
	require.Error(t, err)
	assert.Nil(t, owners)
	assert.Contains(t, err.Error(), "load owners")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// LoadFunds
// ---------------------------------------------------------------------------

func TestLoadFunds_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	gav := int64(5_000_000)
	nav := int64(4_500_000)
	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "fund_id", "fund_name", "fund_type", "gross_asset_value", "net_asset_value"}).
			AddRow(123, "F001", "Alpha Fund", "hedge_fund", &gav, &nav).
			AddRow(123, "F002", "Beta Fund", "private_equity", nil, nil),
	)

	s := NewStore(mock)
	funds, err := s.LoadFunds(context.Background(), 123)
	require.NoError(t, err)
	require.Len(t, funds, 2)
	assert.Equal(t, "F001", funds[0].FundID)
	assert.Equal(t, "Alpha Fund", funds[0].FundName)
	assert.Equal(t, int64(5_000_000), *funds[0].GrossAssetValue)
	assert.Equal(t, "F002", funds[1].FundID)
	assert.Nil(t, funds[1].GrossAssetValue)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadFunds_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "fund_id", "fund_name", "fund_type", "gross_asset_value", "net_asset_value"}),
	)

	s := NewStore(mock)
	funds, err := s.LoadFunds(context.Background(), 123)
	require.NoError(t, err)
	assert.Empty(t, funds)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadFunds_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnError(fmt.Errorf("timeout"))

	s := NewStore(mock)
	funds, err := s.LoadFunds(context.Background(), 123)
	require.Error(t, err)
	assert.Nil(t, funds)
	assert.Contains(t, err.Error(), "load funds")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// ListAdvisors
// ---------------------------------------------------------------------------

func TestListAdvisors_Basic(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT DISTINCT").WillReturnRows(
		pgxmock.NewRows([]string{"crd_number"}).AddRow(100).AddRow(200).AddRow(300),
	)

	s := NewStore(mock)
	crds, err := s.ListAdvisors(context.Background(), ListOpts{IncludeExtracted: true})
	require.NoError(t, err)
	assert.Equal(t, []int{100, 200, 300}, crds)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListAdvisors_WithStateFilter(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT DISTINCT").WithArgs("CA").WillReturnRows(
		pgxmock.NewRows([]string{"crd_number"}).AddRow(100),
	)

	s := NewStore(mock)
	crds, err := s.ListAdvisors(context.Background(), ListOpts{
		State:            "CA",
		IncludeExtracted: true,
	})
	require.NoError(t, err)
	assert.Equal(t, []int{100}, crds)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListAdvisors_WithLimit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT DISTINCT").WithArgs(10).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number"}).AddRow(100),
	)

	s := NewStore(mock)
	crds, err := s.ListAdvisors(context.Background(), ListOpts{
		Limit:            10,
		IncludeExtracted: true,
	})
	require.NoError(t, err)
	assert.Equal(t, []int{100}, crds)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// CreateRun
// ---------------------------------------------------------------------------

func TestCreateRun_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO").WithArgs(123, "advisor", "").WillReturnRows(
		pgxmock.NewRows([]string{"id"}).AddRow(int64(42)),
	)

	s := NewStore(mock)
	id, err := s.CreateRun(context.Background(), 123, "advisor", "")
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateRun_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("INSERT INTO").WithArgs(123, "advisor", "").WillReturnError(fmt.Errorf("constraint violation"))

	s := NewStore(mock)
	id, err := s.CreateRun(context.Background(), 123, "advisor", "")
	require.Error(t, err)
	assert.Equal(t, int64(0), id)
	assert.Contains(t, err.Error(), "create run")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// CompleteRun
// ---------------------------------------------------------------------------

func TestCompleteRun_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE").WithArgs(
		int64(42), 2, 100, 95, 5000, 1000, 0.25,
	).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	s := NewStore(mock)
	err = s.CompleteRun(context.Background(), 42, RunStats{
		TierCompleted:  2,
		TotalQuestions: 100,
		Answered:       95,
		InputTokens:    5000,
		OutputTokens:   1000,
		CostUSD:        0.25,
	})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCompleteRun_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE").WithArgs(
		int64(42), 1, 10, 5, 100, 50, 0.01,
	).WillReturnError(fmt.Errorf("connection lost"))

	s := NewStore(mock)
	err = s.CompleteRun(context.Background(), 42, RunStats{
		TierCompleted:  1,
		TotalQuestions: 10,
		Answered:       5,
		InputTokens:    100,
		OutputTokens:   50,
		CostUSD:        0.01,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "complete run")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// FailRun
// ---------------------------------------------------------------------------

func TestFailRun_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE").WithArgs(int64(42), "something broke").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	s := NewStore(mock)
	err = s.FailRun(context.Background(), 42, "something broke")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFailRun_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE").WithArgs(int64(42), "err msg").
		WillReturnError(fmt.Errorf("connection lost"))

	s := NewStore(mock)
	err = s.FailRun(context.Background(), 42, "err msg")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail run")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// WriteAdvisorAnswers
// ---------------------------------------------------------------------------

func TestWriteAdvisorAnswers_Empty(t *testing.T) {
	s := NewStore(nil)
	err := s.WriteAdvisorAnswers(context.Background(), nil)
	require.NoError(t, err)
}

func TestWriteAdvisorAnswers_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(
		pgx.Identifier{"_tmp_upsert_fed_data_adv_advisor_answers"},
		[]string{
			"crd_number", "question_key", "value", "confidence", "tier",
			"reasoning", "source_doc", "source_section", "model",
			"input_tokens", "output_tokens", "run_id", "extracted_at",
		},
	).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	s := NewStore(mock)
	err = s.WriteAdvisorAnswers(context.Background(), []Answer{
		{
			CRDNumber:   123,
			QuestionKey: "fee_schedule",
			Value:       "1%",
			Confidence:  0.9,
			Tier:        1,
			Reasoning:   "Found in brochure",
			SourceDoc:   "brochure",
			Model:       "haiku",
			RunID:       42,
		},
	})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// WriteFundAnswers
// ---------------------------------------------------------------------------

func TestWriteFundAnswers_Empty(t *testing.T) {
	s := NewStore(nil)
	err := s.WriteFundAnswers(context.Background(), nil)
	require.NoError(t, err)
}

func TestWriteFundAnswers_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(
		pgx.Identifier{"_tmp_upsert_fed_data_adv_fund_answers"},
		[]string{
			"crd_number", "fund_id", "question_key", "value", "confidence", "tier",
			"reasoning", "source_doc", "source_section", "model",
			"input_tokens", "output_tokens", "run_id", "extracted_at",
		},
	).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	s := NewStore(mock)
	err = s.WriteFundAnswers(context.Background(), []Answer{
		{
			CRDNumber:   123,
			FundID:      "F001",
			QuestionKey: "fund_strategy",
			Value:       "long-short equity",
			Confidence:  0.85,
			Tier:        1,
			SourceDoc:   "brochure",
			Model:       "haiku",
			RunID:       42,
		},
	})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// ArchiveExistingAnswers
// ---------------------------------------------------------------------------

func TestArchiveExistingAnswers_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First exec: archive advisor answers.
	mock.ExpectExec("INSERT INTO fed_data.adv_answer_history").
		WithArgs(123, int64(42)).
		WillReturnResult(pgxmock.NewResult("INSERT", 5))

	// Second exec: archive fund answers.
	mock.ExpectExec("INSERT INTO fed_data.adv_answer_history").
		WithArgs(123, int64(42)).
		WillReturnResult(pgxmock.NewResult("INSERT", 3))

	s := NewStore(mock)
	err = s.ArchiveExistingAnswers(context.Background(), 123, 42)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestArchiveExistingAnswers_FirstExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO fed_data.adv_answer_history").
		WithArgs(123, int64(42)).
		WillReturnError(fmt.Errorf("permission denied"))

	s := NewStore(mock)
	err = s.ArchiveExistingAnswers(context.Background(), 123, 42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "archive advisor answers")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// WriteSectionIndex
// ---------------------------------------------------------------------------

func TestWriteSectionIndex_Empty(t *testing.T) {
	s := NewStore(nil)
	err := s.WriteSectionIndex(context.Background(), nil)
	require.NoError(t, err)
}

func TestWriteSectionIndex_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(
		pgx.Identifier{"_tmp_upsert_fed_data_adv_document_sections"},
		[]string{"crd_number", "doc_type", "doc_id", "section_key", "section_title", "char_length", "token_estimate"},
	).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	s := NewStore(mock)
	err = s.WriteSectionIndex(context.Background(), []SectionIndexEntry{
		{
			CRDNumber:     123,
			DocType:       "brochure",
			DocID:         "B001",
			SectionKey:    "item_4",
			SectionTitle:  "Advisory Business",
			CharLength:    5000,
			TokenEstimate: 1200,
		},
	})
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// WriteComputedMetrics
// ---------------------------------------------------------------------------

func TestWriteComputedMetrics_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	m := &ComputedMetrics{CRDNumber: 123}

	mock.ExpectExec("INSERT INTO fed_data.adv_computed_metrics").
		WithArgs(
			m.CRDNumber,
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
			m.RegulatoryRiskScore,
		).WillReturnResult(pgxmock.NewResult("INSERT", 1))

	s := NewStore(mock)
	err = s.WriteComputedMetrics(context.Background(), m)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWriteComputedMetrics_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	m := &ComputedMetrics{CRDNumber: 123}

	mock.ExpectExec("INSERT INTO fed_data.adv_computed_metrics").
		WithArgs(
			m.CRDNumber,
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
			m.RegulatoryRiskScore,
		).WillReturnError(fmt.Errorf("disk full"))

	s := NewStore(mock)
	err = s.WriteComputedMetrics(context.Background(), m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write computed metrics")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// LoadBrochureSections
// ---------------------------------------------------------------------------

func TestLoadBrochureSections_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "brochure_id", "section_key", "section_title", "text_content", "tables", "metadata"}).
			AddRow(123, "B001", "item_4", "Advisory Business", "Some text", json.RawMessage("null"), json.RawMessage("null")).
			AddRow(123, "B001", "item_5", "Fees", "Fee info", json.RawMessage(`[{"header":"Fee Schedule"}]`), json.RawMessage(`{"page":5}`)),
	)

	s := NewStore(mock)
	sections, err := s.LoadBrochureSections(context.Background(), 123)
	require.NoError(t, err)
	require.Len(t, sections, 2)
	assert.Equal(t, "item_4", sections[0].SectionKey)
	assert.Equal(t, "Advisory Business", sections[0].SectionTitle)
	assert.Equal(t, "Some text", sections[0].TextContent)
	assert.Equal(t, "item_5", sections[1].SectionKey)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadBrochureSections_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(123).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "brochure_id", "section_key", "section_title", "text_content", "tables", "metadata"}),
	)

	s := NewStore(mock)
	sections, err := s.LoadBrochureSections(context.Background(), 123)
	require.NoError(t, err)
	assert.Empty(t, sections)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// LoadCRSSections
// ---------------------------------------------------------------------------

func TestLoadCRSSections_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(456).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "crs_id", "section_key", "section_title", "text_content", "tables", "metadata"}).
			AddRow(456, "C001", "intro", "Introduction", "Intro text", json.RawMessage("null"), json.RawMessage("null")),
	)

	s := NewStore(mock)
	sections, err := s.LoadCRSSections(context.Background(), 456)
	require.NoError(t, err)
	require.Len(t, sections, 1)
	assert.Equal(t, "intro", sections[0].SectionKey)
	assert.Equal(t, "C001", sections[0].DocID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadCRSSections_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(456).WillReturnRows(
		pgxmock.NewRows([]string{"crd_number", "crs_id", "section_key", "section_title", "text_content", "tables", "metadata"}),
	)

	s := NewStore(mock)
	sections, err := s.LoadCRSSections(context.Background(), 456)
	require.NoError(t, err)
	assert.Empty(t, sections)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// RefreshMaterializedView
// ---------------------------------------------------------------------------

func TestRefreshMaterializedView_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("REFRESH MATERIALIZED VIEW").
		WillReturnResult(pgxmock.NewResult("REFRESH", 0))

	s := NewStore(mock)
	err = s.RefreshMaterializedView(context.Background())
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRefreshMaterializedView_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("REFRESH MATERIALIZED VIEW").
		WillReturnError(fmt.Errorf("view does not exist"))

	s := NewStore(mock)
	err = s.RefreshMaterializedView(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refresh materialized view")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// jsonValue
// ---------------------------------------------------------------------------

func TestJsonValue_Store(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"nil", nil, "null"},
		{"string", "hello", `"hello"`},
		{"number", 42, "42"},
		{"bool", true, "true"},
		{"nested", map[string]string{"key": "val"}, `{"key":"val"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jsonValue(tt.input)
			assert.Equal(t, tt.expected, string(got))
		})
	}
}

// ---------------------------------------------------------------------------
// itoa
// ---------------------------------------------------------------------------

func TestItoa(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{1, "1"},
		{9, "9"},
		{10, "10"},
		{15, "15"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, itoa(tt.input))
		})
	}
}
