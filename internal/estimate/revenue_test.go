package estimate

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRevenueEstimator_NilPool(t *testing.T) {
	e := NewRevenueEstimator(nil)
	assert.Nil(t, e)
}

func TestEstimate_ZeroEmployees(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	e := NewRevenueEstimator(mock)
	_, err = e.Estimate(context.Background(), "541512", "06", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "employee count must be positive")
}

func TestEstimate_EmptyNAICS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	e := NewRevenueEstimator(mock)
	_, err = e.Estimate(context.Background(), "", "06", 50)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NAICS code is required")
}

func TestEstimate_6DigitNAICS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 6-digit NAICS query returns data.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("541512%", "06").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("541512", 2022, int64(50000), int64(1500), int64(5000000)),
		)

	e := NewRevenueEstimator(mock)
	est, err := e.Estimate(context.Background(), "541512", "06", 100)
	require.NoError(t, err)

	// avg payroll per emp = (5_000_000 * 1000) / 50_000 = 100_000
	// NAICS prefix "54" => multiplier 2.2
	// revenue = 100_000 * 100 * 2.2 = 22_000_000
	assert.Equal(t, int64(22_000_000), est.Amount)
	assert.Equal(t, "cbp_payroll_ratio", est.Method)
	assert.Equal(t, "541512", est.NAICSUsed)
	assert.Equal(t, 2022, est.Year)

	// Confidence: base 0.6 + 0.1 (est>100) + 0.1 (est>1000) + 0.1 (6-digit) = 0.9
	assert.InDelta(t, 0.9, est.Confidence, 0.01)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimate_Fallback4Digit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 6-digit returns no rows.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("541512%", "06").
		WillReturnRows(pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}))

	// 4-digit returns data.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("5415%", "06").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("5415", 2022, int64(80000), int64(2000), int64(8000000)),
		)

	e := NewRevenueEstimator(mock)
	est, err := e.Estimate(context.Background(), "541512", "06", 50)
	require.NoError(t, err)

	assert.Equal(t, "5415", est.NAICSUsed)
	// fellBack = true: confidence base 0.6 + 0.1 (>100) + 0.1 (>1000) - 0.1 (fallback) = 0.7
	assert.InDelta(t, 0.7, est.Confidence, 0.01)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimate_Fallback2Digit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 6-digit returns no rows.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("541512%", "06").
		WillReturnRows(pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}))

	// 4-digit returns no rows.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("5415%", "06").
		WillReturnRows(pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}))

	// 2-digit returns data.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("54%", "06").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("54", 2021, int64(500000), int64(50000), int64(50000000)),
		)

	e := NewRevenueEstimator(mock)
	est, err := e.Estimate(context.Background(), "541512", "06", 200)
	require.NoError(t, err)

	assert.Equal(t, "54", est.NAICSUsed)
	// avg payroll per emp = (50_000_000 * 1000) / 500_000 = 100_000
	// multiplier for "54" = 2.2
	// revenue = 100_000 * 200 * 2.2 = 44_000_000
	assert.Equal(t, int64(44_000_000), est.Amount)
	assert.Equal(t, 2021, est.Year)
	// fellBack = true, 2-digit: base 0.6 + 0.1 (>100) + 0.1 (>1000) - 0.1 (fallback) = 0.7
	// 2-digit is not >= 6 digits so no +0.1
	assert.InDelta(t, 0.7, est.Confidence, 0.01)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimate_NoDataAnywhere(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// All levels return no rows.
	for _, pattern := range []string{"541512%", "5415%", "54%"} {
		mock.ExpectQuery("SELECT naics, year").
			WithArgs(pattern, "06").
			WillReturnRows(pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}))
	}

	e := NewRevenueEstimator(mock)
	_, err = e.Estimate(context.Background(), "541512", "06", 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no CBP data")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimate_ConstructionMultiplier(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT naics, year").
		WithArgs("236220%", "48").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("236220", 2022, int64(100000), int64(5000), int64(10000000)),
		)

	e := NewRevenueEstimator(mock)
	est, err := e.Estimate(context.Background(), "236220", "48", 75)
	require.NoError(t, err)

	// avg payroll = (10_000_000 * 1000) / 100_000 = 100_000
	// Construction "23" multiplier = 2.85
	// revenue = 100_000 * 75 * 2.85 = 21_375_000
	assert.Equal(t, int64(21_375_000), est.Amount)
	assert.Equal(t, "cbp_payroll_ratio", est.Method)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimate_DefaultMultiplier(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// NAICS 72 (Accommodation/Food) uses default multiplier.
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("722511%", "36").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("722511", 2022, int64(200000), int64(10000), int64(8000000)),
		)

	e := NewRevenueEstimator(mock)
	est, err := e.Estimate(context.Background(), "722511", "36", 30)
	require.NoError(t, err)

	// avg payroll = (8_000_000 * 1000) / 200_000 = 40_000
	// Default multiplier = 3.3
	// revenue = 40_000 * 30 * 3.3 = 3_960_000
	assert.Equal(t, int64(3_960_000), est.Amount)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimate_EmptyStateFIPS(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// With empty state FIPS, query aggregates across all states (no state filter).
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("5415%").
		WillReturnRows(pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}))

	// Try without state, fallback path: 6-digit query had no state param.
	// The first query with "541512%" also has no state.
	mock2, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock2.Close()

	mock2.ExpectQuery("SELECT naics, year").
		WithArgs("541512%").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("541512", 2022, int64(200000), int64(5000), int64(20000000)),
		)

	e := NewRevenueEstimator(mock2)
	est, err := e.Estimate(context.Background(), "541512", "", 100)
	require.NoError(t, err)
	assert.Equal(t, "541512", est.NAICSUsed)
	assert.NoError(t, mock2.ExpectationsWereMet())
}

func TestEstimate_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT naics, year").
		WithArgs("541512%", "06").
		WillReturnError(fmt.Errorf("connection refused"))

	e := NewRevenueEstimator(mock)
	_, err = e.Estimate(context.Background(), "541512", "06", 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query market size")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestNAICSLevels(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"541512", []string{"541512", "5415", "54"}},
		{"5415", []string{"5415", "54"}},
		{"54", []string{"54"}},
		{"2", []string{"2"}},
		{"236220", []string{"236220", "2362", "23"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := naicsLevels(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestMultiplier(t *testing.T) {
	assert.Equal(t, 2.85, Multiplier("23"))
	assert.Equal(t, 2.2, Multiplier("54"))
	assert.Equal(t, 2.5, Multiplier("56"))
	assert.Equal(t, 2.5, Multiplier("62"))
	assert.Equal(t, 3.3, Multiplier("72"))
	assert.Equal(t, 3.3, Multiplier("99"))
}

func TestFormatRevenue(t *testing.T) {
	tests := []struct {
		amount   int64
		expected string
	}{
		{1_500_000_000, "$1.5B"},
		{22_000_000, "$22.0M"},
		{500_000, "$500K"},
		{999, "$999"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, FormatRevenue(tt.amount))
		})
	}
}

func TestEstimate_ErrNoRows(t *testing.T) {
	// Verify that pgx.ErrNoRows is handled gracefully (returns nil, nil)
	// rather than treated as a real error.
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// All levels return ErrNoRows directly (simulates pgx driver behavior).
	for _, pattern := range []string{"541512%", "5415%", "54%"} {
		mock.ExpectQuery("SELECT naics, year").
			WithArgs(pattern, "06").
			WillReturnError(pgx.ErrNoRows)
	}

	e := NewRevenueEstimator(mock)
	_, err = e.Estimate(context.Background(), "541512", "06", 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no CBP data")

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConfidenceSmallSample(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Small sample: est=50 (< 100 threshold).
	mock.ExpectQuery("SELECT naics, year").
		WithArgs("541512%", "06").
		WillReturnRows(
			pgxmock.NewRows([]string{"naics", "year", "total_emp", "total_est", "total_payroll"}).
				AddRow("541512", 2022, int64(5000), int64(50), int64(500000)),
		)

	e := NewRevenueEstimator(mock)
	est, err := e.Estimate(context.Background(), "541512", "06", 10)
	require.NoError(t, err)

	// base 0.6 + 0.1 (6-digit) = 0.7 (no sample size bonuses since est=50 < 100)
	assert.InDelta(t, 0.7, est.Confidence, 0.01)
	assert.NoError(t, mock.ExpectationsWereMet())
}
