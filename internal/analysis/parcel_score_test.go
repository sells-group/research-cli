package analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Interface ---

func TestParcelScore_Name(t *testing.T) {
	ps := &ParcelScore{}
	assert.Equal(t, "parcel_scores", ps.Name())
}

func TestParcelScore_Category(t *testing.T) {
	ps := &ParcelScore{}
	assert.Equal(t, Scoring, ps.Category())
}

func TestParcelScore_Dependencies(t *testing.T) {
	ps := &ParcelScore{}
	assert.Equal(t, []string{"proximity_matrix"}, ps.Dependencies())
}

// --- Validate ---

func TestParcelScore_Validate_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT count").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(100)))

	ps := &ParcelScore{}
	assert.NoError(t, ps.Validate(context.Background(), mock))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Validate_EmptyTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT count").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))

	ps := &ParcelScore{}
	err = ps.Validate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "geo.parcel_proximity is empty")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Validate_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT count").
		WillReturnError(fmt.Errorf("connection refused"))

	ps := &ParcelScore{}
	err = ps.Validate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate parcel_proximity")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Run ---

func TestParcelScore_Run_EmptyProximity(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First batch returns 0 rows — no rank update expected.
	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	ps := &ParcelScore{}
	result, err := ps.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Run_SingleBatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch returns 50 rows.
	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 50))

	// Cursor advance: no more rows.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	// Rank update runs since totalRows > 0.
	mock.ExpectExec("UPDATE geo.parcel_scores SET opportunity_rank").
		WillReturnResult(pgxmock.NewResult("UPDATE", 50))

	ps := &ParcelScore{}
	result, err := ps.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(50), result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Run_MultipleBatches(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch 1: full batch.
	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", int64(scoreBatchSize)))

	// Cursor advance batch 1 → "geoid_1000".
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("").
		WillReturnRows(pgxmock.NewRows([]string{"parcel_geoid"}).AddRow("geoid_1000"))

	// Batch 2: partial batch.
	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("geoid_1000", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 200))

	// Cursor advance batch 2: no more rows.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("geoid_1000").
		WillReturnError(fmt.Errorf("no rows in result set"))

	// Rank update.
	mock.ExpectExec("UPDATE geo.parcel_scores SET opportunity_rank").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1200))

	ps := &ParcelScore{}
	result, err := ps.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(scoreBatchSize)+200, result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Run_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnError(fmt.Errorf("relation does not exist"))

	ps := &ParcelScore{}
	_, err = ps.Run(context.Background(), mock, RunOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "process batch")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Run_CursorExhausted(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch succeeds.
	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 500))

	// Cursor query returns no rows.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	// Rank update.
	mock.ExpectExec("UPDATE geo.parcel_scores SET opportunity_rank").
		WillReturnResult(pgxmock.NewResult("UPDATE", 500))

	ps := &ParcelScore{}
	result, err := ps.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(500), result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Run_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ps := &ParcelScore{}
	_, err = ps.Run(ctx, mock, RunOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_Run_RankError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch succeeds.
	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 10))

	// Cursor exhausted.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	// Rank update fails.
	mock.ExpectExec("UPDATE geo.parcel_scores SET opportunity_rank").
		WillReturnError(fmt.Errorf("disk full"))

	ps := &ParcelScore{}
	_, err = ps.Run(context.Background(), mock, RunOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compute ranks")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Metadata ---

func TestParcelScore_Run_Metadata(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 10))

	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	mock.ExpectExec("UPDATE geo.parcel_scores SET opportunity_rank").
		WillReturnResult(pgxmock.NewResult("UPDATE", 10))

	ps := &ParcelScore{}
	result, err := ps.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)

	assert.Equal(t, scoreBatchSize, result.Metadata["batch_size"])
	weightsStr, ok := result.Metadata["weights"].(string)
	require.True(t, ok)
	var weights map[string]float64
	require.NoError(t, json.Unmarshal([]byte(weightsStr), &weights))
	assert.InDelta(t, 0.30, weights["infrastructure"], 0.001)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- processBatch ---

func TestParcelScore_ProcessBatch_ZeroRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.parcel_scores").
		WithArgs("cursor_val", scoreBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	ps := &ParcelScore{}
	rows, err := ps.processBatch(context.Background(), mock, "cursor_val")
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- advanceCursor ---

func TestParcelScore_AdvanceCursor_HasMore(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("start_cursor").
		WillReturnRows(pgxmock.NewRows([]string{"parcel_geoid"}).AddRow("next_cursor"))

	ps := &ParcelScore{}
	cursor, ok := ps.advanceCursor(context.Background(), mock, "start_cursor")
	assert.True(t, ok)
	assert.Equal(t, "next_cursor", cursor)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestParcelScore_AdvanceCursor_Exhausted(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcel_proximity").
		WithArgs("end_cursor").
		WillReturnError(fmt.Errorf("no rows in result set"))

	ps := &ParcelScore{}
	cursor, ok := ps.advanceCursor(context.Background(), mock, "end_cursor")
	assert.False(t, ok)
	assert.Equal(t, "", cursor)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Query builders ---

func TestBuildScoreQuery_ContainsAllSubScores(t *testing.T) {
	q := buildScoreQuery()

	scores := []string{
		"infrastructure_score",
		"connectivity_score",
		"environmental_risk",
		"flood_risk",
		"demographic_score",
	}
	for _, s := range scores {
		assert.Contains(t, q, s, "query missing sub-score: %s", s)
	}
}

func TestBuildScoreQuery_ContainsUpsertClause(t *testing.T) {
	q := buildScoreQuery()
	assert.Contains(t, q, "ON CONFLICT (parcel_geoid) DO UPDATE")
}

func TestBuildScoreQuery_ContainsCTEs(t *testing.T) {
	q := buildScoreQuery()

	ctes := []string{"batch AS", "infra AS", "connectivity AS", "constraints AS", "risk AS", "demo AS", "assembly AS"}
	for _, cte := range ctes {
		assert.Contains(t, q, cte, "query missing CTE: %s", cte)
	}
}

func TestBuildScoreQuery_ContainsConstraintChecks(t *testing.T) {
	q := buildScoreQuery()
	assert.Contains(t, q, "ST_Intersects")
	assert.Contains(t, q, "ST_DWithin")
}

func TestBuildScoreQuery_ContainsAssemblyCheck(t *testing.T) {
	q := buildScoreQuery()
	assert.Contains(t, q, "ST_Touches")
}

func TestBuildRankQuery_ContainsWindowFunction(t *testing.T) {
	q := buildRankQuery()
	assert.Contains(t, q, "RANK() OVER")
}

func TestBuildRankQuery_ContainsOrderByComposite(t *testing.T) {
	q := buildRankQuery()
	assert.Contains(t, q, "ORDER BY composite_score DESC")
}

// --- Default weights ---

func TestDefaultWeights_SumsApproximatelyToOne(t *testing.T) {
	// The additive weights (infra, connectivity, demographic) sum to 0.75.
	// Environmental (0.15) and flood (0.10) are penalty multipliers, not additive.
	// Total accounting: 0.30 + 0.25 + 0.20 + 0.15 + 0.10 = 1.00.
	var total float64
	for _, w := range defaultWeights {
		total += w
	}
	assert.InDelta(t, 1.0, total, 0.001)
}

func TestDefaultWeights_AllPositive(t *testing.T) {
	for name, w := range defaultWeights {
		assert.Greater(t, w, 0.0, "weight %q must be positive", name)
	}
}
