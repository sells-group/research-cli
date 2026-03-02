package analysis

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Interface ---

func TestProximityMatrix_Name(t *testing.T) {
	p := &ProximityMatrix{}
	assert.Equal(t, "proximity_matrix", p.Name())
}

func TestProximityMatrix_Category(t *testing.T) {
	p := &ProximityMatrix{}
	assert.Equal(t, Spatial, p.Category())
}

func TestProximityMatrix_Dependencies(t *testing.T) {
	p := &ProximityMatrix{}
	assert.Nil(t, p.Dependencies())
}

// --- Validate ---

func TestProximityMatrix_Validate_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT count").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(100)))

	p := &ProximityMatrix{}
	assert.NoError(t, p.Validate(context.Background(), mock))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Validate_EmptyTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT count").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))

	p := &ProximityMatrix{}
	err = p.Validate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "geo.parcels is empty")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Validate_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT count").
		WillReturnError(fmt.Errorf("connection refused"))

	p := &ProximityMatrix{}
	err = p.Validate(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate parcels")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- Run ---

func TestProximityMatrix_Run_EmptyParcels(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First batch returns 0 rows.
	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	p := &ProximityMatrix{}
	result, err := p.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Run_SingleBatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch returns 50 rows.
	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 50))

	// Cursor advance: no more rows.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcels").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	p := &ProximityMatrix{}
	result, err := p.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(50), result.RowsAffected)
	assert.Equal(t, 15, result.Metadata["feature_types"])
	assert.Equal(t, proximityBatchSize, result.Metadata["batch_size"])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Run_MultipleBatches(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch 1: full batch.
	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", int64(proximityBatchSize)))

	// Cursor advance batch 1 → "geoid_1000".
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcels").
		WithArgs("").
		WillReturnRows(pgxmock.NewRows([]string{"parcel_geoid"}).AddRow("geoid_1000"))

	// Batch 2: partial batch.
	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("geoid_1000", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 200))

	// Cursor advance batch 2: no more rows.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcels").
		WithArgs("geoid_1000").
		WillReturnError(fmt.Errorf("no rows in result set"))

	p := &ProximityMatrix{}
	result, err := p.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(proximityBatchSize)+200, result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Run_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("", proximityBatchSize).
		WillReturnError(fmt.Errorf("relation does not exist"))

	p := &ProximityMatrix{}
	_, err = p.Run(context.Background(), mock, RunOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "process batch")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Run_CursorQueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Batch succeeds.
	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 500))

	// Cursor query fails with an error that indicates end-of-data.
	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcels").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	p := &ProximityMatrix{}
	result, err := p.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)
	assert.Equal(t, int64(500), result.RowsAffected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Run_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &ProximityMatrix{}
	_, err = p.Run(ctx, mock, RunOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProximityMatrix_Run_Metadata(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 10))

	mock.ExpectQuery("SELECT parcel_geoid FROM geo.parcels").
		WithArgs("").
		WillReturnError(fmt.Errorf("no rows in result set"))

	p := &ProximityMatrix{}
	result, err := p.Run(context.Background(), mock, RunOpts{})
	require.NoError(t, err)

	assert.Equal(t, proximityBatchSize, result.Metadata["batch_size"])
	assert.Equal(t, 15, result.Metadata["feature_types"])
}

// --- processBatch ---

func TestProximityMatrix_ProcessBatch_ZeroRows(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.parcel_proximity").
		WithArgs("cursor_val", proximityBatchSize).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	p := &ProximityMatrix{}
	rows, err := p.processBatch(context.Background(), mock, "cursor_val")
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- buildProximityQuery ---

func TestBuildProximityQuery_ContainsAllFeatureTypes(t *testing.T) {
	q := buildProximityQuery()

	features := []string{
		"dist_power_plant", "dist_substation", "dist_transmission_line",
		"dist_pipeline", "dist_telecom_tower", "dist_epa_site",
		"dist_flood_zone", "dist_wetland", "dist_primary_road",
		"dist_highway", "dist_hospital", "dist_school",
		"dist_airport", "dist_fire_station", "dist_water_body",
	}
	for _, f := range features {
		assert.Contains(t, q, f, "query missing feature type: %s", f)
	}

	census := []string{"county_geoid", "cbsa_code", "census_tract_geoid"}
	for _, c := range census {
		assert.Contains(t, q, c, "query missing census field: %s", c)
	}
}

func TestBuildProximityQuery_ContainsUpsertClause(t *testing.T) {
	q := buildProximityQuery()
	assert.Contains(t, q, "ON CONFLICT (parcel_geoid) DO UPDATE")
}
