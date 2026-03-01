package geocode

import (
	"context"
	"database/sql"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReverseGeocode_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	fullAddr := sql.NullString{String: "100 Main St, Miami, FL 33131", Valid: true}
	state := sql.NullString{String: "FL", Valid: true}
	zip := sql.NullString{String: "33131", Valid: true}
	countyFIPS := sql.NullString{String: "12086", Valid: true}

	mock.ExpectQuery(`SELECT\s+pprint_addy`).
		WithArgs(-80.19, 25.77).
		WillReturnRows(
			pgxmock.NewRows([]string{"pprint_addy", "stateusps", "zip", "county_fips", "rating"}).
				AddRow(fullAddr, state, zip, countyFIPS, 3),
		)

	result, err := ReverseGeocode(context.Background(), mock, 25.77, -80.19)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "100 Main St, Miami, FL 33131", result.Street)
	assert.Equal(t, "FL", result.State)
	assert.Equal(t, "33131", result.ZipCode)
	assert.Equal(t, "12086", result.CountyFIPS)
	assert.Equal(t, 3, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReverseGeocode_NoResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT\s+pprint_addy`).
		WithArgs(-180.0, 90.0).
		WillReturnError(assert.AnError)

	result, err := ReverseGeocode(context.Background(), mock, 90.0, -180.0)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "reverse geocode")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReverseGeocode_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT\s+pprint_addy`).
		WithArgs(-97.75, 30.33).
		WillReturnError(assert.AnError)

	result, err := ReverseGeocode(context.Background(), mock, 30.33, -97.75)

	assert.Error(t, err)
	assert.Nil(t, result)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReverseGeocode_NullFields(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Return result with some NULL fields.
	mock.ExpectQuery(`SELECT\s+pprint_addy`).
		WithArgs(-80.19, 25.77).
		WillReturnRows(
			pgxmock.NewRows([]string{"pprint_addy", "stateusps", "zip", "county_fips", "rating"}).
				AddRow(sql.NullString{}, sql.NullString{String: "FL", Valid: true}, sql.NullString{}, sql.NullString{}, 50),
		)

	result, err := ReverseGeocode(context.Background(), mock, 25.77, -80.19)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "", result.Street)
	assert.Equal(t, "FL", result.State)
	assert.Equal(t, "", result.ZipCode)
	assert.Equal(t, "", result.CountyFIPS)
	assert.Equal(t, 50, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}
