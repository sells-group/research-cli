package geospatial

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// QueryWithinDistance
// ---------------------------------------------------------------------------

func TestQueryWithinDistance_ValidQuery(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()
	mock.ExpectQuery(`SELECT \* FROM geo\.poi WHERE ST_DWithin`).
		WithArgs(-97.74, 30.27, 1000.0, 10).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "name", "category", "subcategory", "address", "geom",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			1, "Capitol", "government", "state_capitol", "1100 Congress Ave", nil,
			30.27, -97.74, "osm", "node123", json.RawMessage(`{}`), now, now,
		))

	rows, err := QueryWithinDistance(context.Background(), mock, "geo.poi", -97.74, 30.27, 1000.0, 10)
	require.NoError(t, err)
	defer rows.Close()

	assert.True(t, rows.Next())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryWithinDistance_InvalidTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	_, err = QueryWithinDistance(context.Background(), mock, "public.users; DROP TABLE", -97.74, 30.27, 1000.0, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid table name")
}

func TestQueryWithinDistance_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT \* FROM geo\.poi WHERE ST_DWithin`).
		WithArgs(-97.74, 30.27, 1000.0, 10).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = QueryWithinDistance(context.Background(), mock, "geo.poi", -97.74, 30.27, 1000.0, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query within distance")
}

// ---------------------------------------------------------------------------
// QueryBBox
// ---------------------------------------------------------------------------

func TestQueryBBox_ValidQuery(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()
	bbox := BBox{MinLng: -98.0, MinLat: 30.0, MaxLng: -97.0, MaxLat: 31.0}

	mock.ExpectQuery(`SELECT \* FROM geo\.counties WHERE geom && ST_MakeEnvelope`).
		WithArgs(bbox.MinLng, bbox.MinLat, bbox.MaxLng, bbox.MaxLat, 50, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad", "geom",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			1, "48453", "48", "453", "Travis County", "06", nil,
			30.33, -97.75, "tiger", "tl_2024", json.RawMessage(`{}`), now, now,
		))

	rows, err := QueryBBox(context.Background(), mock, "geo.counties", bbox, 50, 0)
	require.NoError(t, err)
	defer rows.Close()

	assert.True(t, rows.Next())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryBBox_InvalidTable(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	_, err = QueryBBox(context.Background(), mock, "evil_table", BBox{}, 10, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid table name")
}

func TestQueryBBox_EmptyResult(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	bbox := BBox{MinLng: 0, MinLat: 0, MaxLng: 1, MaxLat: 1}
	mock.ExpectQuery(`SELECT \* FROM geo\.places WHERE geom && ST_MakeEnvelope`).
		WithArgs(bbox.MinLng, bbox.MinLat, bbox.MaxLng, bbox.MaxLat, 10, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips", "geom",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	rows, err := QueryBBox(context.Background(), mock, "geo.places", bbox, 10, 0)
	require.NoError(t, err)
	defer rows.Close()

	assert.False(t, rows.Next())
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ---------------------------------------------------------------------------
// SearchText
// ---------------------------------------------------------------------------

func TestSearchText_MatchesFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()
	mock.ExpectQuery(`SELECT .+ FROM geo\.poi WHERE to_tsvector`).
		WithArgs("capitol", 10).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "name", "category", "subcategory", "address",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).
			AddRow(1, "Texas State Capitol", "government", "capitol", "1100 Congress Ave",
				30.27, -97.74, "osm", "node1", json.RawMessage(`{}`), now, now).
			AddRow(2, "Capitol Visitor Center", "tourism", "museum", "112 E 11th St",
				30.27, -97.74, "osm", "node2", json.RawMessage(`{}`), now, now))

	pois, err := SearchText(context.Background(), mock, "capitol", 10)
	require.NoError(t, err)
	assert.Len(t, pois, 2)
	assert.Equal(t, "Texas State Capitol", pois[0].Name)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSearchText_NoMatches(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT .+ FROM geo\.poi WHERE to_tsvector`).
		WithArgs("zznonexistent", 10).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "name", "category", "subcategory", "address",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	pois, err := SearchText(context.Background(), mock, "zznonexistent", 10)
	require.NoError(t, err)
	assert.Empty(t, pois)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSearchText_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT .+ FROM geo\.poi WHERE to_tsvector`).
		WithArgs("capitol", 10).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = SearchText(context.Background(), mock, "capitol", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "search text")
}

// ---------------------------------------------------------------------------
// PointInPolygon
// ---------------------------------------------------------------------------

func TestPointInPolygon_FullContext(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "4805000", "48", "05000", "Austin", "25", "C1",
			30.27, -97.74, "tiger", "", json.RawMessage(`{}`), now, now))

	// CBSA
	mock.ExpectQuery(`SELECT .+ FROM geo\.cbsa WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "cbsa_code", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "12420", "Austin-Round Rock-Georgetown, TX", "M1",
			30.3, -97.7, "tiger", "", json.RawMessage(`{}`), now, now))

	// Census tract
	mock.ExpectQuery(`SELECT .+ FROM geo\.census_tracts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "tract_ce", "name",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453001100", "48", "453", "001100", "11",
			30.27, -97.74, "tiger", "", json.RawMessage(`{}`), now, now))

	// ZCTA
	mock.ExpectQuery(`SELECT .+ FROM geo\.zcta WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "zcta5", "state_fips", "aland", "awater",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "78701", "48", int64(5000000), int64(100000),
			30.27, -97.74, "tiger", "", json.RawMessage(`{}`), now, now))

	// Congressional district
	mock.ExpectQuery(`SELECT .+ FROM geo\.congressional_districts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "district", "congress", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "4835", "48", "35", "118", "Congressional District 35", "C2",
			30.27, -97.74, "tiger", "", json.RawMessage(`{}`), now, now))

	lc, err := PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.NoError(t, err)
	require.NotNil(t, lc.County)
	assert.Equal(t, "Travis County", lc.County.Name)
	require.NotNil(t, lc.Place)
	assert.Equal(t, "Austin", lc.Place.Name)
	require.NotNil(t, lc.CBSA)
	assert.Equal(t, "12420", lc.CBSA.CBSACode)
	require.NotNil(t, lc.CensusTract)
	assert.Equal(t, "48453001100", lc.CensusTract.GEOID)
	require.NotNil(t, lc.ZCTA)
	assert.Equal(t, "78701", lc.ZCTA.ZCTA5)
	require.NotNil(t, lc.CongressionalDistrict)
	assert.Equal(t, "35", lc.CongressionalDistrict.District)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPointInPolygon_PartialContext(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County found
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place: no match (empty rows)
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// CBSA: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.cbsa WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "cbsa_code", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// Census tract: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.census_tracts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "tract_ce", "name",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// ZCTA: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.zcta WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "zcta5", "state_fips", "aland", "awater",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// Congressional district: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.congressional_districts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "district", "congress", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	lc, err := PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.NoError(t, err)
	require.NotNil(t, lc.County)
	assert.Equal(t, "Travis County", lc.County.Name)
	assert.Nil(t, lc.Place)
	assert.Nil(t, lc.CBSA)
	assert.Nil(t, lc.CensusTract)
	assert.Nil(t, lc.ZCTA)
	assert.Nil(t, lc.CongressionalDistrict)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPointInPolygon_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// County query fails
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pip county")
}

func TestPointInPolygon_PlaceDBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County succeeds
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place query fails
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pip place")
}

func TestPointInPolygon_CBSADBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County succeeds
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// CBSA query fails
	mock.ExpectQuery(`SELECT .+ FROM geo\.cbsa WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pip cbsa")
}

func TestPointInPolygon_CensusTractDBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County succeeds
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// CBSA: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.cbsa WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "cbsa_code", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// Census tract fails
	mock.ExpectQuery(`SELECT .+ FROM geo\.census_tracts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pip census tract")
}

func TestPointInPolygon_ZCTADBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County succeeds
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// CBSA: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.cbsa WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "cbsa_code", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// Census tract: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.census_tracts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "tract_ce", "name",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// ZCTA fails
	mock.ExpectQuery(`SELECT .+ FROM geo\.zcta WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pip zcta")
}

func TestPointInPolygon_CongressionalDistrictDBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	now := time.Now()

	// County succeeds
	mock.ExpectQuery(`SELECT .+ FROM geo\.counties WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now))

	// Place: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.places WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "place_fips", "name", "lsad", "class_fips",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// CBSA: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.cbsa WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "cbsa_code", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// Census tract: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.census_tracts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "tract_ce", "name",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// ZCTA: no match
	mock.ExpectQuery(`SELECT .+ FROM geo\.zcta WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "zcta5", "state_fips", "aland", "awater",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	// Congressional district fails
	mock.ExpectQuery(`SELECT .+ FROM geo\.congressional_districts WHERE ST_Contains`).
		WithArgs(-97.74, 30.27).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = PointInPolygon(context.Background(), mock, -97.74, 30.27)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pip congressional district")
}

// ---------------------------------------------------------------------------
// SearchText scan error
// ---------------------------------------------------------------------------

func TestSearchText_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Return a row with wrong column types to trigger scan error.
	mock.ExpectQuery(`SELECT .+ FROM geo\.poi WHERE to_tsvector`).
		WithArgs("capitol", 10).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "name", "category", "subcategory", "address",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			"not_an_int", "Capitol", "government", "", "",
			30.27, -97.74, "osm", "", json.RawMessage(`{}`), time.Now(), time.Now(),
		))

	_, err = SearchText(context.Background(), mock, "capitol", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan text search row")
}

// ---------------------------------------------------------------------------
// QueryBBox DB error
// ---------------------------------------------------------------------------

func TestQueryBBox_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	bbox := BBox{MinLng: -98.0, MinLat: 30.0, MaxLng: -97.0, MaxLat: 31.0}
	mock.ExpectQuery(`SELECT \* FROM geo\.counties WHERE geom && ST_MakeEnvelope`).
		WithArgs(bbox.MinLng, bbox.MinLat, bbox.MaxLng, bbox.MaxLat, 50, 0).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err = QueryBBox(context.Background(), mock, "geo.counties", bbox, 50, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query bbox")
}

// ---------------------------------------------------------------------------
// validateTable
// ---------------------------------------------------------------------------

func TestValidateTable(t *testing.T) {
	// Valid tables
	for table := range validTables {
		assert.NoError(t, validateTable(table), "expected %q to be valid", table)
	}

	// Invalid tables
	assert.Error(t, validateTable("public.users"))
	assert.Error(t, validateTable(""))
	assert.Error(t, validateTable("geo.poi; DROP TABLE geo.poi"))
}
