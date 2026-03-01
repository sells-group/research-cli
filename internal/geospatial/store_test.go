package geospatial

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

func TestUpsertCounty_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	c := &County{
		GEOID:      "48453",
		StateFIPS:  "48",
		CountyFIPS: "453",
		Name:       "Travis County",
		LSAD:       "06",
		Latitude:   30.33,
		Longitude:  -97.75,
		Source:     "tiger",
		SourceID:   "tl_2024",
		Properties: json.RawMessage(`{"pop":1290188}`),
	}

	mock.ExpectExec("INSERT INTO geo.counties").
		WithArgs(c.GEOID, c.StateFIPS, c.CountyFIPS, c.Name, c.LSAD,
			c.Latitude, c.Longitude, c.Source, c.SourceID, c.Properties).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertCounty(context.Background(), c)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertCounty_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectExec("INSERT INTO geo.counties").
		WillReturnError(fmt.Errorf("connection refused"))

	err = store.UpsertCounty(context.Background(), &County{Source: "tiger"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert county")
}

func TestGetCounty_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	now := time.Now()

	mock.ExpectQuery("SELECT .+ FROM geo.counties WHERE geoid").
		WithArgs("48453").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			1, "48453", "48", "453", "Travis County", "06",
			30.33, -97.75, "tiger", "tl_2024", json.RawMessage(`{}`),
			now, now,
		))

	c, err := store.GetCounty(context.Background(), "48453")
	require.NoError(t, err)
	assert.Equal(t, "Travis County", c.Name)
	assert.Equal(t, "48", c.StateFIPS)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetCounty_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.counties WHERE geoid").
		WithArgs("99999").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	_, err = store.GetCounty(context.Background(), "99999")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get county")
}

func TestListCountiesByState_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	now := time.Now()

	mock.ExpectQuery("SELECT .+ FROM geo.counties WHERE state_fips").
		WithArgs("48").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).
			AddRow(1, "48453", "48", "453", "Travis", "06", 30.33, -97.75, "tiger", "", json.RawMessage(`{}`), now, now).
			AddRow(2, "48491", "48", "491", "Williamson", "06", 30.63, -97.60, "tiger", "", json.RawMessage(`{}`), now, now))

	counties, err := store.ListCountiesByState(context.Background(), "48")
	require.NoError(t, err)
	assert.Len(t, counties, 2)
	assert.Equal(t, "Travis", counties[0].Name)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListCountiesByState_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.counties WHERE state_fips").
		WithArgs("48").
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = store.ListCountiesByState(context.Background(), "48")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list counties by state")
}

func TestUpsertCBSA_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	c := &CBSA{
		CBSACode: "12420",
		Name:     "Austin-Round Rock-Georgetown, TX",
		LSAD:     "M1",
		Latitude: 30.3, Longitude: -97.7,
		Source: "tiger",
	}

	mock.ExpectExec("INSERT INTO geo.cbsa").
		WithArgs(c.CBSACode, c.Name, c.LSAD, c.Latitude, c.Longitude, c.Source, c.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertCBSA(context.Background(), c)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetCBSA_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	now := time.Now()

	mock.ExpectQuery("SELECT .+ FROM geo.cbsa WHERE cbsa_code").
		WithArgs("12420").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "cbsa_code", "name", "lsad", "latitude", "longitude",
			"source", "source_id", "properties", "created_at", "updated_at",
		}).AddRow(
			1, "12420", "Austin-Round Rock-Georgetown, TX", "M1", 30.3, -97.7,
			"tiger", "", json.RawMessage(`{}`), now, now,
		))

	c, err := store.GetCBSA(context.Background(), "12420")
	require.NoError(t, err)
	assert.Equal(t, "12420", c.CBSACode)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPOI_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	p := &POI{
		Name:      "Test POI",
		Category:  "government",
		Latitude:  30.27,
		Longitude: -97.74,
		Source:    "osm",
	}

	mock.ExpectExec("INSERT INTO geo.poi").
		WithArgs(p.Name, p.Category, p.Subcategory, p.Address,
			p.Latitude, p.Longitude, p.Source, p.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertPOI(context.Background(), p)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPOI_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	now := time.Now()

	mock.ExpectQuery("SELECT .+ FROM geo.poi WHERE id").
		WithArgs(1).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "name", "category", "subcategory", "address",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			1, "Capitol", "government", "state_capitol", "1100 Congress Ave",
			30.27, -97.74, "osm", "node123", json.RawMessage(`{}`), now, now,
		))

	p, err := store.GetPOI(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, "Capitol", p.Name)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPOIByCategory_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	now := time.Now()

	// Count query
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("government").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(2))

	// List query
	mock.ExpectQuery("SELECT .+ FROM geo.poi WHERE category").
		WithArgs("government", 10, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "name", "category", "subcategory", "address",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).
			AddRow(1, "Capitol", "government", "", "", 30.27, -97.74, "osm", "", json.RawMessage(`{}`), now, now).
			AddRow(2, "City Hall", "government", "", "", 30.26, -97.74, "osm", "", json.RawMessage(`{}`), now, now))

	pois, total, err := store.ListPOIByCategory(context.Background(), "government", 10, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, pois, 2)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListPOIByCategory_CountError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("government").
		WillReturnError(fmt.Errorf("connection lost"))

	_, _, err = store.ListPOIByCategory(context.Background(), "government", 10, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "count poi by category")
}

func TestUpsertInfrastructure_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	infra := &Infrastructure{
		Name:      "Test Plant",
		Type:      "power_plant",
		FuelType:  "natural_gas",
		Capacity:  500.0,
		Latitude:  30.32,
		Longitude: -97.62,
		Source:    "eia",
	}

	mock.ExpectExec("INSERT INTO geo.infrastructure").
		WithArgs(infra.Name, infra.Type, infra.FuelType, infra.Capacity,
			infra.Latitude, infra.Longitude, infra.Source, infra.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertInfrastructure(context.Background(), infra)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertEPASite_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	site := &EPASite{
		Name:       "Test Facility",
		Program:    "RCRA",
		RegistryID: "TXD000001234",
		Status:     "active",
		Latitude:   30.27,
		Longitude:  -97.74,
		Source:     "epa",
	}

	mock.ExpectExec("INSERT INTO geo.epa_sites").
		WithArgs(site.Name, site.Program, site.RegistryID, site.Status,
			site.Latitude, site.Longitude, site.Source, site.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertEPASite(context.Background(), site)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFloodZone_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	fz := &FloodZone{
		ZoneCode:  "AE",
		FloodType: "100-year",
		Source:    "fema",
	}

	mock.ExpectExec("INSERT INTO geo.flood_zones").
		WithArgs(fz.ZoneCode, fz.FloodType, fz.Source, fz.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertFloodZone(context.Background(), fz)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertDemographic_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	d := &Demographic{
		GEOID:           "48453",
		GeoLevel:        "county",
		Year:            2022,
		TotalPopulation: 1290188,
		MedianIncome:    78947.0,
		MedianAge:       34.5,
		HousingUnits:    548000,
		Source:          "census",
	}

	mock.ExpectExec("INSERT INTO geo.demographics").
		WithArgs(d.GEOID, d.GeoLevel, d.Year, d.TotalPopulation, d.MedianIncome, d.MedianAge, d.HousingUnits, d.Source, d.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertDemographic(context.Background(), d)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertDemographic_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectExec("INSERT INTO geo.demographics").
		WillReturnError(fmt.Errorf("unique violation"))

	err = store.UpsertDemographic(context.Background(), &Demographic{Source: "census"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert demographic")
}

func TestGetDemographic_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	now := time.Now()

	mock.ExpectQuery("SELECT .+ FROM geo.demographics WHERE geoid").
		WithArgs("48453", "county", 2022).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "geo_level", "year", "total_population", "median_income",
			"median_age", "housing_units", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			1, "48453", "county", 2022, 1290188, 78947.0,
			34.5, 548000, "census", "", json.RawMessage(`{}`), now, now,
		))

	d, err := store.GetDemographic(context.Background(), "48453", "county", 2022)
	require.NoError(t, err)
	assert.Equal(t, 1290188, d.TotalPopulation)
	assert.Equal(t, "county", d.GeoLevel)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetDemographic_NotFound(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.demographics WHERE geoid").
		WithArgs("99999", "county", 2022).
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "geo_level", "year", "total_population", "median_income",
			"median_age", "housing_units", "source", "source_id", "properties",
			"created_at", "updated_at",
		}))

	_, err = store.GetDemographic(context.Background(), "99999", "county", 2022)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "demographic not found")
}

func TestNormalizeProperties(t *testing.T) {
	assert.Equal(t, json.RawMessage(`{}`), normalizeProperties(nil))
	assert.Equal(t, json.RawMessage(`{}`), normalizeProperties(json.RawMessage{}))
	assert.Equal(t, json.RawMessage(`{"a":1}`), normalizeProperties(json.RawMessage(`{"a":1}`)))
}

func TestUpsertPlace_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	p := &Place{
		GEOID:     "4805000",
		StateFIPS: "48",
		PlaceFIPS: "05000",
		Name:      "Austin",
		Source:    "tiger",
	}

	mock.ExpectExec("INSERT INTO geo.places").
		WithArgs(p.GEOID, p.StateFIPS, p.PlaceFIPS, p.Name, p.LSAD, p.ClassFIPS,
			p.Latitude, p.Longitude, p.Source, p.SourceID, json.RawMessage(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err = store.UpsertPlace(context.Background(), p)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPlace_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectExec("INSERT INTO geo.places").
		WillReturnError(fmt.Errorf("connection refused"))

	err = store.UpsertPlace(context.Background(), &Place{Source: "tiger"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert place")
}

// ---------------------------------------------------------------------------
// BulkUpsertCounties
// ---------------------------------------------------------------------------

func TestBulkUpsertCounties_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	counties := []County{
		{GEOID: "48453", StateFIPS: "48", CountyFIPS: "453", Name: "Travis", LSAD: "06",
			Latitude: 30.33, Longitude: -97.75, Source: "tiger", SourceID: "tl_2024",
			Properties: json.RawMessage(`{"pop":1290188}`)},
		{GEOID: "48491", StateFIPS: "48", CountyFIPS: "491", Name: "Williamson", LSAD: "06",
			Latitude: 30.63, Longitude: -97.60, Source: "tiger"},
	}

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(
		pgx.Identifier{"_tmp_upsert_geo_counties"},
		[]string{"geoid", "state_fips", "county_fips", "name", "lsad", "latitude", "longitude", "source", "source_id", "properties"},
	).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mock.ExpectCommit()

	n, err := store.BulkUpsertCounties(context.Background(), counties)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsertCounties_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	counties := []County{{GEOID: "48453", Source: "tiger"}}

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	_, err = store.BulkUpsertCounties(context.Background(), counties)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

// ---------------------------------------------------------------------------
// BulkUpsertPOI
// ---------------------------------------------------------------------------

func TestBulkUpsertPOI_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	pois := []POI{
		{Name: "Capitol", Category: "government", Subcategory: "state_capitol",
			Address: "1100 Congress Ave", Latitude: 30.27, Longitude: -97.74,
			Source: "osm", SourceID: "node123"},
	}

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(
		pgx.Identifier{"_tmp_upsert_geo_poi"},
		[]string{"name", "category", "subcategory", "address", "latitude", "longitude", "source", "source_id", "properties"},
	).WillReturnResult(1)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	n, err := store.BulkUpsertPOI(context.Background(), pois)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsertPOI_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	pois := []POI{{Name: "Test", Source: "osm"}}

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	_, err = store.BulkUpsertPOI(context.Background(), pois)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

// ---------------------------------------------------------------------------
// BulkUpsertInfrastructure
// ---------------------------------------------------------------------------

func TestBulkUpsertInfrastructure_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	infras := []Infrastructure{
		{Name: "Plant A", Type: "power_plant", FuelType: "natural_gas", Capacity: 500.0,
			Latitude: 30.32, Longitude: -97.62, Source: "eia", SourceID: "EIA001"},
		{Name: "Plant B", Type: "solar", Capacity: 200.0,
			Latitude: 30.40, Longitude: -97.50, Source: "eia", SourceID: "EIA002"},
	}

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(
		pgx.Identifier{"_tmp_upsert_geo_infrastructure"},
		[]string{"name", "type", "fuel_type", "capacity", "latitude", "longitude", "source", "source_id", "properties"},
	).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mock.ExpectCommit()

	n, err := store.BulkUpsertInfrastructure(context.Background(), infras)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBulkUpsertInfrastructure_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	infras := []Infrastructure{{Name: "Test", Source: "eia"}}

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	_, err = store.BulkUpsertInfrastructure(context.Background(), infras)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin tx")
}

func TestGetCBSA_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.cbsa WHERE cbsa_code").
		WithArgs("99999").
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = store.GetCBSA(context.Background(), "99999")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get cbsa")
}

func TestGetPOI_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.poi WHERE id").
		WithArgs(999).
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = store.GetPOI(context.Background(), 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get poi")
}

func TestListPOIByCategory_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)

	// Count query succeeds
	mock.ExpectQuery("SELECT COUNT").
		WithArgs("government").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(2))

	// List query fails
	mock.ExpectQuery("SELECT .+ FROM geo.poi WHERE category").
		WithArgs("government", 10, 0).
		WillReturnError(fmt.Errorf("connection lost"))

	_, _, err = store.ListPOIByCategory(context.Background(), "government", 10, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list poi by category")
}

func TestGetDemographic_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.demographics WHERE geoid").
		WithArgs("48453", "county", 2022).
		WillReturnError(fmt.Errorf("connection lost"))

	_, err = store.GetDemographic(context.Background(), "48453", "county", 2022)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get demographic")
}

func TestListCountiesByState_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	store := NewPostgresStore(mock)
	mock.ExpectQuery("SELECT .+ FROM geo.counties WHERE state_fips").
		WithArgs("48").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "geoid", "state_fips", "county_fips", "name", "lsad",
			"latitude", "longitude", "source", "source_id", "properties",
			"created_at", "updated_at",
		}).AddRow(
			"not_an_int", "48453", "48", "453", "Travis", "06",
			30.33, -97.75, "tiger", "", json.RawMessage(`{}`),
			time.Now(), time.Now(),
		))

	_, err = store.ListCountiesByState(context.Background(), "48")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan county row")
}
