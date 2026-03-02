package geospatial

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

var errTest = errors.New("test error")

func TestCountyStatsAll_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geoid", "county_name", "state_fips",
		"poi_count", "infrastructure_count", "epa_site_count", "total_capacity",
	}).
		AddRow("01001", "Autauga", "01", 5, 3, 1, 100.5).
		AddRow("01003", "Baldwin", "01", 12, 7, 4, 250.0)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	stats, err := CountyStatsAll(context.Background(), mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(stats))
	}
	if stats[0].GEOID != "01001" {
		t.Errorf("expected GEOID 01001, got %s", stats[0].GEOID)
	}
	if stats[0].POICount != 5 {
		t.Errorf("expected POI count 5, got %d", stats[0].POICount)
	}
	if stats[1].TotalCapacity != 250.0 {
		t.Errorf("expected capacity 250.0, got %f", stats[1].TotalCapacity)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCountyStatsAll_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geoid", "county_name", "state_fips",
		"poi_count", "infrastructure_count", "epa_site_count", "total_capacity",
	})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	stats, err := CountyStatsAll(context.Background(), mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("expected 0 stats, got %d", len(stats))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCountyStatsAll_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT").WillReturnError(errTest)

	_, err = CountyStatsAll(context.Background(), mock)
	if err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInfrastructureDensityByCounty_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geoid", "county_name", "type", "count", "total_capacity",
	}).
		AddRow("01001", "Autauga", "power_plant", 3, 500.0).
		AddRow("01001", "Autauga", "substation", 5, 0.0)

	mock.ExpectQuery("SELECT").WithArgs("01").WillReturnRows(rows)

	results, err := InfrastructureDensityByCounty(context.Background(), mock, "01")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Type != "power_plant" {
		t.Errorf("expected type power_plant, got %s", results[0].Type)
	}
	if results[1].Count != 5 {
		t.Errorf("expected count 5, got %d", results[1].Count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInfrastructureDensityByCounty_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs("01").WillReturnError(errTest)

	_, err = InfrastructureDensityByCounty(context.Background(), mock, "01")
	if err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestDemographicsByLevel_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geo_level", "total_areas", "total_population",
		"avg_median_income", "avg_median_age", "total_housing", "year",
	}).
		AddRow("county", 3000, int64(330000000), 65000.0, 38.5, int64(140000000), 2023).
		AddRow("place", 29000, int64(250000000), 62000.0, 37.0, int64(110000000), 2023)

	mock.ExpectQuery("SELECT").WithArgs(2023).WillReturnRows(rows)

	summaries, err := DemographicsByLevel(context.Background(), mock, 2023)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("expected 2 summaries, got %d", len(summaries))
	}
	if summaries[0].GeoLevel != "county" {
		t.Errorf("expected geo_level county, got %s", summaries[0].GeoLevel)
	}
	if summaries[0].Year != 2023 {
		t.Errorf("expected year 2023, got %d", summaries[0].Year)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestDemographicsByLevel_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geo_level", "total_areas", "total_population",
		"avg_median_income", "avg_median_age", "total_housing", "year",
	})
	mock.ExpectQuery("SELECT").WithArgs(1900).WillReturnRows(rows)

	summaries, err := DemographicsByLevel(context.Background(), mock, 1900)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(summaries) != 0 {
		t.Errorf("expected 0 summaries, got %d", len(summaries))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCountyStatsAll_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geoid", "county_name", "state_fips",
		"poi_count", "infrastructure_count", "epa_site_count", "total_capacity",
	}).AddRow("01001", "Autauga", "01", "not-an-int", 3, 1, 100.5) // wrong type triggers scan error

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	_, err = CountyStatsAll(context.Background(), mock)
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestInfrastructureDensityByCounty_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geoid", "county_name", "type", "count", "total_capacity",
	}).AddRow("01001", "Autauga", "power_plant", "not-an-int", 500.0)

	mock.ExpectQuery("SELECT").WithArgs("01").WillReturnRows(rows)

	_, err = InfrastructureDensityByCounty(context.Background(), mock, "01")
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestDemographicsByLevel_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"geo_level", "total_areas", "total_population",
		"avg_median_income", "avg_median_age", "total_housing", "year",
	}).AddRow("county", "not-an-int", int64(330000000), 65000.0, 38.5, int64(140000000), 2023)

	mock.ExpectQuery("SELECT").WithArgs(2023).WillReturnRows(rows)

	_, err = DemographicsByLevel(context.Background(), mock, 2023)
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestDemographicsByLevel_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT").WithArgs(2023).WillReturnError(errTest)

	_, err = DemographicsByLevel(context.Background(), mock, 2023)
	if err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
