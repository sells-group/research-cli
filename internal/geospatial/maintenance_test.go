package geospatial

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

func TestVacuumAnalyze_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	for _, table := range geoTables {
		mock.ExpectExec("VACUUM ANALYZE " + table).WillReturnResult(pgxmock.NewResult("VACUUM", 0))
	}

	if err := VacuumAnalyze(context.Background(), mock); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestVacuumAnalyze_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("VACUUM ANALYZE " + geoTables[0]).WillReturnError(errTest)

	if err := VacuumAnalyze(context.Background(), mock); err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestClusterSpatialIndexes_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	// Expect CLUSTER for each spatial table — order is non-deterministic (map iteration).
	mock.MatchExpectationsInOrder(false)
	tables := []string{
		"geo.counties", "geo.places", "geo.zcta", "geo.cbsa",
		"geo.census_tracts", "geo.congressional_districts",
		"geo.poi", "geo.infrastructure", "geo.epa_sites",
		"geo.flood_zones", "geo.demographics",
	}
	for _, table := range tables {
		mock.ExpectExec("CLUSTER " + table).WillReturnResult(pgxmock.NewResult("CLUSTER", 0))
	}

	if err := ClusterSpatialIndexes(context.Background(), mock); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestClusterSpatialIndexes_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.MatchExpectationsInOrder(false)
	// One table fails.
	mock.ExpectExec("CLUSTER").WillReturnError(errTest)

	if err := ClusterSpatialIndexes(context.Background(), mock); err == nil {
		t.Fatal("expected error")
	}
}

func TestGetTableStats_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"table_name", "row_count", "total_size", "index_size", "has_spatial",
	}).
		AddRow("geo.counties", int64(3000), "50 MB", "12 MB", true).
		AddRow("geo.poi", int64(50000), "200 MB", "80 MB", true)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	stats, err := GetTableStats(context.Background(), mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("expected 2 stats, got %d", len(stats))
	}
	if stats[0].TableName != "geo.counties" {
		t.Errorf("expected geo.counties, got %s", stats[0].TableName)
	}
	if stats[0].RowCount != 3000 {
		t.Errorf("expected 3000 rows, got %d", stats[0].RowCount)
	}
	if !stats[0].HasSpatial {
		t.Error("expected has_spatial to be true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetTableStats_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectQuery("SELECT").WillReturnError(errTest)

	_, err = GetTableStats(context.Background(), mock)
	if err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetTableStats_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	rows := pgxmock.NewRows([]string{
		"table_name", "row_count", "total_size", "index_size", "has_spatial",
	}).AddRow("geo.counties", "not-an-int", "50 MB", "12 MB", true)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	_, err = GetTableStats(context.Background(), mock)
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestReindexSpatial_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("REINDEX SCHEMA geo").WillReturnResult(pgxmock.NewResult("REINDEX", 0))

	if err := ReindexSpatial(context.Background(), mock); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestReindexSpatial_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	mock.ExpectExec("REINDEX SCHEMA geo").WillReturnError(errTest)

	if err := ReindexSpatial(context.Background(), mock); err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
