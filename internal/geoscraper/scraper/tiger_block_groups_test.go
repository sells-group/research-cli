package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jonas-p/go-shp"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestTIGERBlockGroups_Metadata(t *testing.T) {
	s := &TIGERBlockGroups{}
	assert.Equal(t, "tiger_block_groups", s.Name())
	assert.Equal(t, "geo.block_groups", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestTIGERBlockGroups_ShouldRun(t *testing.T) {
	s := &TIGERBlockGroups{}

	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	nowNov := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(nowNov, &recent))

	stale := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(nowNov, &stale))
}

func TestNewBlockGroupRow(t *testing.T) {
	raw := []any{
		"48",               // statefp
		"453",              // countyfp
		"002100",           // tractce
		"1",                // blkgrpce
		"484530021001",     // geoid
		"Block Group 1",    // namelsad
		"G5030",            // mtfcc
		"S",                // funcstat
		"1000000",          // aland
		"50000",            // awater
		"30.29",            // intptlat
		"-97.74",           // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newBlockGroupRow(raw)
	assert.Equal(t, "484530021001", row[0]) // geoid
	assert.Equal(t, "48", row[1])           // state_fips
	assert.Equal(t, "453", row[2])          // county_fips
	assert.Equal(t, "002100", row[3])       // tract_ce
	assert.Equal(t, "1", row[4])            // blkgrp_ce
	assert.Equal(t, raw[12], row[5])        // geom
	assert.InDelta(t, 30.29, row[6], 0.01)
	assert.InDelta(t, -97.74, row[7], 0.01)
	assert.Equal(t, tigerGeoSource, row[8])
	assert.Equal(t, "tiger/484530021001", row[9])
}

func TestTIGERBlockGroups_Sync(t *testing.T) {
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, blockGroupProduct.Columns, 2)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// 51 states × 2 rows each = 51 upsert calls.
	for range 2 {
		expectBlockGroupUpsert(mock, 2)
	}

	s := &TIGERBlockGroups{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48", "06"}}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTIGERBlockGroups_DownloadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Per-state scraper skips failing states; all fail → 0 rows.
	s := &TIGERBlockGroups{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48"}}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestTIGERBlockGroups_UpsertError(t *testing.T) {
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, blockGroupProduct.Columns, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &TIGERBlockGroups{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48"}}
	_, err = s.Sync(context.Background(), mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestTIGERBlockGroups_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &TIGERBlockGroups{downloadBaseURL: "http://127.0.0.1:1", year: 2024, stateFIPS: []string{"48"}}
	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

func TestTIGERBlockGroups_ParseError(t *testing.T) {
	// Serve a valid ZIP containing a corrupt .shp file.
	zipPath := createCorruptShapefileZIP(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Parse error is non-fatal for per-state scrapers — state is skipped, 0 rows.
	s := &TIGERBlockGroups{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48"}}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func expectBlockGroupUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_block_groups"}, blockGroupCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
