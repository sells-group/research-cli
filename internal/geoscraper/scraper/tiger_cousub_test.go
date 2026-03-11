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

func TestTIGERCousub_Metadata(t *testing.T) {
	s := &TIGERCousub{}
	assert.Equal(t, "tiger_cousub", s.Name())
	assert.Equal(t, "geo.county_subdivisions", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestTIGERCousub_ShouldRun(t *testing.T) {
	s := &TIGERCousub{}

	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	nowNov := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(nowNov, &recent))

	stale := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(nowNov, &stale))
}

func TestNewCousubRow(t *testing.T) {
	raw := []any{
		"48",               // statefp
		"453",              // countyfp
		"90000",            // cousubfp
		"01234567",         // cousubns
		"4845390000",       // geoid
		"Austin CCD",       // name
		"Austin CCD",       // namelsad
		"21",               // lsad
		"Z5",               // classfp
		"G4040",            // mtfcc
		"S",                // funcstat
		"5000000",          // aland
		"100000",           // awater
		"30.29",            // intptlat
		"-97.74",           // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newCousubRow(raw)
	assert.Equal(t, "4845390000", row[0]) // geoid
	assert.Equal(t, "48", row[1])         // state_fips
	assert.Equal(t, "453", row[2])        // county_fips
	assert.Equal(t, "90000", row[3])      // cousub_fips
	assert.Equal(t, "Austin CCD", row[4]) // name
	assert.Equal(t, "21", row[5])         // lsad
	assert.Equal(t, "Z5", row[6])         // class_fips
	assert.Equal(t, raw[15], row[7])      // geom
	assert.InDelta(t, 30.29, row[8], 0.01)
	assert.InDelta(t, -97.74, row[9], 0.01)
	assert.Equal(t, tigerGeoSource, row[10])
	assert.Equal(t, "tiger/4845390000", row[11])
}

func TestTIGERCousub_Sync(t *testing.T) {
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, cousubProduct.Columns, 2)

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

	for range 2 {
		expectCousubUpsert(mock, 2)
	}

	s := &TIGERCousub{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48", "06"}}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTIGERCousub_DownloadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &TIGERCousub{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48"}}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestTIGERCousub_UpsertError(t *testing.T) {
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, cousubProduct.Columns, 1)

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

	s := &TIGERCousub{downloadBaseURL: srv.URL, year: 2024, stateFIPS: []string{"48"}}
	_, err = s.Sync(context.Background(), mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestTIGERCousub_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &TIGERCousub{downloadBaseURL: "http://127.0.0.1:1", year: 2024, stateFIPS: []string{"48"}}
	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

func expectCousubUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_county_subdivisions"}, cousubCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
