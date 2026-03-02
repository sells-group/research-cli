package scraper

import (
	"archive/zip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jonas-p/go-shp"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestFCCTowers_Metadata(t *testing.T) {
	s := &FCCTowers{}
	assert.Equal(t, "fcc_towers", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestFCCTowers_ShouldRun(t *testing.T) {
	s := &FCCTowers{}
	now := fixedNow() // 2026-03-01 12:00 UTC

	// Never synced → should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced after March 1 of this year → should not run.
	recent := time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	// Synced last year → should run.
	stale := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

func TestFCCTowers_NewTowerRow(t *testing.T) {
	// Create WKB point bytes at lon=-97.74, lat=30.27 using go-geom.
	pt := geom.NewPointFlat(geom.XY, []float64{-97.74, 30.27}).SetSRID(4326)
	wkb, err := ewkb.Marshal(pt, ewkb.NDR)
	require.NoError(t, err)

	shpRow := []any{"12345", "AT&T Mobility", "LOC001", "150.5", wkb}

	row, ok := newTowerRow(shpRow)
	require.True(t, ok)
	require.Len(t, row, 9)

	assert.Equal(t, "AT&T Mobility", row[0])          // name
	assert.Equal(t, "telecom_tower", row[1])          // type
	assert.Nil(t, row[2])                             // fuel_type
	assert.InDelta(t, 150.5, row[3].(float64), 0.001) // capacity (height)
	assert.InDelta(t, 30.27, row[4].(float64), 0.001) // latitude
	assert.InDelta(t, -97.74, row[5].(float64), 0.01) // longitude
	assert.Equal(t, fccSource, row[6])                // source
	assert.Equal(t, "fcc_tower/12345", row[7])        // source_id
}

func TestFCCTowers_NewTowerRow_NilGeom(t *testing.T) {
	// Missing WKB bytes.
	shpRow := []any{"12345", "AT&T", "LOC001", "150.5", nil}
	_, ok := newTowerRow(shpRow)
	assert.False(t, ok)

	// Too short.
	_, ok = newTowerRow([]any{"12345", "AT&T"})
	assert.False(t, ok)

	// Non-byte WKB.
	_, ok = newTowerRow([]any{"12345", "AT&T", "LOC001", "150.5", "not bytes"})
	assert.False(t, ok)
}

func TestFCCTowers_NewTowerRow_InvalidWKB(t *testing.T) {
	shpRow := []any{"12345", "AT&T", "LOC001", "150.5", []byte{0xFF, 0xFF}}
	_, ok := newTowerRow(shpRow)
	assert.False(t, ok)
}

func TestFCCTowers_NewTowerRow_NilFields(t *testing.T) {
	// Test with nil licensee and strucheigh.
	pt := geom.NewPointFlat(geom.XY, []float64{-97.74, 30.27}).SetSRID(4326)
	wkb, err := ewkb.Marshal(pt, ewkb.NDR)
	require.NoError(t, err)

	shpRow := []any{"12345", nil, nil, nil, wkb}
	row, ok := newTowerRow(shpRow)
	require.True(t, ok)
	assert.Equal(t, "", row[0])  // name defaults to empty
	assert.Equal(t, 0.0, row[3]) // height defaults to 0
}

func TestFCCTowers_Sync(t *testing.T) {
	// Create a minimal shapefile with 2 point features.
	zipPath := createTestTowerShapefile(t, 2)

	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectInfraUpsert(mock, 2)

	s := &FCCTowers{downloadURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFCCTowers_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCTowers{downloadURL: "http://127.0.0.1:1/bad"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fcc_towers: download")
}

func TestFCCTowers_EmptyShapefile(t *testing.T) {
	zipPath := createTestTowerShapefile(t, 0)

	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCTowers{downloadURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFCCTowers_UpsertError(t *testing.T) {
	zipPath := createTestTowerShapefile(t, 1)
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FCCTowers{downloadURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFCCTowers_ExtractError(t *testing.T) {
	dir := t.TempDir()
	corruptPath := filepath.Join(dir, "corrupt.zip")
	require.NoError(t, os.WriteFile(corruptPath, []byte("not a zip"), 0o644))
	srv := serveFile(t, corruptPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCTowers{downloadURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestFCCTowers_NoShpInZip(t *testing.T) {
	// Create a ZIP with no .shp file.
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "bad.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	fw, err := zw.Create("readme.txt")
	require.NoError(t, err)
	_, err = fw.Write([]byte("no shp here"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FCCTowers{downloadURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no .shp file")
}

// ---------- Helpers ----------

// expectInfraUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.infrastructure table.
func expectInfraUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_infrastructure"}, infraCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

// createTestTowerShapefile creates a shapefile with n point features, zips it,
// and returns the path to the ZIP file.
func createTestTowerShapefile(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	shpPath := filepath.Join(dir, "towers.shp")

	fields := []shp.Field{
		shp.StringField("OBJECTID", 10),
		shp.StringField("LICENSEE", 50),
		shp.StringField("LOCID", 20),
		shp.StringField("STRUCHEIGH", 10),
	}

	w, err := shp.Create(shpPath, shp.POINT)
	require.NoError(t, err)

	require.NoError(t, w.SetFields(fields))

	for i := 0; i < n; i++ {
		idx := w.Write(&shp.Point{X: -97.74 + float64(i)*0.01, Y: 30.27 + float64(i)*0.01})
		_ = w.WriteAttribute(int(idx), 0, i+1)
		_ = w.WriteAttribute(int(idx), 1, "TestCarrier")
		_ = w.WriteAttribute(int(idx), 2, "LOC"+string(rune('A'+i)))
		_ = w.WriteAttribute(int(idx), 3, "100.5")
	}
	w.Close()

	// go-shp creates DBF as "towersdbf" (no dot); rename to "towers.dbf"
	// so that shp.Open (which expects "towers.dbf") can find it.
	err = os.Rename(filepath.Join(dir, "towersdbf"), filepath.Join(dir, "towers.dbf"))
	require.NoError(t, err)

	// ZIP the shapefile components (.shp, .shx, .dbf).
	zipPath := filepath.Join(dir, "towers.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)

	for _, ext := range []string{".shp", ".shx", ".dbf"} {
		base := "towers" + ext
		data, err := os.ReadFile(filepath.Join(dir, base))
		require.NoError(t, err)
		fw, err := zw.Create(base)
		require.NoError(t, err)
		_, err = fw.Write(data)
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	return zipPath
}

// serveFile starts an httptest server that serves a local file at any path.
// The server is automatically closed when the test finishes.
func serveFile(t *testing.T, filePath string) *httptest.Server {
	t.Helper()
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)
	return srv
}
