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

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// ---------- Metadata ----------

func TestNRCSSoils_Metadata(t *testing.T) {
	s := &NRCSSoils{}
	assert.Equal(t, "nrcs_soils", s.Name())
	assert.Equal(t, "geo.soils", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

// ---------- ShouldRun ----------

func TestNRCSSoils_ShouldRun(t *testing.T) {
	s := &NRCSSoils{}
	now := fixedNow() // 2026-03-01 12:00 UTC

	// Never synced -> should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently (after January 1 of this year) -> should not run.
	recent := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	// Synced last year (before January 1 of this year) -> should run.
	stale := time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

// ---------- newSoilRow ----------

func TestNewSoilRow(t *testing.T) {
	wkb := []byte{0x01, 0x06, 0x00, 0x00, 0x20, 0xE6, 0x10, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}

	shpRow := []any{"42", "660845", "Oshtemo sandy loam", "Well drained", "No", wkb}
	row, ok := newSoilRow(shpRow)
	require.True(t, ok)
	require.Len(t, row, 8)

	assert.Equal(t, "660845", row[0])             // mukey
	assert.Equal(t, "Oshtemo sandy loam", row[1]) // muname
	assert.Equal(t, "Well drained", row[2])       // drainage_class
	assert.Equal(t, "No", row[3])                 // hydric_rating
	assert.Equal(t, wkb, row[4])                  // geom
	assert.Equal(t, nrcsSource, row[5])           // source
	assert.Equal(t, "nrcs/660845", row[6])        // source_id
	assert.NotNil(t, row[7])                      // properties
}

func TestNewSoilRow_NilGeom(t *testing.T) {
	shpRow := []any{"42", "660845", "Oshtemo", "Well drained", "No", nil}
	_, ok := newSoilRow(shpRow)
	assert.False(t, ok)
}

func TestNewSoilRow_EmptyMukey(t *testing.T) {
	wkb := []byte{0x01, 0x06}
	shpRow := []any{"42", nil, "Oshtemo", "Well drained", "No", wkb}
	_, ok := newSoilRow(shpRow)
	assert.False(t, ok)
}

func TestNewSoilRow_ShortRow(t *testing.T) {
	shpRow := []any{"42", "660845"}
	_, ok := newSoilRow(shpRow)
	assert.False(t, ok)
}

// ---------- nrcsURL ----------

func TestNRCSURL_Override(t *testing.T) {
	got := nrcsURL("http://test.local/soils.zip")
	assert.Equal(t, "http://test.local/soils.zip", got)
}

func TestNRCSURL_Default(t *testing.T) {
	got := nrcsURL("")
	assert.Contains(t, got, "nrcs.app.box.com")
}

// ---------- Sync ----------

func TestNRCSSoils_Sync(t *testing.T) {
	// Create test shapefile with 2 polygon features.
	shpDir := t.TempDir()
	createNRCSShapefile(t, shpDir, "gsmsoilmu_a_us", []nrcsTestRecord{
		{objectID: "1", mukey: "660845", muname: "Oshtemo sandy loam", drclassdcd: "Well drained", hydricRating: "No"},
		{objectID: "2", mukey: "660846", muname: "Spinks loamy sand", drclassdcd: "Somewhat excessively", hydricRating: "No"},
	})

	// ZIP the shapefile.
	zipPath := filepath.Join(shpDir, "gsmsoilmu_a_us.zip")
	zipShapefile(t, zipPath, shpDir, "gsmsoilmu_a_us")

	// Serve the ZIP.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectSoilUpsert(mock, 2)

	s := &NRCSSoils{downloadURL: srv.URL + "/gsmsoilmu_a_us.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNRCSSoils_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &NRCSSoils{downloadURL: "http://127.0.0.1:1/soils.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nrcs_soils: download")
}

func TestNRCSSoils_UpsertError(t *testing.T) {
	shpDir := t.TempDir()
	createNRCSShapefile(t, shpDir, "gsmsoilmu_a_us", []nrcsTestRecord{
		{objectID: "1", mukey: "660845", muname: "Oshtemo", drclassdcd: "Well drained", hydricRating: "No"},
	})
	zipPath := filepath.Join(shpDir, "gsmsoilmu_a_us.zip")
	zipShapefile(t, zipPath, shpDir, "gsmsoilmu_a_us")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// BulkUpsert fails at Begin.
	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &NRCSSoils{downloadURL: srv.URL + "/soils.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestNRCSSoils_EmptyShapefile(t *testing.T) {
	shpDir := t.TempDir()
	createNRCSShapefile(t, shpDir, "gsmsoilmu_a_us", nil) // no records
	zipPath := filepath.Join(shpDir, "gsmsoilmu_a_us.zip")
	zipShapefile(t, zipPath, shpDir, "gsmsoilmu_a_us")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &NRCSSoils{downloadURL: srv.URL + "/soils.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestNRCSSoils_ExtractError(t *testing.T) {
	dir := t.TempDir()
	corruptPath := filepath.Join(dir, "corrupt.zip")
	require.NoError(t, os.WriteFile(corruptPath, []byte("not a zip"), 0o644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, _ := os.ReadFile(corruptPath)
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &NRCSSoils{downloadURL: srv.URL + "/soils.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestNRCSSoils_NoShpInZip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "noshp.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	fw, _ := zw.Create("readme.txt")
	_, _ = fw.Write([]byte("no shp"))
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, _ := os.ReadFile(zipPath)
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &NRCSSoils{downloadURL: srv.URL + "/soils.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "find .shp")
}

// ---------- Helpers ----------

type nrcsTestRecord struct {
	objectID     string
	mukey        string
	muname       string
	drclassdcd   string
	hydricRating string
}

// createNRCSShapefile creates a shapefile with polygon features for NRCS testing.
func createNRCSShapefile(t *testing.T, dir, name string, records []nrcsTestRecord) {
	t.Helper()
	shpPath := filepath.Join(dir, name+".shp")

	shape, err := shp.Create(shpPath, shp.POLYGON)
	require.NoError(t, err)

	err = shape.SetFields([]shp.Field{
		shp.StringField("OBJECTID", 10),
		shp.StringField("MUKEY", 20),
		shp.StringField("MUNAME", 50),
		shp.StringField("DRCLASSDCD", 30),
		shp.StringField("HYDRICRATI", 10),
	})
	require.NoError(t, err)

	for _, rec := range records {
		points := []shp.Point{
			{X: -95.4, Y: 29.7},
			{X: -95.3, Y: 29.7},
			{X: -95.3, Y: 29.8},
			{X: -95.4, Y: 29.8},
			{X: -95.4, Y: 29.7},
		}
		poly := &shp.Polygon{
			Box:       shp.BBoxFromPoints(points),
			NumParts:  1,
			NumPoints: int32(len(points)),
			Parts:     []int32{0},
			Points:    points,
		}
		idx := shape.Write(poly)
		require.NoError(t, shape.WriteAttribute(int(idx), 0, rec.objectID))
		require.NoError(t, shape.WriteAttribute(int(idx), 1, rec.mukey))
		require.NoError(t, shape.WriteAttribute(int(idx), 2, rec.muname))
		require.NoError(t, shape.WriteAttribute(int(idx), 3, rec.drclassdcd))
		require.NoError(t, shape.WriteAttribute(int(idx), 4, rec.hydricRating))
	}

	shape.Close()
	fixShpDBF(t, dir, name)
}

// expectSoilUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.soils table.
func expectSoilUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_soils"}, soilCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
