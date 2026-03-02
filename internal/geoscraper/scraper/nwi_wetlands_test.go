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

func TestNWIWetlands_Metadata(t *testing.T) {
	s := &NWIWetlands{}
	assert.Equal(t, "nwi_wetlands", s.Name())
	assert.Equal(t, "geo.wetlands", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

// ---------- ShouldRun ----------

func TestNWIWetlands_ShouldRun(t *testing.T) {
	s := &NWIWetlands{}
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

// ---------- classifyWetland ----------

func TestClassifyWetland(t *testing.T) {
	tests := []struct {
		attribute string
		want      string
	}{
		{"E2EM1P", "estuarine"},
		{"M1AB1L", "marine"},
		{"PEM1C", "palustrine"},
		{"L1UBH", "lacustrine"},
		{"R2UBH", "riverine"},
		{"X1234", "unknown"},
		{"", "unknown"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, classifyWetland(tt.attribute), "attribute=%q", tt.attribute)
	}
}

// ---------- newWetlandRow ----------

func TestNewWetlandRow(t *testing.T) {
	wkb := []byte{0x01, 0x06, 0x00, 0x00, 0x20, 0xE6, 0x10, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}

	shpRow := []any{"12345", "PEM1C", "Freshwater Emergent", "2.5", wkb}
	row, ok := newWetlandRow(shpRow)
	require.True(t, ok)
	require.Len(t, row, 7)

	assert.Equal(t, "palustrine", row[0])            // wetland_type
	assert.Equal(t, "PEM1C", row[1])                 // attribute
	assert.InDelta(t, 2.5, *row[2].(*float64), 0.01) // acres
	assert.Equal(t, wkb, row[3])                     // geom
	assert.Equal(t, nwiSource, row[4])               // source
	assert.Equal(t, "nwi/12345", row[5])             // source_id
	assert.NotNil(t, row[6])                         // properties
}

func TestNewWetlandRow_NilGeom(t *testing.T) {
	shpRow := []any{"12345", "PEM1C", "Freshwater", "2.5", nil}
	_, ok := newWetlandRow(shpRow)
	assert.False(t, ok)
}

func TestNewWetlandRow_ShortRow(t *testing.T) {
	shpRow := []any{"12345", "PEM1C"}
	_, ok := newWetlandRow(shpRow)
	assert.False(t, ok)
}

func TestNewWetlandRow_NilAcres(t *testing.T) {
	wkb := []byte{0x01, 0x06}
	shpRow := []any{"12345", "PEM1C", "Freshwater", nil, wkb}
	row, ok := newWetlandRow(shpRow)
	require.True(t, ok)
	assert.Nil(t, row[2]) // acres should be nil
}

// ---------- nwiDownloadURL ----------

func TestNWIDownloadURL_Override(t *testing.T) {
	got := nwiDownloadURL("http://test.local", "TX")
	assert.Equal(t, "http://test.local/TX_shapefile_wetlands.zip", got)
}

func TestNWIDownloadURL_Default(t *testing.T) {
	got := nwiDownloadURL("", "TX")
	assert.Contains(t, got, "fws.gov/wetlands")
	assert.Contains(t, got, "TX_shapefile_wetlands.zip")
}

// ---------- Sync ----------

func TestNWIWetlands_Sync(t *testing.T) {
	// Create test shapefile with 2 polygon features.
	shpDir := t.TempDir()
	createNWIShapefile(t, shpDir, "TX_shapefile_wetlands", []nwiTestRecord{
		{objectID: "1", attribute: "PEM1C", wetlandTy: "Freshwater Emergent", acres: "2.5"},
		{objectID: "2", attribute: "E2EM1P", wetlandTy: "Estuarine", acres: "10.0"},
	})

	// ZIP the shapefile.
	zipPath := filepath.Join(shpDir, "TX_shapefile_wetlands.zip")
	zipShapefile(t, zipPath, shpDir, "TX_shapefile_wetlands")

	// Serve the ZIP for any state request.
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

	// 56 states x 2 features each = 112 rows in a single batch.
	totalFeatures := int64(2 * len(stateAbbrevs))
	expectWetlandUpsert(mock, totalFeatures)

	s := &NWIWetlands{downloadBaseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalFeatures, result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNWIWetlands_StateDownloadError(t *testing.T) {
	// First request succeeds, rest fail. The scraper should skip failing states.
	shpDir := t.TempDir()
	createNWIShapefile(t, shpDir, "AL_shapefile_wetlands", []nwiTestRecord{
		{objectID: "1", attribute: "PEM1C", wetlandTy: "Freshwater", acres: "1.0"},
	})
	zipPath := filepath.Join(shpDir, "AL_shapefile_wetlands.zip")
	zipShapefile(t, zipPath, shpDir, "AL_shapefile_wetlands")

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if callCount == 0 {
			data, err := os.ReadFile(zipPath)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(data)
		} else {
			http.Error(w, "not found", 404)
		}
		callCount++
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Only 1 state succeeds with 1 feature.
	expectWetlandUpsert(mock, 1)

	s := &NWIWetlands{downloadBaseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNWIWetlands_UpsertError(t *testing.T) {
	shpDir := t.TempDir()
	createNWIShapefile(t, shpDir, "AL_shapefile_wetlands", []nwiTestRecord{
		{objectID: "1", attribute: "PEM1C", wetlandTy: "Freshwater", acres: "1.0"},
	})
	zipPath := filepath.Join(shpDir, "AL_shapefile_wetlands.zip")
	zipShapefile(t, zipPath, shpDir, "AL_shapefile_wetlands")

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

	s := &NWIWetlands{downloadBaseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestNWIWetlands_EmptyShapefile(t *testing.T) {
	shpDir := t.TempDir()
	createNWIShapefile(t, shpDir, "TX_shapefile_wetlands", nil) // no records
	zipPath := filepath.Join(shpDir, "TX_shapefile_wetlands.zip")
	zipShapefile(t, zipPath, shpDir, "TX_shapefile_wetlands")

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

	s := &NWIWetlands{downloadBaseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestNWIWetlands_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	s := &NWIWetlands{downloadBaseURL: "http://127.0.0.1:1"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestNWIWetlands_NoShpInZip(t *testing.T) {
	// Serve a ZIP with no .shp file → all states should skip (non-fatal).
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "bad.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	fw, err := zw.Create("readme.txt")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("no shapefile"))
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

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

	s := &NWIWetlands{downloadBaseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced) // all states skipped
}

func TestFindShpFile_NotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := findShpFile(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no .shp file")
}

func TestFindShpFile_Found(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.shp"), []byte("shp"), 0o644))
	path, err := findShpFile(dir)
	require.NoError(t, err)
	assert.Contains(t, path, "test.shp")
}

// ---------- Helpers ----------

type nwiTestRecord struct {
	objectID  string
	attribute string
	wetlandTy string
	acres     string
}

// createNWIShapefile creates a shapefile with polygon features for testing.
func createNWIShapefile(t *testing.T, dir, name string, records []nwiTestRecord) {
	t.Helper()
	shpPath := filepath.Join(dir, name+".shp")

	shape, err := shp.Create(shpPath, shp.POLYGON)
	require.NoError(t, err)

	err = shape.SetFields([]shp.Field{
		shp.StringField("OBJECTID", 10),
		shp.StringField("ATTRIBUTE", 20),
		shp.StringField("WETLAND_TY", 30),
		shp.StringField("ACRES", 15),
	})
	require.NoError(t, err)

	for _, rec := range records {
		// Simple square polygon.
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
		require.NoError(t, shape.WriteAttribute(int(idx), 1, rec.attribute))
		require.NoError(t, shape.WriteAttribute(int(idx), 2, rec.wetlandTy))
		require.NoError(t, shape.WriteAttribute(int(idx), 3, rec.acres))
	}

	shape.Close()
	fixShpDBF(t, dir, name)
}

// fixShpDBF renames the go-shp DBF file from "{name}dbf" to "{name}.dbf".
// go-shp Create strips ".shp" (4 chars) but Open strips only 3 chars,
// causing a naming mismatch for the DBF file.
func fixShpDBF(t *testing.T, dir, name string) {
	t.Helper()
	wrongDBF := filepath.Join(dir, name+"dbf")
	correctDBF := filepath.Join(dir, name+".dbf")
	if _, err := os.Stat(wrongDBF); err == nil {
		require.NoError(t, os.Rename(wrongDBF, correctDBF))
	}
}

// zipShapefile creates a ZIP archive containing the shapefile components.
func zipShapefile(t *testing.T, zipPath, srcDir, baseName string) {
	t.Helper()
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck

	w := zip.NewWriter(f)
	defer w.Close() //nolint:errcheck

	exts := []string{".shp", ".shx", ".dbf"}
	for _, ext := range exts {
		srcPath := filepath.Join(srcDir, baseName+ext)
		data, err := os.ReadFile(srcPath)
		if err != nil {
			continue // file may not exist
		}
		fw, err := w.Create(baseName + ext)
		require.NoError(t, err)
		_, err = fw.Write(data)
		require.NoError(t, err)
	}
}

// expectWetlandUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.wetlands table.
func expectWetlandUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_wetlands"}, wetlandCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
