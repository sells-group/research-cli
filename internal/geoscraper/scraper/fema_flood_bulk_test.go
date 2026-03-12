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

func TestFEMAFloodBulk_Metadata(t *testing.T) {
	s := &FEMAFloodBulk{}
	assert.Equal(t, "fema_flood_bulk", s.Name())
	assert.Equal(t, "geo.flood_zones", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestFEMAFloodBulk_ShouldRun(t *testing.T) {
	s := &FEMAFloodBulk{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	stale := time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

func TestNewNFHLRow(t *testing.T) {
	wkb := []byte{0x01, 0x06, 0x00, 0x00, 0x20}
	raw := []any{"AE", "48001_AR01", "T", "FLOODWAY", "48001C", wkb}

	row, ok := newNFHLRow(raw)
	require.True(t, ok)
	assert.Equal(t, "AE", row[0])         // zone_code
	assert.Equal(t, "high_risk", row[1])  // flood_type
	assert.Equal(t, wkb, row[2])          // geom
	assert.Equal(t, femaSource, row[3])   // source
	assert.Equal(t, "48001_AR01", row[4]) // source_id
}

func TestNewNFHLRow_NilGeom(t *testing.T) {
	raw := []any{"AE", "48001_AR01", "T", "FLOODWAY", "48001C", nil}
	_, ok := newNFHLRow(raw)
	assert.False(t, ok)
}

func TestNewNFHLRow_ShortRow(t *testing.T) {
	raw := []any{"AE", "48001_AR01"}
	_, ok := newNFHLRow(raw)
	assert.False(t, ok)
}

func TestNewNFHLRow_EmptySourceID(t *testing.T) {
	wkb := []byte{0x01, 0x06}
	raw := []any{"X", "", "F", "", "48001C", wkb}
	row, ok := newNFHLRow(raw)
	require.True(t, ok)
	assert.Equal(t, "48001C_X", row[4]) // falls back to dfirm_id + zone
}

func TestFindNFHLShapefile(t *testing.T) {
	dir := t.TempDir()
	// Create a nested structure like NFHL ZIPs have.
	subDir := filepath.Join(dir, "Shape")
	require.NoError(t, os.MkdirAll(subDir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "S_Fld_Haz_Ar.shp"), []byte("shp"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "S_BFE.shp"), []byte("shp"), 0o644))

	path, err := findNFHLShapefile(dir)
	require.NoError(t, err)
	assert.Contains(t, path, "S_Fld_Haz_Ar.shp")
}

func TestFindNFHLShapefile_NotFound(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "other.shp"), []byte("shp"), 0o644))

	_, err := findNFHLShapefile(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no S_Fld_Haz_Ar")
}

func TestFEMAFloodBulk_Sync(t *testing.T) {
	// Create a shapefile named S_Fld_Haz_Ar.
	shpDir := t.TempDir()
	createNFHLShapefile(t, shpDir, []nfhlTestRecord{
		{zone: "AE", arID: "001", sfha: "T", subty: "", dfirm: "48001C"},
		{zone: "X", arID: "002", sfha: "F", subty: "0.2 PCT ANNUAL CHANCE", dfirm: "48001C"},
	})

	zipPath := filepath.Join(shpDir, "48453.zip")
	zipShapefile(t, zipPath, shpDir, "S_Fld_Haz_Ar")

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

	expectFloodBulkUpsert(mock, 2)

	s := &FEMAFloodBulk{downloadBaseURL: srv.URL, countyFIPS: []string{"48453"}}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFEMAFloodBulk_CountyDownloadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &FEMAFloodBulk{
		downloadBaseURL: srv.URL,
		countyFIPS:      []string{"48453"},
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced) // skipped county
}

func TestFEMAFloodBulk_UpsertError(t *testing.T) {
	shpDir := t.TempDir()
	createNFHLShapefile(t, shpDir, []nfhlTestRecord{
		{zone: "AE", arID: "001", sfha: "T", subty: "", dfirm: "48001C"},
	})
	zipPath := filepath.Join(shpDir, "48453.zip")
	zipShapefile(t, zipPath, shpDir, "S_Fld_Haz_Ar")

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

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &FEMAFloodBulk{downloadBaseURL: srv.URL, countyFIPS: []string{"48453"}}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestFEMAFloodBulk_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &FEMAFloodBulk{
		downloadBaseURL: "http://127.0.0.1:1",
		countyFIPS:      []string{"48453"},
	}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestFEMAFloodBulk_BuildURL_Default(t *testing.T) {
	s := &FEMAFloodBulk{}
	url := s.buildURL("48453")
	assert.Contains(t, url, "msc.fema.gov")
	assert.Contains(t, url, "NFHL_48453C")
}

func TestFEMAFloodBulk_BuildURL_Override(t *testing.T) {
	s := &FEMAFloodBulk{downloadBaseURL: "http://test.local"}
	url := s.buildURL("48453")
	assert.Equal(t, "http://test.local/48453.zip", url)
}

func TestFEMAFloodBulk_Sync_AllCountyFIPS(t *testing.T) {
	// When countyFIPS is empty, Sync queries the DB via allCountyFIPS.
	shpDir := t.TempDir()
	createNFHLShapefile(t, shpDir, []nfhlTestRecord{
		{zone: "AE", arID: "001", sfha: "T", subty: "", dfirm: "48001C"},
	})
	zipPath := filepath.Join(shpDir, "48453.zip")
	zipShapefile(t, zipPath, shpDir, "S_Fld_Haz_Ar")

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

	// Mock the allCountyFIPS query.
	mock.ExpectQuery("SELECT state_fips").
		WillReturnRows(pgxmock.NewRows([]string{"fips"}).AddRow("48453"))

	expectFloodBulkUpsert(mock, 1)

	s := &FEMAFloodBulk{downloadBaseURL: srv.URL}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFEMAFloodBulk_Sync_AllCountyFIPS_Error(t *testing.T) {
	// When countyFIPS is empty and DB query fails, Sync returns an error.
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT state_fips").
		WillReturnError(assert.AnError)

	s := &FEMAFloodBulk{}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "county FIPS")
}

func TestFEMAFloodBulk_SyncCounty_ShapefileNotFound(t *testing.T) {
	// Create a ZIP with a different shapefile name so findNFHLShapefile fails.
	shpDir := t.TempDir()
	createNFHLShapefile(t, shpDir, []nfhlTestRecord{
		{zone: "AE", arID: "001", sfha: "T", subty: "", dfirm: "48001C"},
	})

	// Rename the shapefile components so S_Fld_Haz_Ar is not found.
	for _, ext := range []string{".shp", ".shx", ".dbf"} {
		old := filepath.Join(shpDir, "S_Fld_Haz_Ar"+ext)
		renamed := filepath.Join(shpDir, "S_Other_Layer"+ext)
		require.NoError(t, os.Rename(old, renamed))
	}

	zipPath := filepath.Join(shpDir, "48453.zip")
	zipShapefile(t, zipPath, shpDir, "S_Other_Layer")

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

	// syncCounty fails on findNFHLShapefile → county is skipped, 0 rows.
	s := &FEMAFloodBulk{downloadBaseURL: srv.URL, countyFIPS: []string{"48453"}}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFEMAFloodBulk_SyncCounty_CorruptZip(t *testing.T) {
	// Serve a corrupt ZIP that extracts but has no valid shapefile.
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "48453.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	// Create a fake .shp file with corrupt content so findNFHLShapefile finds it
	// but tiger.ParseShapefile fails.
	fw, err := zw.Create("S_Fld_Haz_Ar.shp")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("not a real shapefile"))
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, rErr := os.ReadFile(zipPath)
		if rErr != nil {
			http.Error(w, rErr.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// syncCounty fails on parse → county is skipped, 0 rows.
	s := &FEMAFloodBulk{downloadBaseURL: srv.URL, countyFIPS: []string{"48453"}}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFEMAFloodBulk_SyncCounty_ExtractError(t *testing.T) {
	// Serve a corrupt non-ZIP file so ExtractZIP fails in syncCounty.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a zip file at all"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// syncCounty fails on extract → county is skipped, 0 rows.
	s := &FEMAFloodBulk{downloadBaseURL: srv.URL, countyFIPS: []string{"48453"}}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestFindNFHLShapefile_EmptyDir(t *testing.T) {
	// Empty directory should return "no S_Fld_Haz_Ar.shp" error.
	dir := t.TempDir()
	_, err := findNFHLShapefile(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no S_Fld_Haz_Ar.shp")
}

// ---------- Helpers ----------

type nfhlTestRecord struct {
	zone  string
	arID  string
	sfha  string
	subty string
	dfirm string
}

func createNFHLShapefile(t *testing.T, dir string, records []nfhlTestRecord) {
	t.Helper()
	shpPath := filepath.Join(dir, "S_Fld_Haz_Ar.shp")

	shape, err := shp.Create(shpPath, shp.POLYGON)
	require.NoError(t, err)

	err = shape.SetFields([]shp.Field{
		shp.StringField("FLD_ZONE", 20),
		shp.StringField("FLD_AR_ID", 50),
		shp.StringField("SFHA_TF", 5),
		shp.StringField("ZONE_SUBTY", 100),
		shp.StringField("DFIRM_ID", 20),
	})
	require.NoError(t, err)

	for _, rec := range records {
		points := []shp.Point{
			{X: -97.0, Y: 30.0},
			{X: -97.0, Y: 30.1},
			{X: -96.9, Y: 30.1},
			{X: -96.9, Y: 30.0},
			{X: -97.0, Y: 30.0},
		}
		poly := &shp.Polygon{
			Box:       shp.BBoxFromPoints(points),
			NumParts:  1,
			NumPoints: int32(len(points)),
			Parts:     []int32{0},
			Points:    points,
		}
		idx := shape.Write(poly)
		require.NoError(t, shape.WriteAttribute(int(idx), 0, rec.zone))
		require.NoError(t, shape.WriteAttribute(int(idx), 1, rec.arID))
		require.NoError(t, shape.WriteAttribute(int(idx), 2, rec.sfha))
		require.NoError(t, shape.WriteAttribute(int(idx), 3, rec.subty))
		require.NoError(t, shape.WriteAttribute(int(idx), 4, rec.dfirm))
	}

	shape.Close()
	fixShpDBF(t, dir, "S_Fld_Haz_Ar")
}

func expectFloodBulkUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_flood_zones"}, floodBulkCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
