package scraper

import (
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

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestTIGERRoads_Metadata(t *testing.T) {
	s := &TIGERRoads{}
	assert.Equal(t, "tiger_roads", s.Name())
	assert.Equal(t, "geo.roads", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestTIGERRoads_ShouldRun(t *testing.T) {
	s := &TIGERRoads{}

	// Never synced → should run.
	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	// Use a date after October to test annual logic properly.
	nowNov := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	// Synced after October 1 of this year → should not run.
	recent := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(nowNov, &recent))

	// Synced before October of this year → should run.
	stale := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(nowNov, &stale))
}

func TestNewRoadRow(t *testing.T) {
	raw := []any{
		"I- 35",            // fullname
		"S1100",            // mtfcc
		"1104486",          // linearid
		[]byte{0x01, 0x02}, // wkb
	}

	row := newRoadRow(raw)
	assert.Equal(t, "I- 35", row[0])         // name
	assert.Equal(t, "interstate", row[1])    // route_type
	assert.Equal(t, "S1100", row[2])         // mtfcc
	assert.Equal(t, raw[3], row[3])          // geom
	assert.Equal(t, tigerGeoSource, row[4])  // source
	assert.Equal(t, "tiger/1104486", row[5]) // source_id
}

func TestTIGERRoads_Sync(t *testing.T) {
	zipPath := createTestRoadShapefile(t, 3)

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

	expectRoadUpsert(mock, 3)

	s := &TIGERRoads{downloadURL: srv.URL + "/tl_2024_us_primaryroads.zip", year: 2024}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTIGERRoads_DownloadError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &TIGERRoads{downloadURL: "http://127.0.0.1:1/bad"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tiger_roads: download")
}

func TestTIGERRoads_EmptyShapefile(t *testing.T) {
	zipPath := createTestRoadShapefile(t, 0)

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

	s := &TIGERRoads{downloadURL: srv.URL + "/tl_2024_us_primaryroads.zip", year: 2024}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestTIGERRoads_BuildURL_Default(t *testing.T) {
	s := &TIGERRoads{year: 2024}
	url := s.buildURL(2024)
	assert.Contains(t, url, "census.gov")
	assert.Contains(t, url, "tl_2024_us_primaryroads.zip")
}

func TestTIGERRoads_EffectiveYear_Default(t *testing.T) {
	s := &TIGERRoads{} // year = 0 → uses tigerYear
	assert.Equal(t, tigerYear, s.effectiveYear())
}

func TestTIGERRoads_UpsertError(t *testing.T) {
	zipPath := createTestRoadShapefile(t, 1)
	srv := serveFile(t, zipPath)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &TIGERRoads{downloadURL: srv.URL + "/tl_2024_us_primaryroads.zip", year: 2024}
	_, err = s.Sync(context.Background(), mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestTIGERRoads_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &TIGERRoads{downloadURL: "http://127.0.0.1:1/bad"}
	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

// ---------- Helpers ----------

// expectRoadUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.roads table.
func expectRoadUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_roads"}, roadCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

// createTestRoadShapefile creates a polyline shapefile with n features and
// road-appropriate columns, zips it, and returns the path to the ZIP file.
func createTestRoadShapefile(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	shpPath := filepath.Join(dir, "roads.shp")

	fields := []shp.Field{
		shp.StringField("FULLNAME", 100),
		shp.StringField("MTFCC", 10),
		shp.StringField("LINEARID", 22),
	}

	w, err := shp.Create(shpPath, shp.POLYLINE)
	require.NoError(t, err)
	require.NoError(t, w.SetFields(fields))

	mtfccs := []string{"S1100", "S1200", "S1300"}
	for i := 0; i < n; i++ {
		points := []shp.Point{
			{X: -98.0 + float64(i)*0.1, Y: 30.0},
			{X: -97.0 + float64(i)*0.1, Y: 30.5},
			{X: -96.0 + float64(i)*0.1, Y: 31.0},
		}
		pl := &shp.PolyLine{
			Box:       shp.BBoxFromPoints(points),
			NumParts:  1,
			NumPoints: int32(len(points)),
			Parts:     []int32{0},
			Points:    points,
		}
		idx := w.Write(pl)
		require.NoError(t, w.WriteAttribute(int(idx), 0, "Test Road "+padLeft(i+1, 2)))
		require.NoError(t, w.WriteAttribute(int(idx), 1, mtfccs[i%len(mtfccs)]))
		require.NoError(t, w.WriteAttribute(int(idx), 2, "110"+padLeft(i+1, 4)))
	}
	w.Close()
	fixShpDBF(t, dir, "roads")

	zipPath := filepath.Join(dir, "roads.zip")
	zipShapefile(t, zipPath, dir, "roads")
	return zipPath
}
