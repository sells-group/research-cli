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

func TestTIGERWater_Metadata(t *testing.T) {
	s := &TIGERWater{}
	assert.Equal(t, "tiger_water", s.Name())
	assert.Equal(t, "geo.water_features", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestTIGERWater_ShouldRun(t *testing.T) {
	s := &TIGERWater{}

	now := fixedNow()
	assert.True(t, s.ShouldRun(now, nil))

	nowNov := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(nowNov, &recent))
}

func TestClassifyWater(t *testing.T) {
	tests := []struct {
		mtfcc string
		want  string
	}{
		{"H2030", "lake"},
		{"H2040", "lake"},
		{"H2051", "reservoir"},
		{"H3010", "stream"},
		{"H2025", "swamp"},
		{"H1100", "ocean"},
		{"H9999", "water"},
		{"", "water"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, classifyWater(tt.mtfcc), "mtfcc=%s", tt.mtfcc)
	}
}

func TestNewAreaWaterRow(t *testing.T) {
	raw := []any{
		"01234567",         // ansicode
		"1100",             // hydroid
		"Lake Travis",      // fullname
		"H2030",            // mtfcc
		"5000000",          // aland
		"100000",           // awater
		"30.40",            // intptlat
		"-97.90",           // intptlon
		[]byte{0x01, 0x02}, // wkb
	}

	row := newAreaWaterRow(raw)
	assert.Equal(t, "Lake Travis", row[0]) // name
	assert.Equal(t, "lake", row[1])        // water_type
	assert.Equal(t, "H2030", row[2])       // mtfcc
	assert.Equal(t, raw[8], row[3])        // geom
	assert.Equal(t, tigerGeoSource, row[6])
	assert.Equal(t, "tiger/aw/1100", row[7]) // source_id
}

func TestNewLinearWaterRow(t *testing.T) {
	raw := []any{
		"01234567",         // ansicode
		"1100456",          // linearid
		"Colorado River",   // fullname
		"H3010",            // mtfcc
		[]byte{0x01, 0x02}, // wkb
	}

	row := newLinearWaterRow(raw)
	assert.Equal(t, "Colorado River", row[0]) // name
	assert.Equal(t, "stream", row[1])         // water_type
	assert.Equal(t, "H3010", row[2])          // mtfcc
	assert.Equal(t, raw[4], row[3])           // geom
	assert.Equal(t, tigerGeoSource, row[6])
	assert.Equal(t, "tiger/lw/1100456", row[7]) // source_id
}

func TestTIGERWater_Sync(t *testing.T) {
	// Create area water shapefile.
	areaZipPath := createTestBoundaryShapefile(t, shp.POLYGON, areaWaterProduct.Columns, 2)
	// Create linear water shapefile.
	linearZipPath := createTestWaterShapefile(t, shp.POLYLINE, linearWaterProduct.Columns, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data []byte
		var err error
		if filepath.Base(r.URL.Path) != "" {
			base := filepath.Base(r.URL.Path)
			if len(base) > 10 && base[len(base)-14:] == "_areawater.zip" {
				data, err = os.ReadFile(areaZipPath)
			} else {
				data, err = os.ReadFile(linearZipPath)
			}
		} else {
			data, err = os.ReadFile(areaZipPath)
		}
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

	// 2 counties × (2 area + 1 linear) = 6 rows total in a single batch.
	expectWaterUpsert(mock, 6)

	s := &TIGERWater{
		downloadBaseURL: srv.URL,
		year:            2024,
		countyFIPS:      []string{"48453", "48491"},
	}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(6), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTIGERWater_DownloadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &TIGERWater{
		downloadBaseURL: srv.URL,
		year:            2024,
		countyFIPS:      []string{"48453"},
	}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestTIGERWater_UpsertError(t *testing.T) {
	zipPath := createTestBoundaryShapefile(t, shp.POLYGON, areaWaterProduct.Columns, 1)

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

	s := &TIGERWater{
		downloadBaseURL: srv.URL,
		year:            2024,
		countyFIPS:      []string{"48453"},
	}
	_, err = s.Sync(context.Background(), mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestTIGERWater_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &TIGERWater{
		downloadBaseURL: "http://127.0.0.1:1",
		year:            2024,
		countyFIPS:      []string{"48453"},
	}
	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

func expectWaterUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_water_features"}, waterCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}

// createTestWaterShapefile creates a polyline shapefile for testing water features.
func createTestWaterShapefile(t *testing.T, shpType shp.ShapeType, columns []string, n int) string {
	t.Helper()
	dir := t.TempDir()
	shpPath := filepath.Join(dir, "test.shp")

	fields := make([]shp.Field, len(columns))
	for i, col := range columns {
		fields[i] = shp.StringField(col, 50)
	}

	w, err := shp.Create(shpPath, shpType)
	require.NoError(t, err)
	require.NoError(t, w.SetFields(fields))

	for i := 0; i < n; i++ {
		points := []shp.Point{
			{X: -98.0 + float64(i)*0.1, Y: 30.0},
			{X: -97.0 + float64(i)*0.1, Y: 30.5},
		}
		pl := &shp.PolyLine{
			Box:       shp.BBoxFromPoints(points),
			NumParts:  1,
			NumPoints: int32(len(points)),
			Parts:     []int32{0},
			Points:    points,
		}
		idx := w.Write(pl)
		for j := range columns {
			require.NoError(t, w.WriteAttribute(int(idx), j, testBoundaryValue(columns[j], i)))
		}
	}
	w.Close()
	fixShpDBF(t, dir, "test")

	zipPath := filepath.Join(dir, "test.zip")
	zipShapefile(t, zipPath, dir, "test")
	return zipPath
}
