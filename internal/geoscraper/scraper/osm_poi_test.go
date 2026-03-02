package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geoscraper/overpass"
)

const testOSMOverpassJSON = `{
	"elements": [
		{"type": "node", "id": 123, "lat": 40.7128, "lon": -74.006, "tags": {"amenity": "school", "name": "PS 101"}},
		{"type": "node", "id": 456, "lat": 40.7580, "lon": -73.9855, "tags": {"amenity": "hospital", "name": "NYC Hospital"}},
		{"type": "node", "id": 789, "lat": 40.7484, "lon": -73.9857, "tags": {"leisure": "park", "name": "Bryant Park"}}
	]
}`

func TestOSMPOI_Metadata(t *testing.T) {
	s := &OSMPOI{}
	assert.Equal(t, "osm_poi", s.Name())
	assert.Equal(t, "geo.poi", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Monthly, s.Cadence())
}

func TestOSMPOI_ShouldRun(t *testing.T) {
	s := &OSMPOI{}
	now := fixedNow()

	// Never synced -> should run.
	assert.True(t, s.ShouldRun(now, nil))

	// Synced recently (same month) -> should not run.
	recent := time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	// Synced last month -> should run.
	stale := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

func TestCategorizeOSM(t *testing.T) {
	tests := []struct {
		tags     map[string]string
		category string
		sub      string
	}{
		{map[string]string{"amenity": "school"}, "education", "school"},
		{map[string]string{"amenity": "hospital"}, "healthcare", "hospital"},
		{map[string]string{"amenity": "fire_station"}, "emergency", "fire_station"},
		{map[string]string{"amenity": "police"}, "emergency", "police"},
		{map[string]string{"amenity": "library"}, "education", "library"},
		{map[string]string{"amenity": "post_office"}, "government", "post_office"},
		{map[string]string{"amenity": "place_of_worship"}, "religious", "place_of_worship"},
		{map[string]string{"amenity": "cafe"}, "other", "cafe"},
		{map[string]string{"leisure": "park"}, "recreation", "park"},
		{map[string]string{"leisure": "playground"}, "recreation", "playground"},
		{map[string]string{"building": "yes"}, "other", "unknown"},
	}
	for _, tt := range tests {
		cat, sub := categorizeOSM(tt.tags)
		assert.Equal(t, tt.category, cat, "tags=%v", tt.tags)
		assert.Equal(t, tt.sub, sub, "tags=%v", tt.tags)
	}
}

func TestNewPOIRow(t *testing.T) {
	elem := overpass.Element{
		Type: "node",
		ID:   12345,
		Lat:  40.7128,
		Lon:  -74.006,
		Tags: map[string]string{
			"amenity": "school",
			"name":    "PS 101",
			"addr":    "123 Main St",
		},
	}

	row, ok := newPOIRow(elem)
	require.True(t, ok)
	require.Len(t, row, 8)

	assert.Equal(t, "PS 101", row[0])         // name
	assert.Equal(t, "education", row[1])      // category
	assert.Equal(t, "school", row[2])         // subcategory
	assert.InDelta(t, 40.7128, row[3], 0.001) // latitude
	assert.InDelta(t, -74.006, row[4], 0.001) // longitude
	assert.Equal(t, "osm", row[5])            // source
	assert.Equal(t, "osm/12345", row[6])      // source_id
	assert.IsType(t, []byte{}, row[7])        // properties (JSON)
}

func TestNewPOIRow_NoName(t *testing.T) {
	elem := overpass.Element{
		Type: "node",
		ID:   999,
		Lat:  40.0,
		Lon:  -74.0,
		Tags: map[string]string{"amenity": "school"},
	}

	_, ok := newPOIRow(elem)
	assert.False(t, ok)
}

func TestOSMPOI_Sync(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(testOSMOverpassJSON))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Each tile returns 3 elements. Total = 3 * len(usTiles()).
	totalRows := int64(3 * len(usTiles()))
	expectPOIUpsert(mock, totalRows)

	s := &OSMPOI{endpointURL: srv.URL}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, totalRows, result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOSMPOI_OverpassError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "overloaded", http.StatusInternalServerError)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// All tiles fail -> 0 rows, no upsert expected.
	s := &OSMPOI{endpointURL: srv.URL}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestOSMPOI_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"elements":[]}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &OSMPOI{endpointURL: srv.URL}
	result, err := s.Sync(context.Background(), mock, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestUsTiles(t *testing.T) {
	tiles := usTiles()
	assert.NotEmpty(t, tiles)

	// Verify first tile starts at CONUS southwest corner.
	assert.InDelta(t, 24.396308, tiles[0].south, 0.001)
	assert.InDelta(t, -125.0, tiles[0].west, 0.001)

	// Verify last tile ends at CONUS northeast corner.
	last := tiles[len(tiles)-1]
	assert.InDelta(t, 49.384358, last.north, 0.001)
	assert.InDelta(t, -66.93457, last.east, 0.001)

	// All tiles should be within CONUS bounds.
	for _, tile := range tiles {
		assert.GreaterOrEqual(t, tile.south, usBBox[0])
		assert.GreaterOrEqual(t, tile.west, usBBox[1])
		assert.LessOrEqual(t, tile.north, usBBox[2])
		assert.LessOrEqual(t, tile.east, usBBox[3])
	}
}

func TestOSMPOI_UpsertError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(testOSMOverpassJSON))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &OSMPOI{endpointURL: srv.URL}
	_, err = s.Sync(context.Background(), mock, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestOSMPOI_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"elements":[]}`))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &OSMPOI{endpointURL: srv.URL}
	_, err = s.Sync(ctx, mock, nil, t.TempDir())
	require.Error(t, err)
}

// expectPOIUpsert sets up pgxmock expectations for a single BulkUpsert call
// on the geo.poi table.
func expectPOIUpsert(mock pgxmock.PgxPoolIface, rows int64) {
	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_geo_poi"}, poiCols).WillReturnResult(rows)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", rows))
	mock.ExpectCommit()
}
