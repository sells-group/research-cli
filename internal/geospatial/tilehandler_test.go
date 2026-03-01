package geospatial

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
)

func TestTileHandler_InvalidPath(t *testing.T) {
	handler := NewTileHandler(nil, DefaultLayers(), nil)

	req := httptest.NewRequest(http.MethodGet, "/tiles/bad", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestTileHandler_UnknownLayer(t *testing.T) {
	handler := NewTileHandler(nil, DefaultLayers(), nil)

	req := httptest.NewRequest(http.MethodGet, "/tiles/nonexistent/5/10/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestTileHandler_OutOfZoomRange(t *testing.T) {
	handler := NewTileHandler(nil, DefaultLayers(), nil)

	// Counties minZoom is 3, so z=1 should return 204.
	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/1/0/0.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204 for z below minZoom, got %d", w.Code)
	}
}

func TestTileHandler_CacheHit(t *testing.T) {
	cache := NewTileCache(100, 10*time.Minute)
	handler := NewTileHandler(nil, DefaultLayers(), cache)

	// Pre-populate cache.
	cache.Put("counties", 5, 10, 10, []byte("cached-tile"))

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/5/10/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("X-Cache") != "hit" {
		t.Errorf("expected cache hit, got %s", w.Header().Get("X-Cache"))
	}
	if w.Header().Get("Content-Type") != "application/vnd.mapbox-vector-tile" {
		t.Errorf("expected MVT content type, got %s", w.Header().Get("Content-Type"))
	}
	if w.Body.String() != "cached-tile" {
		t.Errorf("expected cached-tile, got %s", w.Body.String())
	}
}

func TestTileHandler_GeneratesAndCaches(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	cache := NewTileCache(100, 10*time.Minute)
	handler := NewTileHandler(mock, DefaultLayers(), cache)

	tileData := []byte("generated-mvt")
	mock.ExpectQuery("SELECT ST_AsMVT").
		WithArgs(5, 10, 10).
		WillReturnRows(pgxmock.NewRows([]string{"st_asmvt"}).AddRow(tileData))

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/5/10/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("X-Cache") != "miss" {
		t.Errorf("expected cache miss, got %s", w.Header().Get("X-Cache"))
	}

	// Verify tile was cached.
	cached := cache.Get("counties", 5, 10, 10)
	if cached == nil {
		t.Error("expected tile to be cached after generation")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTileHandler_DBError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	handler := NewTileHandler(mock, DefaultLayers(), nil)

	mock.ExpectQuery("SELECT ST_AsMVT").
		WithArgs(5, 10, 10).
		WillReturnError(errTest)

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/5/10/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTileHandler_StatsHandler(t *testing.T) {
	cache := NewTileCache(100, 10*time.Minute)
	handler := NewTileHandler(nil, nil, cache)

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	handler.StatsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if body == "" {
		t.Error("expected non-empty stats output")
	}
}

func TestTileHandler_StatsHandler_NilCache(t *testing.T) {
	handler := NewTileHandler(nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	handler.StatsHandler(w, req)

	if w.Body.String() != "cache disabled" {
		t.Errorf("expected 'cache disabled', got %s", w.Body.String())
	}
}

func TestTileHandler_InvalidZ(t *testing.T) {
	handler := NewTileHandler(nil, DefaultLayers(), nil)

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/abc/10/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid z, got %d", w.Code)
	}
}

func TestTileHandler_InvalidX(t *testing.T) {
	handler := NewTileHandler(nil, DefaultLayers(), nil)

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/5/abc/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid x, got %d", w.Code)
	}
}

func TestTileHandler_InvalidY(t *testing.T) {
	handler := NewTileHandler(nil, DefaultLayers(), nil)

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/5/10/abc.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid y, got %d", w.Code)
	}
}

func TestTileHandler_GeneratesWithoutCache(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	handler := NewTileHandler(mock, DefaultLayers(), nil)

	tileData := []byte("generated-mvt")
	mock.ExpectQuery("SELECT ST_AsMVT").
		WithArgs(5, 10, 10).
		WillReturnRows(pgxmock.NewRows([]string{"st_asmvt"}).AddRow(tileData))

	req := httptest.NewRequest(http.MethodGet, "/tiles/counties/5/10/10.pbf", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("X-Cache") != "miss" {
		t.Errorf("expected cache miss, got %s", w.Header().Get("X-Cache"))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
