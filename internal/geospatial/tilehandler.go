package geospatial

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// TileHandler serves MVT vector tiles over HTTP.
type TileHandler struct {
	pool   db.Pool
	layers map[string]LayerConfig
	cache  *TileCache
}

// NewTileHandler creates a new MVT tile HTTP handler.
func NewTileHandler(pool db.Pool, layers map[string]LayerConfig, cache *TileCache) *TileHandler {
	return &TileHandler{
		pool:   pool,
		layers: layers,
		cache:  cache,
	}
}

// ServeHTTP handles requests at /tiles/{layer}/{z}/{x}/{y}.pbf.
func (h *TileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse path: expect {layer}/{z}/{x}/{y}.pbf after prefix stripping.
	path := strings.TrimPrefix(r.URL.Path, "/tiles/")
	parts := strings.Split(path, "/")
	if len(parts) != 4 {
		http.Error(w, "invalid tile path", http.StatusBadRequest)
		return
	}

	layerName := parts[0]
	layer, ok := h.layers[layerName]
	if !ok {
		http.Error(w, "unknown layer", http.StatusNotFound)
		return
	}

	z, err := strconv.Atoi(parts[1])
	if err != nil {
		http.Error(w, "invalid z coordinate", http.StatusBadRequest)
		return
	}
	x, err := strconv.Atoi(parts[2])
	if err != nil {
		http.Error(w, "invalid x coordinate", http.StatusBadRequest)
		return
	}
	yStr := strings.TrimSuffix(parts[3], ".pbf")
	y, err := strconv.Atoi(yStr)
	if err != nil {
		http.Error(w, "invalid y coordinate", http.StatusBadRequest)
		return
	}

	// Check zoom bounds.
	if z < layer.MinZoom || z > layer.MaxZoom {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Check cache.
	if h.cache != nil {
		if cached := h.cache.Get(layerName, z, x, y); cached != nil {
			w.Header().Set("Content-Type", "application/vnd.mapbox-vector-tile")
			w.Header().Set("X-Cache", "hit")
			_, _ = w.Write(cached)
			return
		}
	}

	// Generate tile.
	tile, err := GenerateMVT(r.Context(), h.pool, layer, z, x, y)
	if err != nil {
		zap.L().Error("geo: tile generation failed",
			zap.String("layer", layerName),
			zap.Int("z", z), zap.Int("x", x), zap.Int("y", y),
			zap.Error(err),
		)
		http.Error(w, "tile generation failed", http.StatusInternalServerError)
		return
	}

	// Cache and respond.
	if h.cache != nil {
		h.cache.Put(layerName, z, x, y, tile)
	}
	w.Header().Set("Content-Type", "application/vnd.mapbox-vector-tile")
	w.Header().Set("X-Cache", "miss")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write(tile)
}

// StatsHandler returns cache statistics as plain text.
func (h *TileHandler) StatsHandler(w http.ResponseWriter, _ *http.Request) {
	if h.cache == nil {
		_, _ = w.Write([]byte("cache disabled"))
		return
	}
	stats := h.cache.Stats()
	_, _ = fmt.Fprintf(w, "entries=%d max=%d hits=%d misses=%d rate=%.2f%%\n",
		stats.Entries, stats.MaxEntries, stats.Hits, stats.Misses, stats.HitRate*100)
}
