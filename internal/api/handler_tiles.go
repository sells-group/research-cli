package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// ServeTiles delegates MVT tile requests to the geospatial tile handler.
// Route: GET /tiles/{layer}/{z}/{x}/{y}
func (h *Handlers) ServeTiles(w http.ResponseWriter, r *http.Request) {
	if h.tileHandler == nil {
		http.Error(w, "tile server not configured", http.StatusServiceUnavailable)
		return
	}
	// Reconstruct the path that TileHandler.ServeHTTP expects: /tiles/{layer}/{z}/{x}/{y}.pbf
	layer := chi.URLParam(r, "layer")
	z := chi.URLParam(r, "z")
	x := chi.URLParam(r, "x")
	yPbf := chi.URLParam(r, "y")
	r.URL.Path = fmt.Sprintf("/tiles/%s/%s/%s/%s", layer, z, x, yPbf)
	h.tileHandler.ServeHTTP(w, r)
}

// TileStats returns tile cache statistics.
// Route: GET /api/v1/tiles/stats
func (h *Handlers) TileStats(w http.ResponseWriter, r *http.Request) {
	if h.tileHandler == nil {
		http.Error(w, "tile server not configured", http.StatusServiceUnavailable)
		return
	}
	h.tileHandler.StatsHandler(w, r)
}
