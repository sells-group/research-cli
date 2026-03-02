package main

import (
	"fmt"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/geospatial"
)

var geoTilesCmd = &cobra.Command{
	Use:   "tiles",
	Short: "Start MVT tile server",
	Long:  "Starts an HTTP server that serves MVT vector tiles at /tiles/{layer}/{z}/{x}/{y}.pbf.",
	RunE:  runGeoTiles,
}

func init() {
	geoTilesCmd.Flags().IntP("port", "p", 8081, "HTTP server port")
	geoTilesCmd.Flags().Int("cache-size", 10000, "Tile cache max entries")
	geoTilesCmd.Flags().Duration("cache-ttl", 1*time.Hour, "Tile cache TTL")
	geoCmd.AddCommand(geoTilesCmd)
}

func runGeoTiles(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	port, _ := cmd.Flags().GetInt("port")
	cacheSize, _ := cmd.Flags().GetInt("cache-size")
	cacheTTL, _ := cmd.Flags().GetDuration("cache-ttl")

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	layers := geospatial.DefaultLayers()
	cache := geospatial.NewTileCache(cacheSize, cacheTTL)

	mux := http.NewServeMux()
	mux.HandleFunc("/tiles/", func(w http.ResponseWriter, r *http.Request) {
		serveTile(w, r, pool, layers, cache)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	zap.L().Info("starting tile server", zap.String("addr", addr), zap.Int("cache_size", cacheSize))

	go func() {
		<-ctx.Done()
		zap.L().Info("shutting down tile server")
		_ = srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return eris.Wrap(err, "tile server")
	}
	return nil
}

// serveTile handles /tiles/{layer}/{z}/{x}/{y}.pbf requests.
func serveTile(w http.ResponseWriter, r *http.Request, pool db.Pool, layers map[string]geospatial.LayerConfig, cache *geospatial.TileCache) {
	// Parse path: /tiles/{layer}/{z}/{x}/{y}.pbf
	path := strings.TrimPrefix(r.URL.Path, "/tiles/")
	parts := strings.Split(path, "/")
	if len(parts) != 4 {
		http.Error(w, "invalid tile path", http.StatusBadRequest)
		return
	}

	layerName := parts[0]
	layer, ok := layers[layerName]
	if !ok {
		http.Error(w, "unknown layer", http.StatusNotFound)
		return
	}

	z, err := strconv.Atoi(parts[1])
	if err != nil {
		http.Error(w, "invalid z", http.StatusBadRequest)
		return
	}
	x, err := strconv.Atoi(parts[2])
	if err != nil {
		http.Error(w, "invalid x", http.StatusBadRequest)
		return
	}
	yStr := strings.TrimSuffix(parts[3], ".pbf")
	y, err := strconv.Atoi(yStr)
	if err != nil {
		http.Error(w, "invalid y", http.StatusBadRequest)
		return
	}

	// Check zoom bounds.
	if z < layer.MinZoom || z > layer.MaxZoom {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Check cache.
	if cached := cache.Get(layerName, z, x, y); cached != nil {
		w.Header().Set("Content-Type", "application/vnd.mapbox-vector-tile")
		w.Header().Set("X-Cache", "hit")
		_, _ = w.Write(cached)
		return
	}

	// Generate tile.
	tile, err := geospatial.GenerateMVT(r.Context(), pool, layer, z, x, y)
	if err != nil {
		zap.L().Error("tile generation failed",
			zap.String("layer", layerName),
			zap.Int("z", z), zap.Int("x", x), zap.Int("y", y),
			zap.Error(err),
		)
		http.Error(w, "tile generation failed", http.StatusInternalServerError)
		return
	}

	// Cache and return.
	cache.Put(layerName, z, x, y, tile)
	w.Header().Set("Content-Type", "application/vnd.mapbox-vector-tile")
	w.Header().Set("X-Cache", "miss")
	_, _ = w.Write(tile)
}
