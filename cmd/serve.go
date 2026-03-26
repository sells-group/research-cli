package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/api"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/enrichmentstart"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/geospatial"
	"github.com/sells-group/research-cli/internal/monitoring"
	"github.com/sells-group/research-cli/internal/readmodel"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
)

var servePort int

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start webhook server for enrichment requests",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("serve"); err != nil {
			return err
		}

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		readPool, closeReadPool, err := sharedReadModelPool(ctx, env.Store)
		if err != nil {
			return err
		}
		defer closeReadPool()

		cache, err := openServeAPICache(ctx)
		if err != nil {
			return err
		}
		defer cache.Close() //nolint:errcheck

		var syncLog *fedsync.SyncLog
		if readPool != nil {
			syncLog = fedsync.NewSyncLog(readPool)
			syncLog.SetCache(cache)
		}

		collector := monitoring.NewCollector(env.Store, syncLog)

		// Start background alert checker if monitoring is enabled.
		if cfg.Monitoring.Enabled {
			alerter := monitoring.NewAlerter(cfg.Monitoring)
			checker := monitoring.NewChecker(collector, alerter, cfg.Monitoring)
			go checker.Run(ctx)
			zap.L().Info("monitoring: alert checker enabled",
				zap.String("webhook_url", cfg.Monitoring.WebhookURL),
			)
		}

		h := api.NewHandlers(cfg, env.Store, env.Pipeline, collector, nil)
		h.SetCache(cache)
		if readPool != nil {
			h.SetReadModel(readmodel.NewPostgresService(readPool, cfg))
			if tileHandler := buildServeTileHandler(readPool); tileHandler != nil {
				h.SetTileHandler(tileHandler)
			}
		}
		if cfg.Temporal.HostPort != "" {
			temporalClient, temporalErr := temporalpkg.NewClient(cfg.Temporal)
			if temporalErr != nil {
				zap.L().Warn("temporal client unavailable, falling back to in-process API starts",
					zap.Error(temporalErr),
				)
			} else {
				defer temporalClient.Close()
				h.SetTemporalClient(temporalClient)
				h.SetEnrichmentStarter(enrichmentstart.NewService(temporalClient))
			}
		}
		router := api.Router(h)
		port := resolvePort(servePort, cfg.Server.Port)
		srvErr := startServer(ctx, router, port)
		h.Drain() // wait for in-flight enrichment jobs after server shutdown
		return srvErr
	},
}

func init() {
	serveCmd.Flags().IntVar(&servePort, "port", 0, "server port (default from config)")
	rootCmd.AddCommand(serveCmd)
}

// startServer creates and runs the HTTP server with graceful shutdown.
func startServer(ctx context.Context, handler http.Handler, port int) error {
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       120 * time.Second,
	}

	// Graceful shutdown — use a fresh context since ctx is already cancelled.
	go func() { // #nosec G118 -- intentional: background context outlives HTTP request for async shutdown
		<-ctx.Done()
		zap.L().Info("shutting down server")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	zap.L().Info("starting server", zap.Int("port", port))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return eris.Wrap(err, "server listen")
	}

	return nil
}

// resolvePort returns the port flag value if non-zero, otherwise the config default.
func resolvePort(flagPort, configPort int) int {
	if flagPort != 0 {
		return flagPort
	}
	return configPort
}

func buildServeTileHandler(pool db.Pool) *geospatial.TileHandler {
	if pool == nil || !cfg.Geo.Enabled {
		return nil
	}

	cacheSize := cfg.Geo.TileCache.MaxEntries
	if cacheSize <= 0 {
		cacheSize = 10000
	}
	cacheTTL := time.Duration(cfg.Geo.TileCache.TTLMinutes) * time.Minute
	if cacheTTL <= 0 {
		cacheTTL = time.Hour
	}

	cache := geospatial.NewTileCache(cacheSize, cacheTTL)
	return geospatial.NewTileHandler(pool, geospatial.DefaultLayers(), cache)
}
