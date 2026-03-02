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
	"github.com/sells-group/research-cli/internal/monitoring"
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

		// Initialize metrics collector (syncLog is nil unless fedsync pool is available).
		collector := monitoring.NewCollector(env.Store, nil)

		// Start background alert checker if monitoring is enabled.
		if cfg.Monitoring.Enabled {
			alerter := monitoring.NewAlerter(cfg.Monitoring)
			checker := monitoring.NewChecker(collector, alerter, cfg.Monitoring)
			go checker.Run(ctx)
			zap.L().Info("monitoring: alert checker enabled",
				zap.String("webhook_url", cfg.Monitoring.WebhookURL),
			)
		}

		h := api.NewHandlers(cfg, env.Store, env.Pipeline, collector)
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
	go func() {
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
