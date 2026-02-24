package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/monitoring"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/store"
)

var servePort int

// webhookSemSize limits concurrent webhook pipeline executions.
const webhookSemSize = 20

// buildMux constructs the HTTP handler for the webhook server.
// It returns the mux and a drain function that waits for all in-flight
// enrichment jobs to complete. The caller should invoke drain after the
// HTTP server has stopped accepting new requests.
func buildMux(_ context.Context, p *pipeline.Pipeline, st store.Store, webhookSecret string, collector *monitoring.Collector) (*http.ServeMux, func()) {
	mux := http.NewServeMux()
	sem := make(chan struct{}, webhookSemSize)
	var wg sync.WaitGroup

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if st != nil {
			if err := st.Ping(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /webhook/enrich", func(w http.ResponseWriter, r *http.Request) {
		// Authenticate if a webhook secret is configured.
		if webhookSecret != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+webhookSecret {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}

		var req struct {
			URL          string `json:"url"`
			SalesforceID string `json:"salesforce_id"`
			Name         string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}

		if req.URL == "" {
			http.Error(w, `{"error":"url is required"}`, http.StatusBadRequest)
			return
		}

		// Check semaphore capacity before accepting.
		select {
		case sem <- struct{}{}:
			// Acquired slot
		default:
			http.Error(w, `{"error":"too many concurrent requests"}`, http.StatusServiceUnavailable)
			return
		}

		company := model.Company{
			URL:          req.URL,
			SalesforceID: req.SalesforceID,
			Name:         req.Name,
		}

		// Run enrichment asynchronously with a background context so
		// in-flight jobs are not cancelled immediately on SIGINT.
		wg.Add(1)
		go func() {
			defer func() { <-sem }()
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					zap.L().Error("webhook enrichment panicked",
						zap.String("company", company.URL),
						zap.Any("panic", r),
						zap.Stack("stack"),
					)
				}
			}()
			if p == nil {
				zap.L().Error("webhook enrichment skipped: pipeline not initialized",
					zap.String("company", company.URL))
				return
			}
			jobCtx, jobCancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer jobCancel()
			result, err := p.Run(jobCtx, company)
			if err != nil {
				zap.L().Error("webhook enrichment failed",
					zap.String("company", company.URL),
					zap.Error(err),
				)
				return
			}
			zap.L().Info("webhook enrichment complete",
				zap.String("company", company.URL),
				zap.Float64("score", result.Score),
			)
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "accepted",
			"company": req.URL,
		})
	})

	if collector != nil {
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
			lookback := cfg.Monitoring.LookbackWindowHours
			if lookback <= 0 {
				lookback = 24
			}
			snap, err := collector.Collect(r.Context(), lookback)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(snap)
		})
	}

	drain := func() {
		wg.Wait()
	}
	return mux, drain
}

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

		mux, drain := buildMux(ctx, env.Pipeline, env.Store, cfg.Server.WebhookSecret, collector)
		port := resolvePort(servePort, cfg.Server.Port)
		srvErr := startServer(ctx, mux, port)
		drain() // wait for in-flight enrichment jobs after server shutdown
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
