package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/sells-group/research-cli/internal/config"
)

// Router builds a chi.Router with middleware and all API routes.
func Router(h *Handlers) chi.Router {
	r := chi.NewRouter()

	// Middleware stack.
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(ZapLogger())
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins(h.cfg),
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		ExposedHeaders:   []string{"X-Request-Id"},
		AllowCredentials: true,
		MaxAge:           300,
	}))
	r.Use(middleware.Compress(5))

	// Custom JSON 404/405 handlers.
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, http.StatusNotFound, "not_found", "not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	secret := ""
	if h.cfg != nil {
		secret = h.cfg.Server.WebhookSecret
	}

	// Versioned API routes.
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", h.Health)
		r.With(BearerAuth(secret)).Post("/webhook/enrich", h.WebhookEnrich)
		r.Get("/metrics", h.Metrics)
		r.Get("/metrics/prometheus", h.PrometheusMetrics)

		r.Get("/runs", h.ListRuns)
		r.Get("/runs/{id}", h.GetRun)
		r.Get("/runs/{id}/provenance", h.GetRunProvenance)
		r.Post("/runs/{id}/retry", h.RetryRun)
		r.Get("/queue/status", h.QueueStatus)

		r.Get("/companies", h.ListCompanies)
		r.Get("/companies/search", h.SearchCompanies)
		r.Get("/companies/geojson", h.CompaniesGeoJSON)
		r.Get("/companies/{id}", h.GetCompanyHandler)
		r.Get("/companies/{id}/identifiers", h.GetCompanyIdentifiers)
		r.Get("/companies/{id}/addresses", h.GetCompanyAddresses)
		r.Get("/companies/{id}/matches", h.GetCompanyMatches)
		r.Get("/companies/{id}/msas", h.GetCompanyMSAs)
		r.Get("/companies/{id}/runs", h.GetCompanyRuns)

		r.Get("/fedsync/statuses", h.FedsyncStatuses)
		r.Get("/fedsync/sync-log", h.FedsyncSyncLog)

		r.Get("/data/tables", h.ListDataTables)
		r.Get("/data/{table}/aggregate", h.AggregateData)
		r.Get("/data/{table}/filters/{column}", h.GetDataFilters)
		r.Get("/data/{table}/{id}", h.GetDataRow)
		r.Get("/data/{table}", h.QueryDataTable)

		r.Get("/analytics/sync-trends", h.SyncTrends)
		r.Get("/analytics/identifier-coverage", h.IdentifierCoverage)
		r.Get("/analytics/xref-coverage", h.XrefCoverage)
		r.Get("/analytics/enrichment-stats", h.EnrichmentStatsHandler)
		r.Get("/analytics/cost-breakdown", h.CostBreakdownHandler)

		r.Get("/tiles/stats", h.TileStats)
	})

	// Backward-compatible top-level aliases.
	r.Get("/health", h.Health)
	r.With(BearerAuth(secret)).Post("/webhook/enrich", h.WebhookEnrich)
	r.Get("/metrics", h.Metrics)
	r.Get("/metrics/prometheus", h.PrometheusMetrics)

	// Temporal workflow progress/control endpoints.
	r.Route("/api/workflows", func(r chi.Router) {
		// Domain-specific endpoints (backward compatible).
		r.Get("/fedsync/progress", h.FedsyncProgress)
		r.Get("/enrichment/{runID}", h.EnrichmentProgress)
		r.Post("/batch/pause", h.BatchPause)
		r.Post("/batch/resume", h.BatchResume)
		// Generic progress endpoint — works with any workflow domain.
		r.Get("/{workflowID}/progress", h.WorkflowProgress)
	})

	r.Get("/tiles/{layer}/{z}/{x}/{y}.pbf", h.ServeTiles)

	return r
}

// corsOrigins returns configured CORS origins or a sensible default.
func corsOrigins(cfg *config.Config) []string {
	if cfg != nil && len(cfg.Server.CORSOrigins) > 0 {
		return cfg.Server.CORSOrigins
	}
	return []string{"https://*.sellsadvisors.com"}
}
