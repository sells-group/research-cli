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
	})

	// Backward-compatible top-level aliases.
	r.Get("/health", h.Health)
	r.With(BearerAuth(secret)).Post("/webhook/enrich", h.WebhookEnrich)
	r.Get("/metrics", h.Metrics)

	return r
}

// corsOrigins returns configured CORS origins or a sensible default.
func corsOrigins(cfg *config.Config) []string {
	if cfg != nil && len(cfg.Server.CORSOrigins) > 0 {
		return cfg.Server.CORSOrigins
	}
	return []string{"https://*.sellsadvisors.com"}
}
