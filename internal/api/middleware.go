package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/opsmetrics"
)

// BearerAuth returns middleware that validates Authorization: Bearer <secret>.
// If secret is empty, all requests pass through.
func BearerAuth(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if secret == "" {
				next.ServeHTTP(w, r)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+secret {
				WriteError(w, r, http.StatusUnauthorized, "unauthorized", "unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ZapLogger returns middleware that logs each request using the global zap logger.
func ZapLogger() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			requestID := middleware.GetReqID(r.Context())
			if requestID != "" {
				w.Header().Set("X-Request-Id", requestID)
			}

			next.ServeHTTP(ww, r)

			routePattern := ""
			if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
				routePattern = routeCtx.RoutePattern()
			}
			opsmetrics.RecordHTTPRequest(r.Method, routePattern, ww.Status(), time.Since(start))

			zap.L().Info("http request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("route", routePattern),
				zap.Int("status", ww.Status()),
				zap.Int("bytes", ww.BytesWritten()),
				zap.Duration("duration", time.Since(start)),
				zap.String("request_id", requestID),
			)
		})
	}
}
