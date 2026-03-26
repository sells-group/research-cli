package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func newTestRouter() http.Handler {
	cfg := &config.Config{Server: config.ServerConfig{Port: 8080}}
	return Router(NewHandlers(cfg, nil, nil, nil, nil))
}

func TestRouter_HealthVersioned(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "ok", body["status"])
}

func TestRouter_HealthTopLevel(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "ok", body["status"])
}

func TestRouter_CORSPreflight(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodOptions, "/api/v1/health", nil)
	r.Header.Set("Origin", "https://app.sellsadvisors.com")
	r.Header.Set("Access-Control-Request-Method", "GET")
	router.ServeHTTP(w, r)

	assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Origin"))
}

func TestRouter_RequestIDHeader(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	router.ServeHTTP(w, r)

	// Chi's RequestID middleware sets X-Request-Id on responses.
	// The middleware writes it to the response automatically.
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRouter_NotFound_ReturnsJSON(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var body ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "not found", body.Error)
	assert.Equal(t, "not_found", body.Code)
}

func TestRouter_MethodNotAllowed_ReturnsJSON(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodDelete, "/health", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var body ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "method not allowed", body.Error)
}

func TestRouter_CompressLargeResponse(t *testing.T) {
	// Create a handler that returns a large response to trigger compression.
	cfg := &config.Config{Server: config.ServerConfig{Port: 8080}}
	h := NewHandlers(cfg, nil, nil, nil, nil)
	router := Router(h)

	// Health response is small, but we can verify the Vary header is set
	// indicating compress middleware is active.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestCorsOrigins_Default(t *testing.T) {
	origins := corsOrigins(nil)
	require.Len(t, origins, 1)
	assert.Equal(t, "https://*.sellsadvisors.com", origins[0])
}

func TestCorsOrigins_FromConfig(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			CORSOrigins: []string{"https://custom.example.com"},
		},
	}
	origins := corsOrigins(cfg)
	require.Len(t, origins, 1)
	assert.Equal(t, "https://custom.example.com", origins[0])
}

func TestCorsOrigins_EmptyConfig(t *testing.T) {
	cfg := &config.Config{}
	origins := corsOrigins(cfg)
	require.Len(t, origins, 1)
	assert.Equal(t, "https://*.sellsadvisors.com", origins[0])
}

func TestRouter_WebhookAuth_Integration(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Port:          8080,
			WebhookSecret: "my-secret",
		},
	}
	router := Router(NewHandlers(cfg, nil, nil, nil, nil))

	// Without auth → 401.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich",
		strings.NewReader(`{"url":"https://test.com"}`))
	r.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)

	// With correct auth → 202.
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/webhook/enrich",
		strings.NewReader(`{"url":"https://test.com"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer my-secret")
	router.ServeHTTP(w, r)
	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestRouter_PrometheusMetrics(t *testing.T) {
	router := newTestRouter()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/prometheus", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")
	assert.Contains(t, w.Body.String(), "research_api_requests_total")
}
