package opsmetrics

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCollector_ServeHTTP(t *testing.T) {
	collector := New()
	collector.RecordHTTPRequest("GET", "/api/v1/health", 200, 25*time.Millisecond)
	collector.RecordHTTPRequest("POST", "/api/v1/webhook/enrich", 500, 150*time.Millisecond)
	collector.RecordCacheEvent("hit", "queue_status", "memory")

	rr := httptest.NewRecorder()
	collector.ServeHTTP(rr)

	body := rr.Body.String()
	assert.Contains(t, body, "research_api_requests_total")
	assert.Contains(t, body, `/api/v1/health`)
	assert.Contains(t, body, `research_api_request_errors_total`)
	assert.Contains(t, body, `research_api_cache_events_total`)
	assert.Contains(t, body, `queue_status`)
}
