package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/enrichmentstart"
	"github.com/sells-group/research-cli/internal/model"
)

// mockRunner implements Runner for testing.
type mockRunner struct {
	result *model.EnrichmentResult
	err    error
	panic  any // if non-nil, panics inside Run
}

func (m *mockRunner) Run(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
	if m.panic != nil {
		panic(m.panic)
	}
	return m.result, m.err
}

type mockStarter struct {
	startWebhook func(ctx context.Context, company model.Company, requestID string) (*enrichmentstart.StartResult, error)
	startRetry   func(ctx context.Context, originalRunID string, company model.Company, requestID string) (*enrichmentstart.StartResult, error)
}

func (m mockStarter) StartWebhook(ctx context.Context, company model.Company, requestID string) (*enrichmentstart.StartResult, error) {
	return m.startWebhook(ctx, company, requestID)
}

func (m mockStarter) StartRetry(ctx context.Context, originalRunID string, company model.Company, requestID string) (*enrichmentstart.StartResult, error) {
	return m.startRetry(ctx, originalRunID, company, requestID)
}

func TestWebhookEnrich_ValidRequest(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	payload := map[string]string{
		"url":           "https://acme.com",
		"salesforce_id": "001ABC",
		"name":          "Acme Corp",
	}
	body, _ := json.Marshal(payload)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "accepted", resp["status"])
	assert.Equal(t, "https://acme.com", resp["company"])

	// Let the background goroutine complete.
	time.Sleep(10 * time.Millisecond)
	h.Drain()
}

func TestWebhookEnrich_MissingURL(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	body := []byte(`{"salesforce_id":"001ABC","name":"Acme Corp"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "url is required")
}

func TestWebhookEnrich_InvalidJSON(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader([]byte("not json")))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid request body")
}

func TestWebhookEnrich_EmptyBody(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader([]byte("{}")))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "url is required")
}

func TestWebhookEnrich_URLOnly(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	body := []byte(`{"url":"https://minimal.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "accepted", resp["status"])
	assert.Equal(t, "https://minimal.com", resp["company"])

	time.Sleep(10 * time.Millisecond)
	h.Drain()
}

func TestWebhookEnrich_SemaphoreFull(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	// Fill the semaphore.
	for i := 0; i < WebhookSemSize; i++ {
		h.sem <- struct{}{}
	}

	body := []byte(`{"url":"https://test.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "too many concurrent requests")

	// Drain semaphore to clean up.
	for i := 0; i < WebhookSemSize; i++ {
		<-h.sem
	}
}

func TestWebhookEnrich_NilPipeline(t *testing.T) {
	// With nil runner the goroutine logs and returns without panic.
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)

	body := []byte(`{"url":"https://test.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)

	// Wait for the goroutine.
	h.Drain()
}

func TestWebhookEnrich_PipelineSuccess(t *testing.T) {
	runner := &mockRunner{
		result: &model.EnrichmentResult{Score: 0.85},
	}
	h := NewHandlers(&config.Config{}, nil, runner, nil, nil)

	body := []byte(`{"url":"https://acme.com","salesforce_id":"001ABC","name":"Acme"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)
	h.Drain()
}

func TestWebhookEnrich_PipelineError(t *testing.T) {
	runner := &mockRunner{
		err: eris.New("extraction failed"),
	}
	h := NewHandlers(&config.Config{}, nil, runner, nil, nil)

	body := []byte(`{"url":"https://fail.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)
	h.Drain()
}

func TestWebhookEnrich_PipelinePanic(t *testing.T) {
	runner := &mockRunner{
		panic: "test panic in pipeline",
	}
	h := NewHandlers(&config.Config{}, nil, runner, nil, nil)

	body := []byte(`{"url":"https://panic.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)

	// Drain should complete without propagating the panic.
	h.Drain()
}

func TestWebhookEnrich_InvalidatesRunsCache(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)
	require.NoError(t, h.cache.Set(apicache.KeyQueueStatus, queueStatusResponse{Queued: 1}, time.Minute))

	body := []byte(`{"url":"https://cache-test.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)
	_, ok := h.cache.Get(apicache.KeyQueueStatus)
	assert.False(t, ok)
	h.Drain()
}

func TestWebhookEnrich_UsesStarterResponse(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil, nil)
	h.SetEnrichmentStarter(mockStarter{
		startWebhook: func(_ context.Context, company model.Company, requestID string) (*enrichmentstart.StartResult, error) {
			assert.Equal(t, "https://acme.com", company.URL)
			assert.Equal(t, "", requestID)
			return &enrichmentstart.StartResult{
				WorkflowID:    "wf-123",
				WorkflowRunID: "run-123",
				Reused:        true,
			}, nil
		},
	})

	body := []byte(`{"url":"https://acme.com"}`)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	h.WebhookEnrich(w, r)

	require.Equal(t, http.StatusAccepted, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "wf-123", resp["workflow_id"])
	assert.Equal(t, "run-123", resp["workflow_run_id"])
	assert.Equal(t, true, resp["reused"])
}
