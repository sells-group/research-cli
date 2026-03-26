package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/enrichmentstart"
	"github.com/sells-group/research-cli/internal/model"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
)

func TestRetryRun_InvalidatesRunsCache(t *testing.T) {
	st := &storemocks.MockStore{}
	st.EXPECT().GetRun(mock.Anything, "run-123").Return(&model.Run{
		ID:     "run-123",
		Status: model.RunStatusFailed,
		Company: model.Company{
			URL: "https://acme.com",
		},
	}, nil).Once()

	h := NewHandlers(&config.Config{}, st, nil, nil, nil)
	h.SetEnrichmentStarter(mockStarter{
		startRetry: func(_ context.Context, originalRunID string, company model.Company, requestID string) (*enrichmentstart.StartResult, error) {
			assert.Equal(t, "run-123", originalRunID)
			assert.Equal(t, "https://acme.com", company.URL)
			assert.Equal(t, "", requestID)
			return &enrichmentstart.StartResult{
				WorkflowID:    "wf-retry-1",
				WorkflowRunID: "wf-run-1",
				Reused:        true,
			}, nil
		},
	})
	require.NoError(t, h.cache.Set(apicache.KeyQueueStatus, queueStatusResponse{Queued: 1}, time.Minute))

	router := chi.NewRouter()
	router.Post("/runs/{id}/retry", h.RetryRun)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/runs/run-123/retry", nil)
	router.ServeHTTP(w, r)

	require.Equal(t, http.StatusAccepted, w.Code)
	_, ok := h.cache.Get(apicache.KeyQueueStatus)
	assert.False(t, ok)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "wf-retry-1", body["workflow_id"])
	assert.Equal(t, "wf-run-1", body["workflow_run_id"])
	assert.Equal(t, true, body["reused"])
	h.Drain()
	st.AssertExpectations(t)
}
