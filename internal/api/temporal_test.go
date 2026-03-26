package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
)

func TestWorkflowProgress_RequiresQuery(t *testing.T) {
	mockClient := &mocks.Client{}
	h := NewHandlers(nil, nil, nil, nil, nil)
	h.SetTemporalClient(mockClient)

	router := chi.NewRouter()
	router.Get("/api/workflows/{workflowID}/progress", h.WorkflowProgress)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/workflows/run-123/progress", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	mockClient.AssertNotCalled(t, "QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWorkflowProgress_WithExplicitQuery(t *testing.T) {
	mockClient := &mocks.Client{}
	h := NewHandlers(nil, nil, nil, nil, nil)
	h.SetTemporalClient(mockClient)

	payloads, err := converter.GetDefaultDataConverter().ToPayloads(map[string]any{"percent": 50})
	require.NoError(t, err)
	mockClient.On("QueryWorkflow", mock.Anything, "run-123", "", "fedsync_progress").Return(client.NewValue(payloads), nil).Once()

	router := chi.NewRouter()
	router.Get("/api/workflows/{workflowID}/progress", h.WorkflowProgress)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/workflows/run-123/progress?query=fedsync_progress", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]int
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, 50, body["percent"])
}

func TestRouter_FedsyncProgressRoutePrecedence(t *testing.T) {
	mockClient := &mocks.Client{}
	h := NewHandlers(nil, nil, nil, nil, nil)
	h.SetTemporalClient(mockClient)

	payloads, err := converter.GetDefaultDataConverter().ToPayloads(map[string]any{"datasets": 3})
	require.NoError(t, err)
	mockClient.On("QueryWorkflow", mock.Anything, "wf-1", "", "fedsync_progress").Return(client.NewValue(payloads), nil).Once()

	router := Router(h)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/workflows/fedsync/progress?workflow_id=wf-1", nil)
	router.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]int
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, 3, body["datasets"])
}
