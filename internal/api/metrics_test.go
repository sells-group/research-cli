package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/monitoring"
	"github.com/sells-group/research-cli/internal/store/mocks"
)

func TestMetrics_NilCollector(t *testing.T) {
	h := NewHandlers(&config.Config{}, nil, nil, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Metrics(w, r)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "metrics collector not configured")
}

func TestMetrics_Success(t *testing.T) {
	st := mocks.NewMockStore(t)
	st.EXPECT().ListRuns(mock.Anything, mock.Anything).Return(nil, nil)
	st.EXPECT().CountDLQ(mock.Anything).Return(0, nil)

	collector := monitoring.NewCollector(st, nil)
	h := NewHandlers(&config.Config{}, st, nil, collector)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Metrics(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var snap monitoring.MetricsSnapshot
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &snap))
	assert.Equal(t, 24, snap.LookbackHours)
}

func TestMetrics_DefaultLookback(t *testing.T) {
	st := mocks.NewMockStore(t)
	st.EXPECT().ListRuns(mock.Anything, mock.Anything).Return(nil, nil)
	st.EXPECT().CountDLQ(mock.Anything).Return(0, nil)

	collector := monitoring.NewCollector(st, nil)
	// Config without explicit lookback hours — should default to 24.
	h := NewHandlers(&config.Config{}, st, nil, collector)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Metrics(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var snap monitoring.MetricsSnapshot
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &snap))
	assert.Equal(t, 24, snap.LookbackHours)
}

func TestMetrics_CustomLookback(t *testing.T) {
	st := mocks.NewMockStore(t)
	st.EXPECT().ListRuns(mock.Anything, mock.Anything).Return(nil, nil)
	st.EXPECT().CountDLQ(mock.Anything).Return(0, nil)

	collector := monitoring.NewCollector(st, nil)
	cfg := &config.Config{
		Monitoring: config.MonitoringConfig{LookbackWindowHours: 48},
	}
	h := NewHandlers(cfg, st, nil, collector)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Metrics(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var snap monitoring.MetricsSnapshot
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &snap))
	assert.Equal(t, 48, snap.LookbackHours)
}

func TestMetrics_NilConfig(t *testing.T) {
	st := mocks.NewMockStore(t)
	st.EXPECT().ListRuns(mock.Anything, mock.Anything).Return(nil, nil)
	st.EXPECT().CountDLQ(mock.Anything).Return(0, nil)

	collector := monitoring.NewCollector(st, nil)
	// Pass nil config — should use default lookback of 24.
	h := NewHandlers(nil, st, nil, collector)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Metrics(w, r)

	assert.Equal(t, http.StatusOK, w.Code)

	var snap monitoring.MetricsSnapshot
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &snap))
	assert.Equal(t, 24, snap.LookbackHours)
}

func TestMetrics_CollectError(t *testing.T) {
	st := mocks.NewMockStore(t)
	st.EXPECT().ListRuns(mock.Anything, mock.Anything).Return(nil, eris.New("db connection lost"))

	collector := monitoring.NewCollector(st, nil)
	h := NewHandlers(&config.Config{}, st, nil, collector)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.Metrics(w, r)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "db connection lost")
}
