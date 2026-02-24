package monitoring

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func TestAlerter_Evaluate_NoAlerts(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		FailureRateThreshold: 0.10,
		CostThresholdUSD:     500.0,
	})

	snap := &MetricsSnapshot{
		PipelineTotal:    100,
		PipelineComplete: 95,
		PipelineFailed:   5,
		PipelineFailRate: 0.05,
		PipelineCostUSD:  100.0,
		FedsyncFailed:    0,
		LookbackHours:    24,
	}

	alerts := a.Evaluate(snap)
	assert.Empty(t, alerts)
}

func TestAlerter_Evaluate_PipelineFailureRate(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		FailureRateThreshold: 0.10,
		CostThresholdUSD:     500.0,
	})

	snap := &MetricsSnapshot{
		PipelineTotal:    20,
		PipelineComplete: 12,
		PipelineFailed:   8,
		PipelineFailRate: 0.4, // 8/20 = 40%
		PipelineCostUSD:  50.0,
		LookbackHours:    24,
	}

	alerts := a.Evaluate(snap)
	require.Len(t, alerts, 1)
	assert.Equal(t, AlertPipelineFailureRate, alerts[0].Type)
	assert.Equal(t, "high", alerts[0].Severity)
	assert.Contains(t, alerts[0].Message, "40.0%")
}

func TestAlerter_Evaluate_FedsyncFailure(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		FailureRateThreshold: 0.10,
		CostThresholdUSD:     500.0,
	})

	snap := &MetricsSnapshot{
		FedsyncTotal:  5,
		FedsyncFailed: 2,
		LookbackHours: 24,
	}

	alerts := a.Evaluate(snap)
	require.Len(t, alerts, 1)
	assert.Equal(t, AlertFedsyncFailure, alerts[0].Type)
	assert.Contains(t, alerts[0].Message, "2 fedsync")
}

func TestAlerter_Evaluate_CostOverrun(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		FailureRateThreshold: 0.10,
		CostThresholdUSD:     100.0,
	})

	snap := &MetricsSnapshot{
		PipelineTotal:    50,
		PipelineComplete: 48,
		PipelineFailed:   2,
		PipelineFailRate: 0.04,
		PipelineCostUSD:  250.0,
		LookbackHours:    24,
	}

	alerts := a.Evaluate(snap)
	require.Len(t, alerts, 1)
	assert.Equal(t, AlertCostOverrun, alerts[0].Type)
	assert.Contains(t, alerts[0].Message, "$250.00")
}

func TestAlerter_Evaluate_MultipleAlerts(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		FailureRateThreshold: 0.10,
		CostThresholdUSD:     100.0,
	})

	snap := &MetricsSnapshot{
		PipelineTotal:    20,
		PipelineComplete: 10,
		PipelineFailed:   10,
		PipelineFailRate: 0.5,
		PipelineCostUSD:  300.0,
		FedsyncTotal:     3,
		FedsyncFailed:    1,
		LookbackHours:    24,
	}

	alerts := a.Evaluate(snap)
	assert.Len(t, alerts, 3)

	types := make(map[AlertType]bool)
	for _, a := range alerts {
		types[a.Type] = true
	}
	assert.True(t, types[AlertPipelineFailureRate])
	assert.True(t, types[AlertFedsyncFailure])
	assert.True(t, types[AlertCostOverrun])
}

func TestAlerter_Evaluate_MinimumRunsRequired(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		FailureRateThreshold: 0.10,
		CostThresholdUSD:     500.0,
	})

	// Only 3 finished runs — below the 5-run minimum for failure rate alert.
	snap := &MetricsSnapshot{
		PipelineTotal:    3,
		PipelineComplete: 1,
		PipelineFailed:   2,
		PipelineFailRate: 0.666,
		LookbackHours:    24,
	}

	alerts := a.Evaluate(snap)
	assert.Empty(t, alerts)
}

func TestAlerter_SendAlerts_Webhook(t *testing.T) {
	var received atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var alert Alert
		err := json.NewDecoder(r.Body).Decode(&alert)
		require.NoError(t, err)
		assert.NotEmpty(t, alert.Type)
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	a := NewAlerter(config.MonitoringConfig{
		WebhookURL: ts.URL,
	})

	alerts := []Alert{
		{Type: AlertPipelineFailureRate, Severity: "high", Message: "test alert 1"},
		{Type: AlertFedsyncFailure, Severity: "high", Message: "test alert 2"},
	}

	sent := a.SendAlerts(context.Background(), alerts)
	assert.Equal(t, 2, sent)
	assert.Equal(t, int32(2), received.Load())
}

func TestAlerter_SendAlerts_EmptyURL(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		WebhookURL: "",
	})

	sent := a.SendAlerts(context.Background(), []Alert{
		{Type: AlertPipelineFailureRate, Message: "test"},
	})
	assert.Equal(t, 0, sent)
}

func TestAlerter_SendAlerts_EmptyAlerts(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		WebhookURL: "http://example.com",
	})

	sent := a.SendAlerts(context.Background(), nil)
	assert.Equal(t, 0, sent)
}

func TestAlerter_SendAlerts_WebhookError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	a := NewAlerter(config.MonitoringConfig{
		WebhookURL: ts.URL,
	})

	alerts := []Alert{
		{Type: AlertPipelineFailureRate, Message: "test"},
	}

	sent := a.SendAlerts(context.Background(), alerts)
	assert.Equal(t, 0, sent)
}

func TestAlerter_Evaluate_ZeroCostThreshold(t *testing.T) {
	a := NewAlerter(config.MonitoringConfig{
		CostThresholdUSD: 0, // disabled
	})

	snap := &MetricsSnapshot{
		PipelineCostUSD: 999.0,
		LookbackHours:   24,
	}

	alerts := a.Evaluate(snap)
	assert.Empty(t, alerts)
}
