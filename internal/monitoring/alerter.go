package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
)

// AlertType identifies the kind of alert.
type AlertType string

const (
	AlertPipelineFailureRate AlertType = "pipeline_failure_rate"
	AlertFedsyncFailure      AlertType = "fedsync_failure"
	AlertCostOverrun         AlertType = "cost_overrun"
)

// Alert represents a single alert to be sent.
type Alert struct {
	Type      AlertType      `json:"type"`
	Severity  string         `json:"severity"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}

// Alerter evaluates a MetricsSnapshot against configured thresholds
// and sends alerts via webhook when thresholds are breached.
type Alerter struct {
	cfg    config.MonitoringConfig
	client *http.Client
}

// NewAlerter creates a new Alerter with the given monitoring config.
func NewAlerter(cfg config.MonitoringConfig) *Alerter {
	return &Alerter{
		cfg:    cfg,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Evaluate checks the snapshot against thresholds and returns any alerts.
func (a *Alerter) Evaluate(snap *MetricsSnapshot) []Alert {
	var alerts []Alert
	now := time.Now().UTC()

	// Check pipeline failure rate.
	finished := snap.PipelineComplete + snap.PipelineFailed
	if finished >= 5 && snap.PipelineFailRate > a.cfg.FailureRateThreshold {
		alerts = append(alerts, Alert{
			Type:     AlertPipelineFailureRate,
			Severity: "high",
			Message: fmt.Sprintf(
				"Pipeline failure rate %.1f%% exceeds threshold %.1f%% (%d failed / %d finished in last %dh)",
				snap.PipelineFailRate*100, a.cfg.FailureRateThreshold*100,
				snap.PipelineFailed, finished, snap.LookbackHours,
			),
			Details: map[string]any{
				"failure_rate": snap.PipelineFailRate,
				"threshold":   a.cfg.FailureRateThreshold,
				"failed":      snap.PipelineFailed,
				"finished":    finished,
			},
			Timestamp: now,
		})
	}

	// Check fedsync failures.
	if snap.FedsyncFailed > 0 {
		alerts = append(alerts, Alert{
			Type:     AlertFedsyncFailure,
			Severity: "high",
			Message: fmt.Sprintf(
				"%d fedsync dataset(s) failed in last %dh",
				snap.FedsyncFailed, snap.LookbackHours,
			),
			Details: map[string]any{
				"failed_count": snap.FedsyncFailed,
				"total_syncs":  snap.FedsyncTotal,
			},
			Timestamp: now,
		})
	}

	// Check cost overrun.
	if a.cfg.CostThresholdUSD > 0 && snap.PipelineCostUSD > a.cfg.CostThresholdUSD {
		alerts = append(alerts, Alert{
			Type:     AlertCostOverrun,
			Severity: "high",
			Message: fmt.Sprintf(
				"API cost $%.2f exceeds threshold $%.2f in last %dh",
				snap.PipelineCostUSD, a.cfg.CostThresholdUSD, snap.LookbackHours,
			),
			Details: map[string]any{
				"cost_usd":       snap.PipelineCostUSD,
				"threshold_usd":  a.cfg.CostThresholdUSD,
				"pipeline_total": snap.PipelineTotal,
			},
			Timestamp: now,
		})
	}

	return alerts
}

// SendAlerts delivers alerts to the configured webhook URL.
// Returns the number of alerts successfully sent.
func (a *Alerter) SendAlerts(ctx context.Context, alerts []Alert) int {
	if a.cfg.WebhookURL == "" || len(alerts) == 0 {
		return 0
	}

	sent := 0
	for _, alert := range alerts {
		if err := a.sendWebhook(ctx, alert); err != nil {
			zap.L().Error("monitoring: failed to send alert",
				zap.String("type", string(alert.Type)),
				zap.Error(err),
			)
			continue
		}
		zap.L().Info("monitoring: alert sent",
			zap.String("type", string(alert.Type)),
			zap.String("severity", alert.Severity),
		)
		sent++
	}
	return sent
}

// sendWebhook posts a single alert to the webhook URL.
func (a *Alerter) sendWebhook(ctx context.Context, alert Alert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return eris.Wrap(err, "monitoring: marshal alert")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return eris.Wrap(err, "monitoring: create webhook request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return eris.Wrap(err, "monitoring: webhook request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		return eris.Errorf("monitoring: webhook returned status %d", resp.StatusCode)
	}
	return nil
}
