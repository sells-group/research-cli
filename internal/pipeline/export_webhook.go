package pipeline

import (
	"context"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
)

// WebhookExporter sends failed enrichment results to a webhook (e.g. ToolJet)
// for manual review. Only fires when the gate does not pass.
type WebhookExporter struct {
	webhookURL string
}

// NewWebhookExporter creates a WebhookExporter.
func NewWebhookExporter(webhookURL string) *WebhookExporter {
	return &WebhookExporter{webhookURL: webhookURL}
}

// Name implements ResultExporter.
func (e *WebhookExporter) Name() string { return "webhook" }

// ExportResult implements ResultExporter.
func (e *WebhookExporter) ExportResult(ctx context.Context, result *model.EnrichmentResult, gate *GateResult) error {
	if gate.Passed || e.webhookURL == "" {
		return nil
	}
	if err := sendToToolJet(ctx, result, e.webhookURL); err != nil {
		zap.L().Warn("exporter: webhook failed",
			zap.String("company", result.Company.Name),
			zap.Error(err),
		)
	}
	return nil
}

// Flush implements ResultExporter.
func (e *WebhookExporter) Flush(_ context.Context) error { return nil }
