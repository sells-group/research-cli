package pipeline

import (
	"context"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
)

// NotionExporter updates Notion Lead Tracker pages with enrichment status.
type NotionExporter struct {
	client notion.Client
}

// NewNotionExporter creates a NotionExporter.
func NewNotionExporter(client notion.Client) *NotionExporter {
	return &NotionExporter{client: client}
}

// Name implements ResultExporter.
func (e *NotionExporter) Name() string { return "notion" }

// ExportResult implements ResultExporter.
func (e *NotionExporter) ExportResult(ctx context.Context, result *model.EnrichmentResult, gate *GateResult) error {
	if e.client == nil || result.Company.NotionPageID == "" {
		return nil
	}

	status := "Enriched"
	if !gate.Passed {
		status = "Manual Review"
	}
	if err := updateNotionStatus(ctx, e.client, result.Company.NotionPageID, status, result); err != nil {
		zap.L().Warn("exporter: notion update failed",
			zap.String("company", result.Company.Name),
			zap.Error(err),
		)
		// Retry once.
		if retryErr := updateNotionStatus(ctx, e.client, result.Company.NotionPageID, status, result); retryErr != nil {
			zap.L().Error("exporter: notion retry also failed",
				zap.String("company", result.Company.Name),
				zap.Error(retryErr),
			)
		}
	}
	return nil
}

// Flush implements ResultExporter.
func (e *NotionExporter) Flush(_ context.Context) error { return nil }
