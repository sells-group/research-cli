package pipeline

import (
	"context"

	"github.com/sells-group/research-cli/internal/model"
)

// ResultExporter writes enrichment results to a destination.
type ResultExporter interface {
	// ExportResult processes a single enrichment result.
	// Called as each company completes. Exporters that need batch
	// aggregation (CSV, deferred SF) collect internally.
	ExportResult(ctx context.Context, result *model.EnrichmentResult, gate *GateResult) error

	// Flush finalizes any batched operations (e.g., write CSV, flush SF bulk).
	// Called after all companies have been processed.
	Flush(ctx context.Context) error

	// Name returns the exporter's human-readable identifier.
	Name() string
}
