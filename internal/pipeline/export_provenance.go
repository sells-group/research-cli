package pipeline

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// provenanceColumns defines the ordered provenance CSV output columns.
var provenanceColumns = []string{
	"Account Name",
	"Account ID",
	"Website",
	"Field",
	"Value",
	"Confidence",
	"Tier",
	"Source URL",
	"Reasoning",
}

// ProvenanceCSVExporter writes a tall-format provenance CSV with per-field
// source URLs and reasoning alongside each value.
type ProvenanceCSVExporter struct {
	outPath string

	mu      sync.Mutex
	results []*model.EnrichmentResult
}

// NewProvenanceCSVExporter creates a ProvenanceCSVExporter.
func NewProvenanceCSVExporter(outPath string) *ProvenanceCSVExporter {
	return &ProvenanceCSVExporter{outPath: outPath}
}

// Name implements ResultExporter.
func (e *ProvenanceCSVExporter) Name() string { return "provenance-csv" }

// ExportResult implements ResultExporter.
func (e *ProvenanceCSVExporter) ExportResult(_ context.Context, result *model.EnrichmentResult, _ *GateResult) error {
	e.mu.Lock()
	e.results = append(e.results, result)
	e.mu.Unlock()
	return nil
}

// Flush implements ResultExporter.
func (e *ProvenanceCSVExporter) Flush(_ context.Context) error {
	e.mu.Lock()
	results := e.results
	e.mu.Unlock()

	if len(results) == 0 {
		return nil
	}

	f, err := os.Create(e.outPath) // #nosec G304 -- path from CLI flag
	if err != nil {
		return eris.Wrap(err, "provenance export: create file")
	}
	defer f.Close() //nolint:errcheck

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(provenanceColumns); err != nil {
		return eris.Wrap(err, "provenance export: write header")
	}

	for _, r := range results {
		keys := make([]string, 0, len(r.FieldValues))
		for k := range r.FieldValues {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			fv := r.FieldValues[k]
			row := []string{
				r.Company.Name,
				r.Company.SalesforceID,
				stripScheme(r.Company.URL),
				fv.FieldKey,
				fmt.Sprintf("%v", fv.Value),
				fmt.Sprintf("%.2f", fv.Confidence),
				fmt.Sprintf("%d", fv.Tier),
				fv.Source,
				fv.Reasoning,
			}
			if err := w.Write(row); err != nil {
				return eris.Wrap(err, "provenance export: write row")
			}
		}
	}

	return nil
}
