package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// JSONExporter writes full EnrichmentResults as indented JSON.
// If outPath is empty, writes to stdout.
type JSONExporter struct {
	outPath string

	mu      sync.Mutex
	results []*model.EnrichmentResult
}

// NewJSONExporter creates a JSONExporter. An empty outPath writes to stdout.
func NewJSONExporter(outPath string) *JSONExporter {
	return &JSONExporter{outPath: outPath}
}

// Name implements ResultExporter.
func (e *JSONExporter) Name() string { return "json" }

// ExportResult implements ResultExporter.
func (e *JSONExporter) ExportResult(_ context.Context, result *model.EnrichmentResult, _ *GateResult) error {
	e.mu.Lock()
	e.results = append(e.results, result)
	e.mu.Unlock()
	return nil
}

// Flush implements ResultExporter.
func (e *JSONExporter) Flush(_ context.Context) error {
	e.mu.Lock()
	results := e.results
	e.mu.Unlock()

	if len(results) == 0 {
		return nil
	}

	var w *os.File
	if e.outPath != "" {
		f, err := os.Create(e.outPath) // #nosec G304 -- path from CLI flag
		if err != nil {
			return eris.Wrap(err, "json export: create file")
		}
		defer f.Close() //nolint:errcheck
		w = f
	} else {
		w = os.Stdout
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
