package pipeline

import (
	"context"
	"sync"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// ExportFormat identifies the CSV export format.
type ExportFormat string

// ExportFormatSFReport and ExportFormatGrata enumerate CSV export formats.
const (
	ExportFormatSFReport ExportFormat = "sf-report-csv"
	ExportFormatGrata    ExportFormat = "grata-csv"
)

// CSVExporter collects enrichment results and writes a CSV file on Flush.
type CSVExporter struct {
	format    ExportFormat
	outPath   string
	originals []SFReportCompany

	mu      sync.Mutex
	results []*model.EnrichmentResult
}

// NewCSVExporter creates a CSVExporter. originals is only used for SF report format.
func NewCSVExporter(format ExportFormat, outPath string, originals []SFReportCompany) *CSVExporter {
	return &CSVExporter{
		format:    format,
		outPath:   outPath,
		originals: originals,
	}
}

// Name implements ResultExporter.
func (e *CSVExporter) Name() string { return "csv-" + string(e.format) }

// ExportResult implements ResultExporter.
func (e *CSVExporter) ExportResult(_ context.Context, result *model.EnrichmentResult, _ *GateResult) error {
	e.mu.Lock()
	e.results = append(e.results, result)
	e.mu.Unlock()
	return nil
}

// Flush implements ResultExporter.
func (e *CSVExporter) Flush(_ context.Context) error {
	e.mu.Lock()
	results := e.results
	e.mu.Unlock()

	if len(results) == 0 {
		return nil
	}

	switch e.format {
	case ExportFormatSFReport:
		return ExportSFReportCSV(results, e.originals, e.outPath)
	case ExportFormatGrata:
		return ExportGrataCSV(results, e.outPath)
	default:
		return eris.Errorf("csv exporter: unknown format %q", e.format)
	}
}
