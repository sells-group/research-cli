// Package adv provides Temporal workflows for ADV document OCR processing.
package adv

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/sells-group/research-cli/internal/temporal/sdk"
)

// SyncWorkflowParams configures the ADV document sync workflow.
type SyncWorkflowParams struct {
	ZIPURL        string
	TempDir       string
	Name          string // "adv_part2" or "adv_part3"
	Table         string
	SectionsTable string
	Columns       []string
	ConflictKeys  []string
	BatchSize     int
	FirmLimit     int
}

// DocumentSyncWorkflow orchestrates downloading, extracting, and OCR-ing ADV documents.
func DocumentSyncWorkflow(ctx workflow.Context, params SyncWorkflowParams) error {
	retryPolicy := &temporal.RetryPolicy{
		MaximumAttempts: 3,
	}

	// Long-running activity options for download/extract.
	longCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         retryPolicy,
	})

	// Shorter timeout for individual PDF processing.
	pdfCtx := workflow.WithActivityOptions(ctx, sdk.ShortActivityOptions())

	// Upsert activity options.
	upsertCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         retryPolicy,
	})

	// Step 1: Download ZIP.
	var zipPath string
	err := workflow.ExecuteActivity(longCtx, (*Activities).DownloadFOIAZIP, DownloadParams{
		ZIPURL:  params.ZIPURL,
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Get(ctx, &zipPath)
	if err != nil {
		return err
	}

	// Step 2: Extract and map PDFs.
	var mappings []DocMappingResult
	err = workflow.ExecuteActivity(longCtx, (*Activities).ExtractAndMapPDFs, ExtractParams{
		ZIPPath: zipPath,
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Get(ctx, &mappings)
	if err != nil {
		return err
	}

	// Apply firm limit.
	if params.FirmLimit > 0 && len(mappings) > params.FirmLimit {
		mappings = mappings[:params.FirmLimit]
	}

	// Step 3: Fan-out PDF processing. Launch all as futures, collect sequentially.
	// Filter to mappings with valid PDF paths.
	var validMappings []DocMappingResult
	for _, m := range mappings {
		if m.PDFPath != "" {
			validMappings = append(validMappings, m)
		}
	}

	// Execute all PDF activities and collect futures.
	futures := make([]workflow.Future, len(validMappings))
	for i, m := range validMappings {
		futures[i] = workflow.ExecuteActivity(pdfCtx, (*Activities).ProcessPDFViaDocling, ProcessPDFParams{
			PDFPath:   m.PDFPath,
			CRDNumber: m.CRDNumber,
			DocID:     m.DocID,
			DateFiled: m.DateFiled,
		})
	}

	// Collect results using BatchFlusher for auto-flushing at threshold.
	docFlusher := sdk.NewBatchFlusher(params.BatchSize, func(items [][]any) error {
		var n int64
		return workflow.ExecuteActivity(upsertCtx, (*Activities).UpsertDocumentBatch, UpsertParams{
			Table:        params.Table,
			Columns:      params.Columns,
			ConflictKeys: params.ConflictKeys,
			Rows:         items,
		}).Get(ctx, &n)
	})

	for _, f := range futures {
		var result ProcessPDFResult
		if err := f.Get(ctx, &result); err != nil {
			// Skip failed PDFs.
			continue
		}

		row := []any{
			result.CRDNumber, result.DocID, result.DateFiled,
			result.FullText, workflow.Now(ctx),
		}
		if err := docFlusher.Add(row); err != nil {
			return err
		}
	}

	// Flush remaining doc rows.
	if err := docFlusher.Flush(); err != nil {
		return err
	}

	// Flush remaining section rows (currently unused but preserved for future use).
	var sectionRows [][]any
	if len(sectionRows) > 0 {
		var n int64
		err := workflow.ExecuteActivity(upsertCtx, (*Activities).UpsertSectionBatch, UpsertSectionParams{
			Table:        params.SectionsTable,
			Columns:      sectionColumns(params.SectionsTable),
			ConflictKeys: sectionConflictKeys(params.SectionsTable),
			Rows:         sectionRows,
		}).Get(ctx, &n)
		if err != nil {
			return err
		}
	}

	return nil
}

// sectionColumns returns the column names for a sections table.
func sectionColumns(table string) []string {
	docIDCol := "brochure_id"
	if containsCRS(table) {
		docIDCol = "crs_id"
	}
	return []string{
		"crd_number", docIDCol, "section_key", "section_title",
		"text_content", "tables", "metadata", "updated_at",
	}
}

// sectionConflictKeys returns the conflict keys for a sections table.
func sectionConflictKeys(table string) []string {
	docIDCol := "brochure_id"
	if containsCRS(table) {
		docIDCol = "crs_id"
	}
	return []string{"crd_number", docIDCol, "section_key"}
}

// containsCRS checks if a table name contains "crs".
func containsCRS(table string) bool {
	for i := 0; i+2 < len(table); i++ {
		if table[i] == 'c' && table[i+1] == 'r' && table[i+2] == 's' {
			return true
		}
	}
	return false
}
