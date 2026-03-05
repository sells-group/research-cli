package dataset

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/ocr"
)

// advDocSyncConfig parameterizes the shared ADV document sync flow for Part 2/3.
type advDocSyncConfig struct {
	name          string
	foiaSelector  func(*foiaReportsMetadata) []foiaFileEntry
	table         string // fed_data.adv_brochures / fed_data.adv_crs
	sectionsTable string // fed_data.adv_brochure_sections / fed_data.adv_crs_sections
	columns       []string
	conflictKeys  []string
	batchSize     int
	firmLimit     int
	parseMapping  func(string) ([]docMapping, error)
	pdfFallback   func([]string) []docMapping // nil for Part 2
	sectionParser func(*ocr.StructuredDocument, int, string) []sectionRow
}

// docMapping represents one row from a brochure/CRS mapping CSV.
type docMapping struct {
	CRDNumber   int
	DocID       string
	DateFiled   string
	PDFFileName string
}

// sectionRow holds a parsed section ready for DB insertion.
type sectionRow struct {
	CRDNumber   int
	DocID       string
	SectionKey  string
	Title       string
	TextContent string
	Tables      json.RawMessage // JSONB
	Metadata    json.RawMessage // JSONB
}

// syncADVDocuments is the shared sync implementation for ADV Part 2 and Part 3.
// It downloads a ZIP, extracts PDFs, OCRs them, and upserts both flat text and
// structured sections into the database.
func syncADVDocuments(
	ctx context.Context,
	pool db.Pool,
	f fetcher.Fetcher,
	ext ocr.Extractor,
	structExt ocr.StructuredExtractor,
	tempDir string,
	cfg advDocSyncConfig,
) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", cfg.name))

	// Fetch IAPD reports metadata to find the latest ZIP URL.
	meta, err := fetchFOIAMetadata(ctx, f)
	if err != nil {
		return nil, eris.Wrapf(err, "%s: fetch FOIA metadata", cfg.name)
	}

	entries := cfg.foiaSelector(meta)
	foiaType := cfg.name
	if strings.Contains(cfg.name, "part2") {
		foiaType = "advBrochures"
	} else if strings.Contains(cfg.name, "part3") {
		foiaType = "advFirmCRSDocs"
	}

	url, err := latestFileURL(entries, foiaType)
	if err != nil {
		return nil, eris.Wrapf(err, "%s: resolve URL", cfg.name)
	}
	log.Info("downloading ZIP", zap.String("url", url))

	zipPath := filepath.Join(tempDir, cfg.name+".zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrapf(err, "%s: download ZIP", cfg.name)
	}
	defer os.Remove(zipPath) //nolint:errcheck

	// Extract ZIP to temp dir.
	extractDir := filepath.Join(tempDir, cfg.name+"_extract")
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrapf(err, "%s: create extract dir", cfg.name)
	}
	defer os.RemoveAll(extractDir) //nolint:errcheck

	extractedFiles, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrapf(err, "%s: extract ZIP", cfg.name)
	}

	// Find mapping CSV.
	var mappingPath string
	for _, fp := range extractedFiles {
		base := strings.ToLower(filepath.Base(fp))
		if strings.Contains(base, "mapping") && strings.HasSuffix(base, ".csv") {
			mappingPath = fp
			break
		}
	}

	var mappings []docMapping
	if mappingPath != "" {
		mappings, err = cfg.parseMapping(mappingPath)
		if err != nil {
			return nil, eris.Wrapf(err, "%s: parse mapping CSV", cfg.name)
		}
	} else if cfg.pdfFallback != nil {
		log.Warn("no mapping CSV found, falling back to PDF filenames")
		mappings = cfg.pdfFallback(extractedFiles)
	} else {
		return nil, eris.Errorf("%s: mapping CSV not found in ZIP", cfg.name)
	}
	log.Info("parsed mapping", zap.Int("entries", len(mappings)))

	// Limit to avoid extremely long OCR runs.
	if len(mappings) > cfg.firmLimit {
		mappings = mappings[:cfg.firmLimit]
	}

	var docBatch [][]any
	var sectionBatch [][]any
	var totalRows int64

	sectionCols := []string{
		"crd_number", "brochure_id", "section_key", "section_title",
		"text_content", "tables", "metadata", "updated_at",
	}
	sectionConflict := []string{"crd_number", "brochure_id", "section_key"}
	if strings.Contains(cfg.sectionsTable, "crs") {
		sectionCols[1] = "crs_id"
		sectionConflict[1] = "crs_id"
	}

	for _, m := range mappings {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		pdfPath := findPDFInExtracted(extractDir, m.PDFFileName)
		if pdfPath == "" {
			log.Debug("PDF not found", zap.String("pdf", m.PDFFileName), zap.Int("crd", m.CRDNumber))
			continue
		}

		var text string

		// If structured extractor available, use it for both text and sections.
		if structExt != nil && cfg.sectionParser != nil {
			structDoc, err := structExt.ExtractStructured(ctx, pdfPath)
			if err != nil {
				log.Debug("skipping structured OCR", zap.Int("crd", m.CRDNumber), zap.Error(err))
				continue
			}
			text = structDoc.FullText

			// Parse and batch sections.
			sections := cfg.sectionParser(structDoc, m.CRDNumber, m.DocID)
			for _, s := range sections {
				sectionBatch = append(sectionBatch, []any{
					s.CRDNumber, s.DocID, s.SectionKey, s.Title,
					s.TextContent, s.Tables, s.Metadata, time.Now(),
				})
			}

			// Flush section batch.
			if len(sectionBatch) >= cfg.batchSize*5 {
				if _, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
					Table:        cfg.sectionsTable,
					Columns:      sectionCols,
					ConflictKeys: sectionConflict,
				}, sectionBatch); err != nil {
					return nil, eris.Wrapf(err, "%s: upsert sections", cfg.name)
				}
				sectionBatch = sectionBatch[:0]
			}
		} else {
			// Fall back to flat text extraction.
			text, err = ext.ExtractText(ctx, pdfPath)
			if err != nil {
				log.Debug("skipping OCR", zap.Int("crd", m.CRDNumber), zap.Error(err))
				continue
			}
		}

		now := time.Now()
		docBatch = append(docBatch, []any{m.CRDNumber, m.DocID, m.DateFiled, text, now})

		if len(docBatch) >= cfg.batchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        cfg.table,
				Columns:      cfg.columns,
				ConflictKeys: cfg.conflictKeys,
			}, docBatch)
			if err != nil {
				return nil, eris.Wrapf(err, "%s: bulk upsert", cfg.name)
			}
			totalRows += n
			docBatch = docBatch[:0]
		}
	}

	// Flush final doc batch.
	if len(docBatch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        cfg.table,
			Columns:      cfg.columns,
			ConflictKeys: cfg.conflictKeys,
		}, docBatch)
		if err != nil {
			return nil, eris.Wrapf(err, "%s: bulk upsert final", cfg.name)
		}
		totalRows += n
	}

	// Flush final section batch.
	if len(sectionBatch) > 0 {
		if _, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        cfg.sectionsTable,
			Columns:      sectionCols,
			ConflictKeys: sectionConflict,
		}, sectionBatch); err != nil {
			return nil, eris.Wrapf(err, "%s: upsert sections final", cfg.name)
		}
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"docs_processed": len(mappings)},
	}, nil
}

// findPDFInExtracted searches for a PDF file in the extracted directory tree.
func findPDFInExtracted(dir, pdfName string) string {
	if pdfName == "" {
		return ""
	}

	// Try direct path first.
	direct := filepath.Join(dir, pdfName)
	if _, err := os.Stat(direct); err == nil {
		return direct
	}

	// Walk directory to find the file by base name.
	target := strings.ToLower(filepath.Base(pdfName))
	var found string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.ToLower(filepath.Base(path)) == target {
			found = path
			return filepath.SkipAll
		}
		return nil
	})

	return found
}

// Regex patterns for metadata extraction.
var (
	dollarPattern  = regexp.MustCompile(`\$[\d,]+(?:\.\d{2})?`)
	percentPattern = regexp.MustCompile(`\d+(?:\.\d+)?%`)
	phonePattern   = regexp.MustCompile(`\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}`)
	urlPattern     = regexp.MustCompile(`https?://[^\s]+`)
	yesNoPattern   = regexp.MustCompile(`(?i)\b(yes|no)\b`)
)

// extractSectionMetadata parses dollar amounts, percentages, phone numbers,
// URLs, and yes/no flags from section text.
func extractSectionMetadata(text string) map[string]any {
	meta := make(map[string]any)

	if dollars := dollarPattern.FindAllString(text, -1); len(dollars) > 0 {
		meta["dollar_amounts"] = dollars
	}
	if pcts := percentPattern.FindAllString(text, -1); len(pcts) > 0 {
		meta["percentages"] = pcts
	}
	if phones := phonePattern.FindAllString(text, -1); len(phones) > 0 {
		meta["phone_numbers"] = phones
	}
	if urls := urlPattern.FindAllString(text, -1); len(urls) > 0 {
		meta["urls"] = urls
	}
	if yns := yesNoPattern.FindAllString(text, -1); len(yns) > 0 {
		meta["yes_no_flags"] = yns
	}

	if len(meta) == 0 {
		return nil
	}
	return meta
}

// marshalJSONB marshals a value to json.RawMessage for JSONB storage.
// Returns nil (SQL NULL) if the value is nil.
func marshalJSONB(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return json.RawMessage(b)
}

// tablesToJSON converts OCR TableData slices to JSONB.
func tablesToJSON(tables []ocr.TableData) json.RawMessage {
	if len(tables) == 0 {
		return nil
	}
	return marshalJSONB(tables)
}
