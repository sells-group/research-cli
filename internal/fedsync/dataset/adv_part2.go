package dataset

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/ocr"
)

const (
	advPart2BatchSize = 100
	advPart2FirmLimit = 500
)

// ADVPart2 syncs SEC ADV Part 2 brochure PDFs → OCR → text.
// Downloads the monthly bulk brochure ZIP, extracts mapping CSV + PDFs,
// OCRs each PDF and upserts into adv_brochures.
type ADVPart2 struct {
	cfg *config.Config
}

// Name implements Dataset.
func (d *ADVPart2) Name() string { return "adv_part2" }

// Table implements Dataset.
func (d *ADVPart2) Table() string { return "fed_data.adv_brochures" }

// Phase implements Dataset.
func (d *ADVPart2) Phase() Phase { return Phase2 }

// Cadence implements Dataset.
func (d *ADVPart2) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *ADVPart2) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync fetches and loads SEC ADV Part 2 brochure data.
func (d *ADVPart2) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	ext, err := ocr.NewExtractor(d.cfg.Fedsync.OCR, d.cfg.Fedsync.MistralKey)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: create OCR extractor")
	}

	// Fetch IAPD reports metadata to find the latest brochure ZIP URL.
	meta, err := fetchFOIAMetadata(ctx, f)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: fetch FOIA metadata")
	}

	url, err := latestFileURL(meta.ADVBrochures, "advBrochures")
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: resolve brochure URL")
	}
	log.Info("downloading brochure ZIP", zap.String("url", url))

	zipPath := filepath.Join(tempDir, "adv_brochures.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "adv_part2: download brochure ZIP")
	}
	defer os.Remove(zipPath) //nolint:errcheck

	// Extract ZIP to temp dir.
	extractDir := filepath.Join(tempDir, "adv_brochures_extract")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return nil, eris.Wrap(err, "adv_part2: create extract dir")
	}
	defer os.RemoveAll(extractDir) //nolint:errcheck

	extractedFiles, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: extract brochure ZIP")
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
	if mappingPath == "" {
		return nil, eris.New("adv_part2: mapping CSV not found in brochure ZIP")
	}

	// Parse mapping CSV.
	mappings, err := parseBrochureMapping(mappingPath)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: parse mapping CSV")
	}
	log.Info("parsed brochure mapping", zap.Int("entries", len(mappings)))

	// Limit to first N firms to avoid extremely long OCR runs.
	if len(mappings) > advPart2FirmLimit {
		mappings = mappings[:advPart2FirmLimit]
	}

	columns := []string{"crd_number", "brochure_id", "filing_date", "text_content", "extracted_at"}
	conflictKeys := []string{"crd_number", "brochure_id"}

	var batch [][]any
	var totalRows int64

	for _, m := range mappings {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Find the PDF file in the extracted directory.
		pdfPath := findPDFInExtracted(extractDir, m.PDFFileName)
		if pdfPath == "" {
			log.Debug("PDF not found in extract", zap.String("pdf", m.PDFFileName), zap.Int("crd", m.CRDNumber))
			continue
		}

		text, err := ext.ExtractText(ctx, pdfPath)
		if err != nil {
			log.Debug("skipping brochure OCR", zap.Int("crd", m.CRDNumber), zap.Error(err))
			continue
		}

		now := time.Now()
		batch = append(batch, []any{m.CRDNumber, m.BrochureID, m.DateFiled, text, now})

		if len(batch) >= advPart2BatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part2: bulk upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        d.Table(),
			Columns:      columns,
			ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part2: bulk upsert final batch")
		}
		totalRows += n
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"brochures_processed": len(mappings)},
	}, nil
}

// brochureMapping represents one row from the brochure mapping CSV.
type brochureMapping struct {
	CRDNumber   int
	BrochureID  string
	DateFiled   string
	PDFFileName string
}

// parseBrochureMapping reads the mapping CSV from the brochure ZIP.
func parseBrochureMapping(path string) ([]brochureMapping, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, eris.Wrap(err, "open mapping CSV")
	}
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "read mapping header")
	}
	colIdx := mapColumns(header)

	var result []brochureMapping
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		crd := parseIntOr(trimQuotes(getCol(record, colIdx, "crdnumber")), 0)
		if crd == 0 {
			continue
		}

		brochureID := trimQuotes(getCol(record, colIdx, "brochureid"))
		if brochureID == "" {
			continue
		}

		pdfFileName := trimQuotes(getCol(record, colIdx, "pdffilename"))
		if pdfFileName == "" {
			continue
		}

		dateFiled := trimQuotes(getCol(record, colIdx, "datefiled"))

		result = append(result, brochureMapping{
			CRDNumber:   crd,
			BrochureID:  brochureID,
			DateFiled:   dateFiled,
			PDFFileName: pdfFileName,
		})
	}

	return result, nil
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
