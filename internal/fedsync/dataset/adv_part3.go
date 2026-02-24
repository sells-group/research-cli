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
	advPart3BatchSize = 100
	advPart3FirmLimit = 500
)

// ADVPart3 syncs CRS (Client Relationship Summary) PDFs → OCR → text.
// Downloads the monthly bulk CRS ZIP from the FOIA metadata API,
// extracts mapping CSV + PDFs, OCRs each PDF and upserts into adv_crs.
type ADVPart3 struct {
	cfg *config.Config
}

func (d *ADVPart3) Name() string     { return "adv_part3" }
func (d *ADVPart3) Table() string    { return "fed_data.adv_crs" }
func (d *ADVPart3) Phase() Phase     { return Phase3 }
func (d *ADVPart3) Cadence() Cadence { return Monthly }

func (d *ADVPart3) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

func (d *ADVPart3) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", d.Name()))

	ext, err := ocr.NewExtractor(d.cfg.Fedsync.OCR, d.cfg.Fedsync.MistralKey)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: create OCR extractor")
	}

	// Fetch IAPD reports metadata to find the latest CRS ZIP URL.
	meta, err := fetchFOIAMetadata(ctx, f)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: fetch FOIA metadata")
	}

	url, err := latestFileURL(meta.ADVFirmCRSDocs, "advFirmCRSDocs")
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: resolve CRS URL")
	}
	log.Info("downloading CRS ZIP", zap.String("url", url))

	zipPath := filepath.Join(tempDir, "adv_crs.zip")
	if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
		return nil, eris.Wrap(err, "adv_part3: download CRS ZIP")
	}
	defer os.Remove(zipPath) //nolint:errcheck

	// Extract ZIP to temp dir.
	extractDir := filepath.Join(tempDir, "adv_crs_extract")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return nil, eris.Wrap(err, "adv_part3: create extract dir")
	}
	defer os.RemoveAll(extractDir) //nolint:errcheck

	extractedFiles, err := fetcher.ExtractZIP(zipPath, extractDir)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: extract CRS ZIP")
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

	var mappings []crsMapping
	if mappingPath != "" {
		mappings, err = parseCRSMapping(mappingPath)
		if err != nil {
			return nil, eris.Wrap(err, "adv_part3: parse mapping CSV")
		}
	} else {
		// Fallback: treat all PDFs in the ZIP as CRS docs.
		log.Warn("no mapping CSV found in CRS ZIP, falling back to PDF filenames")
		mappings = crsMappingsFromPDFs(extractedFiles)
	}
	log.Info("parsed CRS mapping", zap.Int("entries", len(mappings)))

	// Limit to first N entries to avoid extremely long OCR runs.
	if len(mappings) > advPart3FirmLimit {
		mappings = mappings[:advPart3FirmLimit]
	}

	columns := []string{"crd_number", "crs_id", "filing_date", "text_content", "extracted_at"}
	conflictKeys := []string{"crd_number", "crs_id"}

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
			log.Debug("skipping CRS OCR", zap.Int("crd", m.CRDNumber), zap.Error(err))
			continue
		}

		now := time.Now()
		batch = append(batch, []any{m.CRDNumber, m.CRSID, m.DateFiled, text, now})

		if len(batch) >= advPart3BatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        d.Table(),
				Columns:      columns,
				ConflictKeys: conflictKeys,
			}, batch)
			if err != nil {
				return nil, eris.Wrap(err, "adv_part3: bulk upsert")
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
			return nil, eris.Wrap(err, "adv_part3: bulk upsert final batch")
		}
		totalRows += n
	}

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata:   map[string]any{"crs_processed": len(mappings)},
	}, nil
}

// crsMapping represents one row from the CRS mapping CSV.
type crsMapping struct {
	CRDNumber   int
	CRSID       string
	DateFiled   string
	PDFFileName string
}

// parseCRSMapping reads the mapping CSV from the CRS ZIP.
func parseCRSMapping(path string) ([]crsMapping, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, eris.Wrap(err, "open CRS mapping CSV")
	}
	defer f.Close() //nolint:errcheck

	reader := csv.NewReader(f)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		return nil, eris.Wrap(err, "read CRS mapping header")
	}
	colIdx := mapColumns(header)

	var result []crsMapping
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

		// Try CRS-specific column names, then brochure-style fallbacks.
		crsID := trimQuotes(getCol(record, colIdx, "crsid"))
		if crsID == "" {
			crsID = trimQuotes(getCol(record, colIdx, "documentid"))
		}
		if crsID == "" {
			crsID = trimQuotes(getCol(record, colIdx, "brochureid"))
		}
		if crsID == "" {
			continue
		}

		pdfFileName := trimQuotes(getCol(record, colIdx, "pdffilename"))
		if pdfFileName == "" {
			pdfFileName = trimQuotes(getCol(record, colIdx, "filename"))
		}
		if pdfFileName == "" {
			continue
		}

		dateFiled := trimQuotes(getCol(record, colIdx, "datefiled"))

		result = append(result, crsMapping{
			CRDNumber:   crd,
			CRSID:       crsID,
			DateFiled:   dateFiled,
			PDFFileName: pdfFileName,
		})
	}

	return result, nil
}

// crsMappingsFromPDFs builds CRS mappings from PDF filenames when no mapping CSV exists.
// Attempts to derive CRD from filename patterns like "crs_12345.pdf" or "12345.pdf".
func crsMappingsFromPDFs(files []string) []crsMapping {
	var result []crsMapping
	for _, fp := range files {
		base := filepath.Base(fp)
		if !strings.HasSuffix(strings.ToLower(base), ".pdf") {
			continue
		}

		name := strings.TrimSuffix(base, filepath.Ext(base))
		name = strings.ToLower(name)

		// Try patterns: "crs_12345", "12345", etc.
		numStr := name
		numStr = strings.TrimPrefix(numStr, "crs_")

		crd := parseIntOr(numStr, 0)
		if crd == 0 {
			continue
		}

		result = append(result, crsMapping{
			CRDNumber:   crd,
			CRSID:       "crs_" + numStr,
			PDFFileName: base,
		})
	}
	return result
}
