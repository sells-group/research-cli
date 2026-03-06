package adv

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/docling"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// Activities holds dependencies for Temporal activity implementations.
type Activities struct {
	Pool    db.Pool
	Fetcher fetcher.Fetcher
	Docling docling.Client
}

// DownloadParams holds input for the DownloadFOIAZIP activity.
type DownloadParams struct {
	ZIPURL  string
	TempDir string
	Name    string
}

// ExtractParams holds input for the ExtractAndMapPDFs activity.
type ExtractParams struct {
	ZIPPath string
	TempDir string
	Name    string
}

// DocMappingResult represents one row from a brochure/CRS mapping CSV.
type DocMappingResult struct {
	CRDNumber   int
	DocID       string
	DateFiled   string
	PDFFileName string
	PDFPath     string
}

// ProcessPDFParams holds input for the ProcessPDFViaDocling activity.
type ProcessPDFParams struct {
	PDFPath   string
	CRDNumber int
	DocID     string
	DateFiled string
}

// ProcessPDFResult holds the output of PDF processing via Docling.
type ProcessPDFResult struct {
	FullText    string
	CRDNumber   int
	DocID       string
	DateFiled   string
	SectionKeys []string
}

// UpsertParams holds input for the UpsertDocumentBatch activity.
type UpsertParams struct {
	Table        string
	Columns      []string
	ConflictKeys []string
	Rows         [][]any
}

// UpsertSectionParams holds input for the UpsertSectionBatch activity.
type UpsertSectionParams struct {
	Table        string
	Columns      []string
	ConflictKeys []string
	Rows         [][]any
}

// DownloadFOIAZIP downloads a ZIP file to temp dir and returns its path.
func (a *Activities) DownloadFOIAZIP(ctx context.Context, params DownloadParams) (string, error) {
	log := zap.L().With(zap.String("activity", "DownloadFOIAZIP"), zap.String("name", params.Name))

	if err := os.MkdirAll(params.TempDir, 0o750); err != nil {
		return "", eris.Wrap(err, "adv: create temp dir")
	}

	zipPath := filepath.Join(params.TempDir, params.Name+".zip")
	log.Info("downloading ZIP", zap.String("url", params.ZIPURL))

	if _, err := a.Fetcher.DownloadToFile(ctx, params.ZIPURL, zipPath); err != nil {
		return "", eris.Wrapf(err, "adv: download ZIP %s", params.Name)
	}

	log.Info("download complete", zap.String("path", zipPath))
	return zipPath, nil
}

// ExtractAndMapPDFs extracts a ZIP and parses the mapping CSV inside.
func (a *Activities) ExtractAndMapPDFs(ctx context.Context, params ExtractParams) ([]DocMappingResult, error) {
	log := zap.L().With(zap.String("activity", "ExtractAndMapPDFs"), zap.String("name", params.Name))

	extractDir := filepath.Join(params.TempDir, params.Name+"_extract")
	if err := os.MkdirAll(extractDir, 0o750); err != nil {
		return nil, eris.Wrap(err, "adv: create extract dir")
	}

	extractedFiles, err := fetcher.ExtractZIP(params.ZIPPath, extractDir)
	if err != nil {
		return nil, eris.Wrapf(err, "adv: extract ZIP %s", params.Name)
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
		return nil, eris.Errorf("adv: mapping CSV not found in ZIP %s", params.Name)
	}

	// Find all PDFs by filename for resolution.
	pdfIndex := make(map[string]string) // lowercase base name -> full path
	for _, fp := range extractedFiles {
		if strings.HasSuffix(strings.ToLower(fp), ".pdf") {
			pdfIndex[strings.ToLower(filepath.Base(fp))] = fp
		}
	}

	// Parse mapping CSV.
	csvFile, err := os.Open(mappingPath) //#nosec G304 -- path from our own ZIP extraction
	if err != nil {
		return nil, eris.Wrapf(err, "adv: open mapping CSV %s", params.Name)
	}
	defer csvFile.Close() //nolint:errcheck

	rowCh, errCh := fetcher.StreamCSV(ctx, csvFile, fetcher.CSVOptions{HasHeader: true})

	var results []DocMappingResult
	for row := range rowCh {
		if len(row) < 4 {
			continue
		}

		pdfName := row[3]
		pdfPath := ""
		if p, ok := pdfIndex[strings.ToLower(filepath.Base(pdfName))]; ok {
			pdfPath = p
		}

		results = append(results, DocMappingResult{
			CRDNumber:   parseInt(row[0]),
			DocID:       row[1],
			DateFiled:   row[2],
			PDFFileName: pdfName,
			PDFPath:     pdfPath,
		})
	}

	if err := <-errCh; err != nil {
		return nil, eris.Wrapf(err, "adv: parse mapping CSV %s", params.Name)
	}

	log.Info("extracted and mapped PDFs", zap.Int("mappings", len(results)))

	// Check context after potentially long extraction.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return results, nil
}

// ProcessPDFViaDocling OCRs a single PDF via the Docling service.
func (a *Activities) ProcessPDFViaDocling(ctx context.Context, params ProcessPDFParams) (*ProcessPDFResult, error) {
	log := zap.L().With(
		zap.String("activity", "ProcessPDFViaDocling"),
		zap.Int("crd", params.CRDNumber),
		zap.String("doc_id", params.DocID),
	)

	pdfData, err := os.ReadFile(params.PDFPath)
	if err != nil {
		return nil, eris.Wrapf(err, "adv: read PDF %s", params.PDFPath)
	}

	doc, err := a.Docling.Convert(ctx, pdfData, docling.ConvertOpts{})
	if err != nil {
		return nil, eris.Wrapf(err, "adv: docling convert CRD %d doc %s", params.CRDNumber, params.DocID)
	}

	// Build full text and collect section keys from headings.
	var fullText strings.Builder
	var sectionKeys []string
	for _, page := range doc.Pages {
		for _, el := range page.Elements {
			if el.Text != "" {
				fullText.WriteString(el.Text)
				fullText.WriteString("\n")
			}
			if el.Type == "heading" && el.Text != "" {
				sectionKeys = append(sectionKeys, el.Text)
			}
		}
	}

	log.Debug("processed PDF", zap.Int("text_len", fullText.Len()), zap.Int("sections", len(sectionKeys)))

	return &ProcessPDFResult{
		FullText:    fullText.String(),
		CRDNumber:   params.CRDNumber,
		DocID:       params.DocID,
		DateFiled:   params.DateFiled,
		SectionKeys: sectionKeys,
	}, nil
}

// UpsertDocumentBatch upserts a batch of document rows.
func (a *Activities) UpsertDocumentBatch(ctx context.Context, params UpsertParams) (int64, error) {
	n, err := db.BulkUpsert(ctx, a.Pool, db.UpsertConfig{
		Table:        params.Table,
		Columns:      params.Columns,
		ConflictKeys: params.ConflictKeys,
	}, params.Rows)
	if err != nil {
		return 0, eris.Wrapf(err, "adv: upsert documents to %s", params.Table)
	}
	return n, nil
}

// UpsertSectionBatch upserts a batch of section rows.
func (a *Activities) UpsertSectionBatch(ctx context.Context, params UpsertSectionParams) (int64, error) {
	n, err := db.BulkUpsert(ctx, a.Pool, db.UpsertConfig{
		Table:        params.Table,
		Columns:      params.Columns,
		ConflictKeys: params.ConflictKeys,
	}, params.Rows)
	if err != nil {
		return 0, eris.Wrapf(err, "adv: upsert sections to %s", params.Table)
	}
	return n, nil
}

// parseInt converts a string to int, returning 0 on failure.
func parseInt(s string) int {
	s = strings.TrimSpace(s)
	var n int
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}
