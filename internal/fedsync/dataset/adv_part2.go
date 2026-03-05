package dataset

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/docling"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/ocr"
)

const (
	advPart2BatchSize = 100
	advPart2FirmLimit = 500
)

// ADVPart2 syncs SEC ADV Part 2 brochure PDFs → OCR → text + sections.
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
	ext, structExt, err := buildExtractors(d.cfg)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part2: create extractor")
	}

	return syncADVDocuments(ctx, pool, f, ext, structExt, tempDir, advDocSyncConfig{
		name:          d.Name(),
		foiaSelector:  func(m *foiaReportsMetadata) []foiaFileEntry { return m.ADVBrochures },
		table:         d.Table(),
		sectionsTable: "fed_data.adv_brochure_sections",
		columns:       []string{"crd_number", "brochure_id", "filing_date", "text_content", "extracted_at"},
		conflictKeys:  []string{"crd_number", "brochure_id"},
		batchSize:     advPart2BatchSize,
		firmLimit:     advPart2FirmLimit,
		parseMapping:  parseBrochureMappingDoc,
		sectionParser: parseBrochureSections,
	})
}

// brochureItemPattern matches Item N headers in ADV Part 2 brochures.
var brochureItemPattern = regexp.MustCompile(
	`(?i)^[\s]*item\s+(\d{1,2})\s*[:\-–—.\s]+\s*(.*)$`,
)

// parseBrochureSections extracts Item 1-18 sections from a Docling-parsed brochure.
func parseBrochureSections(doc *ocr.StructuredDocument, crd int, docID string) []sectionRow {
	if doc == nil || len(doc.Sections) == 0 {
		return nil
	}

	var rows []sectionRow
	for _, sec := range doc.Sections {
		key := matchBrochureItemKey(sec.Title)
		if key == "" {
			continue
		}

		meta := extractSectionMetadata(sec.Text)
		rows = append(rows, sectionRow{
			CRDNumber:   crd,
			DocID:       docID,
			SectionKey:  key,
			Title:       sec.Title,
			TextContent: sec.Text,
			Tables:      tablesToJSON(sec.Tables),
			Metadata:    marshalJSONB(meta),
		})
	}
	return rows
}

// matchBrochureItemKey extracts an item key like "item_4" from a heading title.
func matchBrochureItemKey(title string) string {
	m := brochureItemPattern.FindStringSubmatch(title)
	if m == nil {
		return ""
	}
	num := parseIntOr(m[1], 0)
	if num < 1 || num > 18 {
		return ""
	}
	return fmt.Sprintf("item_%d", num)
}

// parseBrochureMappingDoc reads the brochure mapping CSV and returns docMapping entries.
func parseBrochureMappingDoc(path string) ([]docMapping, error) {
	f, err := os.Open(path) // #nosec G304 -- path from extracted ZIP in trusted temp directory
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

	var result []docMapping
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

		result = append(result, docMapping{
			CRDNumber:   crd,
			DocID:       brochureID,
			DateFiled:   trimQuotes(getCol(record, colIdx, "datefiled")),
			PDFFileName: pdfFileName,
		})
	}
	return result, nil
}

// buildExtractors creates an Extractor and optional StructuredExtractor based on config.
func buildExtractors(cfg *config.Config) (ocr.Extractor, ocr.StructuredExtractor, error) {
	if cfg.Fedsync.DoclingURL != "" && cfg.Fedsync.OCR.Provider == "docling" {
		client := docling.NewClient(cfg.Fedsync.DoclingURL, cfg.Fedsync.DoclingAPIKey)
		ext := ocr.NewDoclingExtractor(client)
		return ext, ext, nil
	}

	ext, err := ocr.NewExtractor(cfg.Fedsync.OCR, cfg.Fedsync.MistralKey)
	if err != nil {
		return nil, nil, err
	}
	return ext, nil, nil
}
