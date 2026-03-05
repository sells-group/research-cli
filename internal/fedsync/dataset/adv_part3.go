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

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/ocr"
)

const (
	advPart3BatchSize = 100
	advPart3FirmLimit = 500
)

// CRS section keys.
const (
	crsSectionRelationshipsServices = "relationships_services"
	crsSectionFeesCosts             = "fees_costs"
	crsSectionDisciplinary          = "disciplinary"
	crsSectionConversationStarters  = "conversation_starters"
	crsSectionAdditionalInfo        = "additional_info"
)

// crsHeadingMap maps normalized CRS heading text to section keys.
var crsHeadingMap = map[string]string{
	"relationships and services":                                                crsSectionRelationshipsServices,
	"what investment services and advice can you provide me":                    crsSectionRelationshipsServices,
	"fees costs conflicts and standard of conduct":                              crsSectionFeesCosts,
	"fees, costs, conflicts, and standard of conduct":                           crsSectionFeesCosts,
	"what fees will i pay":                                                      crsSectionFeesCosts,
	"disciplinary history":                                                      crsSectionDisciplinary,
	"do you or your financial professionals have legal or disciplinary history": crsSectionDisciplinary,
	"conversation starters":                                                     crsSectionConversationStarters,
	"key questions to ask":                                                      crsSectionConversationStarters,
	"additional information":                                                    crsSectionAdditionalInfo,
	"who is my primary contact":                                                 crsSectionAdditionalInfo,
}

// ADVPart3 syncs CRS (Client Relationship Summary) PDFs → OCR → text + sections.
type ADVPart3 struct {
	cfg *config.Config
}

// Name implements Dataset.
func (d *ADVPart3) Name() string { return "adv_part3" }

// Table implements Dataset.
func (d *ADVPart3) Table() string { return "fed_data.adv_crs" }

// Phase implements Dataset.
func (d *ADVPart3) Phase() Phase { return Phase3 }

// Cadence implements Dataset.
func (d *ADVPart3) Cadence() Cadence { return Monthly }

// ShouldRun implements Dataset.
func (d *ADVPart3) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return MonthlySchedule(now, lastSync)
}

// Sync fetches and loads CRS (Client Relationship Summary) data.
func (d *ADVPart3) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	ext, structExt, err := buildExtractors(d.cfg)
	if err != nil {
		return nil, eris.Wrap(err, "adv_part3: create extractor")
	}

	return syncADVDocuments(ctx, pool, f, ext, structExt, tempDir, advDocSyncConfig{
		name:          d.Name(),
		foiaSelector:  func(m *foiaReportsMetadata) []foiaFileEntry { return m.ADVFirmCRSDocs },
		table:         d.Table(),
		sectionsTable: "fed_data.adv_crs_sections",
		columns:       []string{"crd_number", "crs_id", "filing_date", "text_content", "extracted_at"},
		conflictKeys:  []string{"crd_number", "crs_id"},
		batchSize:     advPart3BatchSize,
		firmLimit:     advPart3FirmLimit,
		parseMapping:  parseCRSMappingDoc,
		pdfFallback:   crsMappingsFromPDFsDoc,
		sectionParser: parseCRSSections,
	})
}

// parseCRSSections extracts the 5 CRS sections from a Docling-parsed document.
func parseCRSSections(doc *ocr.StructuredDocument, crd int, docID string) []sectionRow {
	if doc == nil || len(doc.Sections) == 0 {
		return nil
	}

	var rows []sectionRow
	for _, sec := range doc.Sections {
		key := matchCRSSectionKey(sec.Title)
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

// matchCRSSectionKey maps a heading title to a CRS section key.
func matchCRSSectionKey(title string) string {
	normalized := strings.ToLower(strings.TrimSpace(title))
	// Remove trailing question marks and punctuation for fuzzy matching.
	normalized = strings.TrimRight(normalized, "?!.")
	normalized = strings.TrimSpace(normalized)

	if key, ok := crsHeadingMap[normalized]; ok {
		return key
	}

	// Substring matching for common CRS headings.
	for pattern, key := range crsHeadingMap {
		if strings.Contains(normalized, pattern) {
			return key
		}
	}
	return ""
}

// parseCRSMappingDoc reads the CRS mapping CSV and returns docMapping entries.
func parseCRSMappingDoc(path string) ([]docMapping, error) {
	f, err := os.Open(path) // #nosec G304 -- path from extracted ZIP in trusted temp directory
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

		// Try CRS-specific column names, then fallbacks.
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

		result = append(result, docMapping{
			CRDNumber:   crd,
			DocID:       crsID,
			DateFiled:   trimQuotes(getCol(record, colIdx, "datefiled")),
			PDFFileName: pdfFileName,
		})
	}
	return result, nil
}

// crsMappingsFromPDFsDoc builds CRS mappings from PDF filenames when no mapping CSV exists.
func crsMappingsFromPDFsDoc(files []string) []docMapping {
	var result []docMapping
	for _, fp := range files {
		base := filepath.Base(fp)
		if !strings.HasSuffix(strings.ToLower(base), ".pdf") {
			continue
		}

		name := strings.TrimSuffix(base, filepath.Ext(base))
		name = strings.ToLower(name)
		numStr := strings.TrimPrefix(name, "crs_")

		crd := parseIntOr(numStr, 0)
		if crd == 0 {
			continue
		}

		result = append(result, docMapping{
			CRDNumber:   crd,
			DocID:       "crs_" + numStr,
			PDFFileName: base,
		})
	}
	return result
}
