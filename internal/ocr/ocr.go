package ocr

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/docling"
)

// Extractor extracts text content from PDF files.
type Extractor interface {
	ExtractText(ctx context.Context, pdfPath string) (string, error)
}

// StructuredExtractor provides structured PDF parsing with section and table extraction.
type StructuredExtractor interface {
	ExtractStructured(ctx context.Context, pdfPath string) (*StructuredDocument, error)
}

// StructuredDocument holds the result of structured PDF extraction.
type StructuredDocument struct {
	FullText string
	Sections []DocumentSection
}

// DocumentSection represents a section of a document identified by heading.
type DocumentSection struct {
	Key      string // e.g. "item_4" or "fees_costs"
	Title    string
	Text     string
	Tables   []TableData
	Metadata map[string]any // regex-extracted: dollar amounts, percentages, URLs, etc.
}

// TableData holds a parsed table with headers and rows.
type TableData struct {
	Headers []string
	Rows    [][]string
}

// NewExtractor creates an Extractor based on config.
func NewExtractor(cfg config.OCRConfig, mistralKey string) (Extractor, error) {
	switch cfg.Provider {
	case "local", "":
		return NewPdfToText(cfg.PdfToTextPath), nil
	case "mistral":
		if mistralKey == "" {
			return nil, eris.New("ocr: mistral provider requires mistral_api_key")
		}
		return NewMistralOCR(mistralKey, ""), nil
	case "docling":
		return nil, eris.New("ocr: docling provider requires NewDoclingExtractor()")
	default:
		return nil, eris.Errorf("ocr: unknown provider %q", cfg.Provider)
	}
}

// NewDoclingExtractor creates an Extractor backed by a Docling client.
func NewDoclingExtractor(client docling.Client) *DoclingExtractor {
	return &DoclingExtractor{client: client}
}

// NewStructuredExtractor creates a StructuredExtractor for the given config.
func NewStructuredExtractor(cfg config.OCRConfig, doclingClient docling.Client) (StructuredExtractor, error) {
	if cfg.Provider != "docling" {
		return nil, eris.Errorf("ocr: structured extraction requires docling provider, got %q", cfg.Provider)
	}
	return NewDoclingExtractor(doclingClient), nil
}
