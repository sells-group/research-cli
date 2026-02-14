package ocr

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
)

// Extractor extracts text content from PDF files.
type Extractor interface {
	ExtractText(ctx context.Context, pdfPath string) (string, error)
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
	default:
		return nil, eris.Errorf("ocr: unknown provider %q", cfg.Provider)
	}
}
