package ocr

import (
	"bytes"
	"context"
	"os/exec"

	"github.com/rotisserie/eris"
)

// PdfToText extracts text from PDFs using the pdftotext CLI tool.
type PdfToText struct {
	binPath string
}

// NewPdfToText creates a PdfToText extractor. If binPath is empty, "pdftotext" is used.
func NewPdfToText(binPath string) *PdfToText {
	if binPath == "" {
		binPath = "pdftotext"
	}
	return &PdfToText{binPath: binPath}
}

// ExtractText runs pdftotext -layout on the given PDF and returns stdout.
func (p *PdfToText) ExtractText(ctx context.Context, pdfPath string) (string, error) {
	cmd := exec.CommandContext(ctx, p.binPath, "-layout", pdfPath, "-")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", eris.Wrapf(err, "ocr: pdftotext failed for %s: %s", pdfPath, stderr.String())
	}

	return stdout.String(), nil
}
