package ocr

import (
	"context"
	"os"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/docling"
)

// DoclingExtractor uses the Docling service for PDF text extraction.
type DoclingExtractor struct {
	client docling.Client
}

// ExtractText implements Extractor by converting a PDF via Docling and returning flattened text.
func (d *DoclingExtractor) ExtractText(ctx context.Context, pdfPath string) (string, error) {
	data, err := os.ReadFile(pdfPath) // #nosec G304 -- path from internal OCR pipeline, not user input
	if err != nil {
		return "", eris.Wrapf(err, "ocr: read PDF %s", pdfPath)
	}

	doc, err := d.client.Convert(ctx, data, docling.ConvertOpts{})
	if err != nil {
		return "", eris.Wrap(err, "ocr: docling convert")
	}

	return flattenDocument(doc), nil
}

// ExtractStructured implements StructuredExtractor by converting a PDF via Docling
// and grouping elements into sections by headings.
func (d *DoclingExtractor) ExtractStructured(ctx context.Context, pdfPath string) (*StructuredDocument, error) {
	data, err := os.ReadFile(pdfPath) // #nosec G304 -- path from internal OCR pipeline, not user input
	if err != nil {
		return nil, eris.Wrapf(err, "ocr: read PDF %s", pdfPath)
	}

	doc, err := d.client.Convert(ctx, data, docling.ConvertOpts{})
	if err != nil {
		return nil, eris.Wrap(err, "ocr: docling convert")
	}

	result := &StructuredDocument{
		FullText: flattenDocument(doc),
		Sections: buildSections(doc),
	}
	return result, nil
}

// flattenDocument concatenates all element text with newlines, rendering tables
// as tab-separated text.
func flattenDocument(doc *docling.Document) string {
	var sb strings.Builder
	first := true
	for _, page := range doc.Pages {
		for _, el := range page.Elements {
			text := elementText(el)
			if text == "" {
				continue
			}
			if !first {
				sb.WriteString("\n")
			}
			sb.WriteString(text)
			first = false
		}
	}
	return sb.String()
}

// elementText returns the text representation of an element, rendering tables
// as tab-separated rows.
func elementText(el docling.Element) string {
	if el.Table != nil {
		return renderTable(el.Table)
	}
	return el.Text
}

// renderTable formats a table as tab-separated text with headers and rows.
func renderTable(t *docling.TableData) string {
	var sb strings.Builder
	if len(t.Headers) > 0 {
		sb.WriteString(strings.Join(t.Headers, "\t"))
	}
	for _, row := range t.Rows {
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(strings.Join(row, "\t"))
	}
	return sb.String()
}

// buildSections groups consecutive elements by headings into DocumentSections.
func buildSections(doc *docling.Document) []DocumentSection {
	var sections []DocumentSection
	var current *DocumentSection

	for _, page := range doc.Pages {
		for _, el := range page.Elements {
			if el.Type == "heading" {
				// Flush previous section.
				if current != nil {
					sections = append(sections, *current)
				}
				current = &DocumentSection{
					Title: el.Text,
				}
				continue
			}

			// If no heading seen yet, create an untitled section.
			if current == nil {
				current = &DocumentSection{}
			}

			if el.Table != nil {
				current.Tables = append(current.Tables, TableData{
					Headers: el.Table.Headers,
					Rows:    el.Table.Rows,
				})
				// Also append table text to section text.
				tableText := renderTable(el.Table)
				if tableText != "" {
					if current.Text != "" {
						current.Text += "\n"
					}
					current.Text += tableText
				}
			} else if el.Text != "" {
				if current.Text != "" {
					current.Text += "\n"
				}
				current.Text += el.Text
			}
		}
	}

	// Flush final section.
	if current != nil {
		sections = append(sections, *current)
	}

	return sections
}

// Compile-time interface checks.
var (
	_ Extractor           = (*DoclingExtractor)(nil)
	_ StructuredExtractor = (*DoclingExtractor)(nil)
)
