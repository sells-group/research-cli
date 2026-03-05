package ocr

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/docling"
)

type mockDoclingClient struct {
	doc *docling.Document
	err error
}

func (m *mockDoclingClient) Convert(_ context.Context, _ []byte, _ docling.ConvertOpts) (*docling.Document, error) {
	return m.doc, m.err
}

func TestDoclingExtractor_ExtractText(t *testing.T) {
	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 fake"), 0644))

	client := &mockDoclingClient{
		doc: &docling.Document{
			Pages: []docling.Page{
				{
					Elements: []docling.Element{
						{Type: "heading", Text: "Introduction"},
						{Type: "paragraph", Text: "First paragraph."},
						{Type: "table", Table: &docling.TableData{
							Headers: []string{"Name", "Value"},
							Rows:    [][]string{{"Alice", "100"}, {"Bob", "200"}},
						}},
						{Type: "paragraph", Text: "Conclusion text."},
					},
				},
			},
		},
	}

	ext := NewDoclingExtractor(client)
	text, err := ext.ExtractText(context.Background(), pdfPath)
	require.NoError(t, err)

	assert.Contains(t, text, "Introduction")
	assert.Contains(t, text, "First paragraph.")
	assert.Contains(t, text, "Name\tValue")
	assert.Contains(t, text, "Alice\t100")
	assert.Contains(t, text, "Bob\t200")
	assert.Contains(t, text, "Conclusion text.")
}

func TestDoclingExtractor_ExtractStructured(t *testing.T) {
	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 fake"), 0644))

	client := &mockDoclingClient{
		doc: &docling.Document{
			Pages: []docling.Page{
				{
					Elements: []docling.Element{
						{Type: "paragraph", Text: "Preamble before any heading."},
						{Type: "heading", Text: "Section One"},
						{Type: "paragraph", Text: "Content of section one."},
						{Type: "table", Table: &docling.TableData{
							Headers: []string{"Col1", "Col2"},
							Rows:    [][]string{{"a", "b"}},
						}},
						{Type: "heading", Text: "Section Two"},
						{Type: "paragraph", Text: "Content of section two."},
					},
				},
			},
		},
	}

	ext := NewDoclingExtractor(client)
	result, err := ext.ExtractStructured(context.Background(), pdfPath)
	require.NoError(t, err)

	// FullText should contain all elements.
	assert.Contains(t, result.FullText, "Preamble before any heading.")
	assert.Contains(t, result.FullText, "Section One")
	assert.Contains(t, result.FullText, "Content of section one.")
	assert.Contains(t, result.FullText, "Content of section two.")

	// Sections: preamble (untitled), Section One, Section Two.
	require.Len(t, result.Sections, 3)

	// Untitled preamble section.
	assert.Empty(t, result.Sections[0].Title)
	assert.Equal(t, "Preamble before any heading.", result.Sections[0].Text)
	assert.Empty(t, result.Sections[0].Tables)

	// Section One with table.
	assert.Equal(t, "Section One", result.Sections[1].Title)
	assert.Contains(t, result.Sections[1].Text, "Content of section one.")
	require.Len(t, result.Sections[1].Tables, 1)
	assert.Equal(t, []string{"Col1", "Col2"}, result.Sections[1].Tables[0].Headers)
	assert.Equal(t, [][]string{{"a", "b"}}, result.Sections[1].Tables[0].Rows)

	// Section Two.
	assert.Equal(t, "Section Two", result.Sections[2].Title)
	assert.Equal(t, "Content of section two.", result.Sections[2].Text)
	assert.Empty(t, result.Sections[2].Tables)

	// Key is left empty for dataset-specific parsers to populate.
	for _, s := range result.Sections {
		assert.Empty(t, s.Key)
	}
}

func TestDoclingExtractor_FileError(t *testing.T) {
	client := &mockDoclingClient{
		doc: &docling.Document{},
	}

	ext := NewDoclingExtractor(client)

	_, err := ext.ExtractText(context.Background(), "/nonexistent/path/file.pdf")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ocr: read PDF")

	_, err = ext.ExtractStructured(context.Background(), "/nonexistent/path/file.pdf")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ocr: read PDF")
}

func TestDoclingExtractor_ConvertError(t *testing.T) {
	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 fake"), 0644))

	client := &mockDoclingClient{
		err: eris.New("connection refused"),
	}

	ext := NewDoclingExtractor(client)

	_, err := ext.ExtractText(context.Background(), pdfPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ocr: docling convert")

	_, err = ext.ExtractStructured(context.Background(), pdfPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ocr: docling convert")
}

func TestNewStructuredExtractor_Docling(t *testing.T) {
	client := &mockDoclingClient{
		doc: &docling.Document{},
	}

	ext, err := NewStructuredExtractor(config.OCRConfig{Provider: "docling"}, client)
	require.NoError(t, err)
	require.NotNil(t, ext)
	assert.IsType(t, &DoclingExtractor{}, ext)
}

func TestNewStructuredExtractor_UnsupportedProvider(t *testing.T) {
	client := &mockDoclingClient{
		doc: &docling.Document{},
	}

	for _, provider := range []string{"local", "mistral", "", "unknown"} {
		t.Run(provider, func(t *testing.T) {
			_, err := NewStructuredExtractor(config.OCRConfig{Provider: provider}, client)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "structured extraction requires docling provider")
		})
	}
}

func TestDoclingExtractor_EmptyDocument(t *testing.T) {
	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 fake"), 0644))

	client := &mockDoclingClient{
		doc: &docling.Document{
			Pages: []docling.Page{},
		},
	}

	ext := NewDoclingExtractor(client)

	text, err := ext.ExtractText(context.Background(), pdfPath)
	require.NoError(t, err)
	assert.Empty(t, text)

	result, err := ext.ExtractStructured(context.Background(), pdfPath)
	require.NoError(t, err)
	assert.Empty(t, result.FullText)
	assert.Empty(t, result.Sections)
}
