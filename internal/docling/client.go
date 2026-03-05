// Package docling provides a client for the Docling PDF parsing service.
package docling

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
)

// Client converts PDF documents into structured elements via Docling.
type Client interface {
	Convert(ctx context.Context, pdfData []byte, opts ConvertOpts) (*Document, error)
}

// ConvertOpts holds options for the Convert call.
type ConvertOpts struct{}

// Document represents a parsed PDF document.
type Document struct {
	Pages []Page
}

// Page represents a single page of parsed content.
type Page struct {
	Elements []Element
}

// Element represents a structural element within a page.
type Element struct {
	Type  string     // heading, paragraph, table, list_item
	Text  string     // text content of the element
	Level int        // heading level (1-6), zero for non-headings
	Table *TableData // non-nil for table elements
}

// TableData holds parsed table content.
type TableData struct {
	Headers []string
	Rows    [][]string
}

type httpClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// NewClient creates a Docling Client that talks to the given base URL.
// If apiKey is non-empty, it is sent as an X-API-Key header on every request.
func NewClient(baseURL, apiKey string) Client {
	return &httpClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client:  &http.Client{Timeout: 120 * time.Second},
	}
}

// apiResponse mirrors the Docling JSON response envelope.
type apiResponse struct {
	Document apiDocument `json:"document"`
}

type apiDocument struct {
	Pages []apiPage    `json:"pages"`
	Body  []apiElement `json:"body"`
}

type apiPage struct {
	PageNo int `json:"page_no"`
}

type apiElement struct {
	Type  string        `json:"type"`
	Text  string        `json:"text"`
	Level int           `json:"level"`
	Data  *apiTableData `json:"data,omitempty"`
}

type apiTableData struct {
	Headers []string   `json:"headers"`
	Rows    [][]string `json:"rows"`
}

// Convert sends PDF bytes to Docling and returns a structured Document.
func (c *httpClient) Convert(ctx context.Context, pdfData []byte, _ ConvertOpts) (*Document, error) {
	body, contentType, err := buildMultipart(pdfData)
	if err != nil {
		return nil, err
	}

	url := c.baseURL + "/api/v1/convert"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, eris.Wrap(err, "docling: create request")
	}
	req.Header.Set("Content-Type", contentType)
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "docling: API call")
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "docling: read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("docling: API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var apiResp apiResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, eris.Wrap(err, "docling: unmarshal response")
	}

	return mapResponse(&apiResp), nil
}

func buildMultipart(pdfData []byte) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	fw, err := w.CreateFormFile("file", "document.pdf")
	if err != nil {
		return nil, "", eris.Wrap(err, "docling: create form file")
	}
	if _, err := fw.Write(pdfData); err != nil {
		return nil, "", eris.Wrap(err, "docling: write PDF data")
	}

	optJSON, err := json.Marshal(map[string]string{"output_format": "json"})
	if err != nil {
		return nil, "", eris.Wrap(err, "docling: marshal options")
	}
	if err := w.WriteField("options", string(optJSON)); err != nil {
		return nil, "", eris.Wrap(err, "docling: write options field")
	}

	if err := w.Close(); err != nil {
		return nil, "", eris.Wrap(err, "docling: close multipart writer")
	}

	return &buf, w.FormDataContentType(), nil
}

func mapResponse(apiResp *apiResponse) *Document {
	// Build a single page from body elements. Docling returns a flat body
	// array; we group all elements into one page for now.
	elements := make([]Element, 0, len(apiResp.Document.Body))
	for _, ae := range apiResp.Document.Body {
		el := Element{
			Type:  ae.Type,
			Text:  ae.Text,
			Level: ae.Level,
		}
		if ae.Data != nil {
			el.Table = &TableData{
				Headers: ae.Data.Headers,
				Rows:    ae.Data.Rows,
			}
		}
		elements = append(elements, el)
	}

	// Use the page count from the API if available, otherwise default to 1.
	pageCount := len(apiResp.Document.Pages)
	if pageCount == 0 {
		pageCount = 1
	}

	pages := make([]Page, pageCount)
	// Assign all elements to the first page.
	if len(elements) > 0 {
		pages[0] = Page{Elements: elements}
	}

	// Ensure remaining pages have initialized (empty) element slices.
	for i := range pages {
		if pages[i].Elements == nil {
			pages[i].Elements = []Element{}
		}
	}

	return &Document{Pages: pages}
}
