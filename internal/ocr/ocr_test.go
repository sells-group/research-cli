package ocr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExtractor_Local(t *testing.T) {
	ext, err := NewExtractor(config.OCRConfig{Provider: "local", PdfToTextPath: "/usr/bin/pdftotext"}, "")
	require.NoError(t, err)
	assert.IsType(t, &PdfToText{}, ext)
}

func TestNewExtractor_LocalDefault(t *testing.T) {
	ext, err := NewExtractor(config.OCRConfig{Provider: ""}, "")
	require.NoError(t, err)
	assert.IsType(t, &PdfToText{}, ext)
}

func TestNewExtractor_MistralMissingKey(t *testing.T) {
	_, err := NewExtractor(config.OCRConfig{Provider: "mistral"}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mistral provider requires mistral_api_key")
}

func TestNewExtractor_MistralWithKey(t *testing.T) {
	ext, err := NewExtractor(config.OCRConfig{Provider: "mistral"}, "test-key")
	require.NoError(t, err)
	assert.IsType(t, &MistralOCR{}, ext)
}

func TestNewExtractor_UnknownProvider(t *testing.T) {
	_, err := NewExtractor(config.OCRConfig{Provider: "unknown"}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown provider "unknown"`)
}

func TestPdfToText_BinPath(t *testing.T) {
	p := NewPdfToText("")
	assert.Equal(t, "pdftotext", p.binPath)

	p = NewPdfToText("/custom/pdftotext")
	assert.Equal(t, "/custom/pdftotext", p.binPath)
}

func TestMistralOCR_DefaultModel(t *testing.T) {
	m := NewMistralOCR("key", "")
	assert.Equal(t, defaultMistralModel, m.model)
	assert.Equal(t, mistralOCREndpoint, m.endpoint)
}

func TestMistralOCR_CustomModel(t *testing.T) {
	m := NewMistralOCR("key", "custom-model")
	assert.Equal(t, "custom-model", m.model)
}

func TestMistralOCR_ExtractText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req mistralOCRRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "test-model", req.Model)
		assert.Equal(t, "document_url", req.Document.Type)
		assert.Contains(t, req.Document.DocumentURL, "data:application/pdf;base64,")

		resp := mistralOCRResponse{
			Pages: []mistralOCRPage{
				{Index: 0, Markdown: "Page one content"},
				{Index: 1, Markdown: "Page two content"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	}))
	defer srv.Close()

	// Create a temp PDF file
	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 test content"), 0644))

	m := &MistralOCR{
		apiKey:   "test-key",
		model:    "test-model",
		endpoint: srv.URL,
		client:   &http.Client{},
	}

	text, err := m.ExtractText(context.Background(), pdfPath)
	require.NoError(t, err)
	assert.Equal(t, "Page one content\n\nPage two content", text)
}

func TestMistralOCR_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid api key"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 test"), 0644))

	m := &MistralOCR{
		apiKey:   "bad-key",
		model:    "test-model",
		endpoint: srv.URL,
		client:   &http.Client{},
	}

	_, err := m.ExtractText(context.Background(), pdfPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mistral API returned 401")
}

func TestMistralOCR_FileNotFound(t *testing.T) {
	m := NewMistralOCR("key", "model")
	_, err := m.ExtractText(context.Background(), "/nonexistent/file.pdf")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read PDF")
}

func TestPdfToText_ExtractText_BinaryNotFound(t *testing.T) {
	p := NewPdfToText("/nonexistent/pdftotext")
	_, err := p.ExtractText(context.Background(), "/tmp/test.pdf")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pdftotext failed")
}

func TestPdfToText_ExtractText_Success(t *testing.T) {
	// Create a fake pdftotext script that echoes content
	tmpDir := t.TempDir()
	fakeBin := filepath.Join(tmpDir, "pdftotext")
	script := "#!/bin/sh\necho 'Extracted text content'\n"
	require.NoError(t, os.WriteFile(fakeBin, []byte(script), 0755))

	p := NewPdfToText(fakeBin)
	text, err := p.ExtractText(context.Background(), "/tmp/dummy.pdf")
	require.NoError(t, err)
	assert.Contains(t, text, "Extracted text content")
}

func TestMistralOCR_MalformedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{invalid json`)) //nolint:errcheck
	}))
	defer srv.Close()

	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 test"), 0644))

	m := &MistralOCR{
		apiKey:   "test-key",
		model:    "test-model",
		endpoint: srv.URL,
		client:   &http.Client{},
	}

	_, err := m.ExtractText(context.Background(), pdfPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal mistral response")
}

func TestMistralOCR_EmptyPages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := mistralOCRResponse{Pages: []mistralOCRPage{}}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	}))
	defer srv.Close()

	tmpDir := t.TempDir()
	pdfPath := filepath.Join(tmpDir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("%PDF-1.4 test"), 0644))

	m := &MistralOCR{
		apiKey:   "test-key",
		model:    "test-model",
		endpoint: srv.URL,
		client:   &http.Client{},
	}

	text, err := m.ExtractText(context.Background(), pdfPath)
	require.NoError(t, err)
	assert.Empty(t, text)
}
