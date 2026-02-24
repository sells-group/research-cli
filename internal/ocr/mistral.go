package ocr

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/rotisserie/eris"
)

const (
	mistralOCREndpoint  = "https://api.mistral.ai/v1/ocr"
	defaultMistralModel = "pixtral-large-latest"
)

// MistralOCR extracts text from PDFs using the Mistral OCR API.
type MistralOCR struct {
	apiKey   string
	model    string
	endpoint string
	client   *http.Client
}

// NewMistralOCR creates a MistralOCR extractor. If model is empty, the default is used.
func NewMistralOCR(apiKey, model string) *MistralOCR {
	if model == "" {
		model = defaultMistralModel
	}
	return &MistralOCR{
		apiKey:   apiKey,
		model:    model,
		endpoint: mistralOCREndpoint,
		client:   &http.Client{},
	}
}

type mistralOCRRequest struct {
	Model    string             `json:"model"`
	Document mistralOCRDocument `json:"document"`
}

type mistralOCRDocument struct {
	Type       string `json:"type"`
	DocumentURL string `json:"document_url"`
}

type mistralOCRResponse struct {
	Pages []mistralOCRPage `json:"pages"`
}

type mistralOCRPage struct {
	Index    int    `json:"index"`
	Markdown string `json:"markdown"`
}

// ExtractText reads a PDF file, sends it to Mistral OCR, and returns the extracted text.
func (m *MistralOCR) ExtractText(ctx context.Context, pdfPath string) (string, error) {
	data, err := os.ReadFile(pdfPath)
	if err != nil {
		return "", eris.Wrapf(err, "ocr: read PDF %s", pdfPath)
	}

	encoded := base64.StdEncoding.EncodeToString(data)
	dataURL := "data:application/pdf;base64," + encoded

	reqBody := mistralOCRRequest{
		Model: m.model,
		Document: mistralOCRDocument{
			Type:       "document_url",
			DocumentURL: dataURL,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", eris.Wrap(err, "ocr: marshal mistral request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", eris.Wrap(err, "ocr: create mistral request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.apiKey)

	resp, err := m.client.Do(req)
	if err != nil {
		return "", eris.Wrap(err, "ocr: mistral API call")
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", eris.Wrap(err, "ocr: read mistral response")
	}

	if resp.StatusCode != http.StatusOK {
		return "", eris.Errorf("ocr: mistral API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var ocrResp mistralOCRResponse
	if err := json.Unmarshal(respBody, &ocrResp); err != nil {
		return "", eris.Wrap(err, "ocr: unmarshal mistral response")
	}

	var sb strings.Builder
	for i, page := range ocrResp.Pages {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		sb.WriteString(page.Markdown)
	}

	return sb.String(), nil
}
