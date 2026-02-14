package scrape

import (
	"context"
	"strings"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/jina"
)

// JinaAdapter wraps a Jina Reader client as a Scraper.
type JinaAdapter struct {
	client jina.Client
}

// NewJinaAdapter creates a JinaAdapter from a Jina client.
func NewJinaAdapter(client jina.Client) *JinaAdapter {
	return &JinaAdapter{client: client}
}

func (j *JinaAdapter) Name() string { return "jina" }

// Supports returns true — Jina can attempt any URL.
func (j *JinaAdapter) Supports(_ string) bool { return true }

// Scrape fetches a URL via Jina Reader and validates the response.
func (j *JinaAdapter) Scrape(ctx context.Context, targetURL string) (*Result, error) {
	resp, err := j.client.Read(ctx, targetURL)
	if err != nil {
		return nil, err
	}

	if needsFallback(resp) {
		return nil, eris.New("jina: response needs fallback")
	}

	return &Result{
		Page: model.CrawledPage{
			URL:        resp.Data.URL,
			Title:      resp.Data.Title,
			Markdown:   resp.Data.Content,
			StatusCode: resp.Code,
		},
		Source: "jina",
	}, nil
}

// needsFallback checks whether a Jina response contains usable content
// or indicates the page is blocked/empty. Returns true if the response
// should be retried with a different scraper.
func needsFallback(resp *jina.ReadResponse) bool {
	if resp == nil {
		return true
	}

	if resp.Code != 0 && resp.Code != 200 {
		return true
	}

	content := strings.TrimSpace(resp.Data.Content)

	if len(content) < 100 {
		return true
	}

	lower := strings.ToLower(content)

	challengeSignatures := []string{
		"checking your browser",
		"enable javascript",
		"please enable cookies",
		"access denied",
		"403 forbidden",
		"just a moment",
		"cloudflare",
		"attention required",
	}

	for _, sig := range challengeSignatures {
		if strings.Contains(lower, sig) && len(content) < 1000 {
			return true
		}
	}

	return false
}
