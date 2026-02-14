package scrape

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/pkg/firecrawl"
	firecrawlmocks "github.com/sells-group/research-cli/pkg/firecrawl/mocks"
)

func TestFirecrawlAdapter_Name(t *testing.T) {
	t.Parallel()
	adapter := NewFirecrawlAdapter(firecrawlmocks.NewMockClient(t))
	assert.Equal(t, "firecrawl", adapter.Name())
}

func TestFirecrawlAdapter_Supports(t *testing.T) {
	t.Parallel()
	adapter := NewFirecrawlAdapter(firecrawlmocks.NewMockClient(t))
	assert.True(t, adapter.Supports("https://example.com"))
	assert.True(t, adapter.Supports(""))
}

func TestFirecrawlAdapter_Scrape_Success(t *testing.T) {
	t.Parallel()
	mock := firecrawlmocks.NewMockClient(t)
	adapter := NewFirecrawlAdapter(mock)

	mock.EXPECT().Scrape(context.Background(), firecrawl.ScrapeRequest{
		URL:     "https://acme.com/about",
		Formats: []string{"markdown"},
	}).Return(&firecrawl.ScrapeResponse{
		Success: true,
		Data: firecrawl.PageData{
			URL:        "https://acme.com/about",
			Title:      "About Acme",
			Markdown:   "# About Us\n\nWe do things.",
			StatusCode: 200,
		},
	}, nil)

	result, err := adapter.Scrape(context.Background(), "https://acme.com/about")
	require.NoError(t, err)
	assert.Equal(t, "firecrawl", result.Source)
	assert.Equal(t, "https://acme.com/about", result.Page.URL)
	assert.Equal(t, "About Acme", result.Page.Title)
	assert.Equal(t, "# About Us\n\nWe do things.", result.Page.Markdown)
	assert.Equal(t, 200, result.Page.StatusCode)
}

func TestFirecrawlAdapter_Scrape_ClientError(t *testing.T) {
	t.Parallel()
	mock := firecrawlmocks.NewMockClient(t)
	adapter := NewFirecrawlAdapter(mock)

	mock.EXPECT().Scrape(context.Background(), firecrawl.ScrapeRequest{
		URL:     "https://fail.com",
		Formats: []string{"markdown"},
	}).Return(nil, errors.New("api error: rate limited"))

	_, err := adapter.Scrape(context.Background(), "https://fail.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limited")
}

func TestFirecrawlAdapter_Scrape_NotSuccessful(t *testing.T) {
	t.Parallel()
	mock := firecrawlmocks.NewMockClient(t)
	adapter := NewFirecrawlAdapter(mock)

	mock.EXPECT().Scrape(context.Background(), firecrawl.ScrapeRequest{
		URL:     "https://blocked.com",
		Formats: []string{"markdown"},
	}).Return(&firecrawl.ScrapeResponse{
		Success: false,
		Data:    firecrawl.PageData{},
	}, nil)

	_, err := adapter.Scrape(context.Background(), "https://blocked.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scrape not successful")
}
