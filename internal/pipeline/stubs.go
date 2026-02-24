package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/jomei/notionapi"

	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
	"github.com/sells-group/research-cli/pkg/salesforce"
)

// Compile-time interface checks.
var (
	_ anthropic.Client  = (*StubAnthropicClient)(nil)
	_ firecrawl.Client  = (*StubFirecrawlClient)(nil)
	_ jina.Client       = (*StubJinaClient)(nil)
	_ perplexity.Client = (*StubPerplexityClient)(nil)
	_ salesforce.Client = (*StubSalesforceClient)(nil)
	_ notion.Client     = (*StubNotionClient)(nil)
	_ ppp.Querier       = (*StubPPPClient)(nil)
)

// --- Anthropic Stub ---

// StubAnthropicClient implements anthropic.Client with canned responses.
type StubAnthropicClient struct{}

// CreateMessage implements anthropic.Client.
func (s *StubAnthropicClient) CreateMessage(_ context.Context, req anthropic.MessageRequest) (*anthropic.MessageResponse, error) {
	// Detect classification vs extraction by checking prompt content.
	content := ""
	for _, m := range req.Messages {
		content += m.Content
	}
	for _, sys := range req.System {
		content += sys.Text
	}

	var responseText string
	if strings.Contains(strings.ToLower(content), "classify") || strings.Contains(strings.ToLower(content), "page_type") {
		responseText = `{"page_type": "homepage", "confidence": 0.9}`
	} else {
		responseText = `{"value": "stub extracted value", "confidence": 0.75, "reasoning": "stub response"}`
	}

	return &anthropic.MessageResponse{
		ID:         "stub-msg-001",
		Model:      req.Model,
		Content:    []anthropic.ContentBlock{{Type: "text", Text: responseText}},
		StopReason: "end_turn",
		Usage: anthropic.TokenUsage{
			InputTokens:  150,
			OutputTokens: 50,
		},
	}, nil
}

// CreateBatch implements anthropic.Client.
func (s *StubAnthropicClient) CreateBatch(_ context.Context, req anthropic.BatchRequest) (*anthropic.BatchResponse, error) {
	return &anthropic.BatchResponse{
		ID:               "stub-batch-001",
		ProcessingStatus: "ended",
		RequestCounts: anthropic.RequestCounts{
			Succeeded: int64(len(req.Requests)),
		},
	}, nil
}

// GetBatch implements anthropic.Client.
func (s *StubAnthropicClient) GetBatch(_ context.Context, _ string) (*anthropic.BatchResponse, error) {
	return &anthropic.BatchResponse{
		ID:               "stub-batch-001",
		ProcessingStatus: "ended",
		RequestCounts: anthropic.RequestCounts{
			Succeeded: 1,
		},
	}, nil
}

// GetBatchResults implements anthropic.Client.
func (s *StubAnthropicClient) GetBatchResults(_ context.Context, _ string) (anthropic.BatchResultIterator, error) {
	return &stubBatchIterator{done: true}, nil
}

// stubBatchIterator implements anthropic.BatchResultIterator.
type stubBatchIterator struct {
	done bool
}

// Next implements anthropic.BatchResultIterator.
func (it *stubBatchIterator) Next() bool { return false }

// Err implements anthropic.BatchResultIterator.
func (it *stubBatchIterator) Err() error { return nil }

// Close implements anthropic.BatchResultIterator.
func (it *stubBatchIterator) Close() error { return nil }

// Item implements anthropic.BatchResultIterator.
func (it *stubBatchIterator) Item() anthropic.BatchResultItem {
	return anthropic.BatchResultItem{}
}

// --- Firecrawl Stub ---

// StubFirecrawlClient implements firecrawl.Client with canned responses.
type StubFirecrawlClient struct{}

// Crawl implements firecrawl.Client.
func (s *StubFirecrawlClient) Crawl(_ context.Context, _ firecrawl.CrawlRequest) (*firecrawl.CrawlResponse, error) {
	return &firecrawl.CrawlResponse{
		Success: true,
		ID:      "stub-crawl-001",
	}, nil
}

// GetCrawlStatus implements firecrawl.Client.
func (s *StubFirecrawlClient) GetCrawlStatus(_ context.Context, _ string) (*firecrawl.CrawlStatusResponse, error) {
	return &firecrawl.CrawlStatusResponse{
		Status: "completed",
		Total:  3,
		Data: []firecrawl.PageData{
			{URL: "https://example.com", Markdown: stubHomepageMarkdown, Title: "Example Construction", StatusCode: 200},
			{URL: "https://example.com/about", Markdown: stubAboutMarkdown, Title: "About Us", StatusCode: 200},
			{URL: "https://example.com/services", Markdown: stubServicesMarkdown, Title: "Our Services", StatusCode: 200},
		},
	}, nil
}

// Scrape implements firecrawl.Client.
func (s *StubFirecrawlClient) Scrape(_ context.Context, req firecrawl.ScrapeRequest) (*firecrawl.ScrapeResponse, error) {
	return &firecrawl.ScrapeResponse{
		Success: true,
		Data: firecrawl.PageData{
			URL:        req.URL,
			Markdown:   stubHomepageMarkdown,
			Title:      "Stub Page",
			StatusCode: 200,
		},
	}, nil
}

// BatchScrape implements firecrawl.Client.
func (s *StubFirecrawlClient) BatchScrape(_ context.Context, _ firecrawl.BatchScrapeRequest) (*firecrawl.BatchScrapeResponse, error) {
	return &firecrawl.BatchScrapeResponse{
		Success: true,
		ID:      "stub-batchscrape-001",
	}, nil
}

// GetBatchScrapeStatus implements firecrawl.Client.
func (s *StubFirecrawlClient) GetBatchScrapeStatus(_ context.Context, _ string) (*firecrawl.BatchScrapeStatusResponse, error) {
	return &firecrawl.BatchScrapeStatusResponse{
		Status: "completed",
		Total:  1,
		Data: []firecrawl.PageData{
			{URL: "https://example.com", Markdown: stubHomepageMarkdown, Title: "Example", StatusCode: 200},
		},
	}, nil
}

// --- Jina Stub ---

// StubJinaClient implements jina.Client with canned responses.
type StubJinaClient struct{}

// Read implements jina.Client.
func (s *StubJinaClient) Read(_ context.Context, targetURL string) (*jina.ReadResponse, error) {
	return &jina.ReadResponse{
		Code: 200,
		Data: jina.ReadData{
			Title:   fmt.Sprintf("Stub Page - %s", targetURL),
			URL:     targetURL,
			Content: stubHomepageMarkdown,
			Usage:   jina.ReadUsage{Tokens: 500},
		},
	}, nil
}

// Search implements jina.Client.
func (s *StubJinaClient) Search(_ context.Context, _ string, _ ...jina.SearchOption) (*jina.SearchResponse, error) {
	return &jina.SearchResponse{
		Code: 200,
		Data: []jina.SearchResult{
			{
				Title:       "ABC Construction Company | Better Business Bureau Profile",
				URL:         "https://www.bbb.org/us/il/springfield/profile/construction/abc-construction-0001-12345",
				Content:     "ABC Construction Company in Springfield, IL. See BBB rating, reviews, complaints.",
				Description: "BBB accredited since 2010. A+ rating.",
			},
			{
				Title:       "ABC Construction Company - Google Maps",
				URL:         "https://www.google.com/maps/place/ABC+Construction+Company",
				Content:     "123 Main Street, Springfield, IL 62701",
				Description: "Construction company in Springfield, IL.",
			},
			{
				Title:       "ABC Construction Company - Illinois Secretary of State",
				URL:         "https://www.ilsos.gov/corporatellc/CorporateLlcController?command=info&id=abc123",
				Content:     "ABC Construction Company, Inc. Active. Springfield, IL.",
				Description: "Illinois business filing.",
			},
		},
	}, nil
}

// --- Perplexity Stub ---

// StubPerplexityClient implements perplexity.Client with canned responses.
type StubPerplexityClient struct{}

// ChatCompletion implements perplexity.Client.
func (s *StubPerplexityClient) ChatCompletion(_ context.Context, _ perplexity.ChatCompletionRequest) (*perplexity.ChatCompletionResponse, error) {
	return &perplexity.ChatCompletionResponse{
		ID: "stub-pplx-001",
		Choices: []perplexity.Choice{
			{
				Index: 0,
				Message: perplexity.Message{
					Role:    "assistant",
					Content: stubLinkedInContent,
				},
			},
		},
		Usage: perplexity.Usage{
			PromptTokens:     100,
			CompletionTokens: 200,
		},
	}, nil
}

// --- Salesforce Stub ---

// StubSalesforceClient implements salesforce.Client as a no-op.
type StubSalesforceClient struct{}

// Query implements salesforce.Client.
func (s *StubSalesforceClient) Query(_ context.Context, _ string, _ any) error {
	return nil
}

// InsertOne implements salesforce.Client.
func (s *StubSalesforceClient) InsertOne(_ context.Context, _ string, _ map[string]any) (string, error) {
	return "stub-sf-001", nil
}

// UpdateOne implements salesforce.Client.
func (s *StubSalesforceClient) UpdateOne(_ context.Context, _ string, _ string, _ map[string]any) error {
	return nil
}

// UpdateCollection implements salesforce.Client.
func (s *StubSalesforceClient) UpdateCollection(_ context.Context, _ string, records []salesforce.CollectionRecord) ([]salesforce.CollectionResult, error) {
	results := make([]salesforce.CollectionResult, len(records))
	for i, r := range records {
		results[i] = salesforce.CollectionResult{ID: r.ID, Success: true}
	}
	return results, nil
}

// DescribeSObject implements salesforce.Client.
func (s *StubSalesforceClient) DescribeSObject(_ context.Context, name string) (*salesforce.SObjectDescription, error) {
	return &salesforce.SObjectDescription{Name: name, Label: name}, nil
}

// --- Notion Stub ---

// StubNotionClient implements notion.Client as a no-op.
type StubNotionClient struct{}

// QueryDatabase implements notion.Client.
func (s *StubNotionClient) QueryDatabase(_ context.Context, _ string, _ *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error) {
	return &notionapi.DatabaseQueryResponse{}, nil
}

// CreatePage implements notion.Client.
func (s *StubNotionClient) CreatePage(_ context.Context, _ *notionapi.PageCreateRequest) (*notionapi.Page, error) {
	return &notionapi.Page{}, nil
}

// UpdatePage implements notion.Client.
func (s *StubNotionClient) UpdatePage(_ context.Context, _ string, _ *notionapi.PageUpdateRequest) (*notionapi.Page, error) {
	return &notionapi.Page{}, nil
}

// --- PPP Stub ---

// StubPPPClient implements ppp.Querier as a no-op.
type StubPPPClient struct{}

// FindLoans implements ppp.Querier.
func (s *StubPPPClient) FindLoans(_ context.Context, _, _, _ string) ([]ppp.LoanMatch, error) {
	return nil, nil
}

// Close implements ppp.Querier.
func (s *StubPPPClient) Close() {}

// --- Canned Content ---

const stubHomepageMarkdown = `# ABC Construction Company

Welcome to ABC Construction, your trusted partner for residential and commercial construction services.

## About Us
Founded in 2005, ABC Construction has been serving the greater metropolitan area for nearly 20 years.
We are a family-owned business dedicated to quality craftsmanship and customer satisfaction.

**Phone:** (555) 123-4567
**Email:** info@abcconstruction.com
**Address:** 123 Main Street, Suite 100, Springfield, IL 62701

## Our Services
- Residential Roofing (new installation and repair)
- Commercial Roofing
- Siding Installation
- Gutter Installation and Repair
- Storm Damage Restoration
- General Contracting

## Why Choose Us
- Licensed and Insured (License #12345)
- 20-Year Workmanship Warranty
- A+ BBB Rating
- Over 5,000 projects completed
- Free estimates

## Our Team
**John Smith** - President & CEO
**Jane Doe** - VP of Operations

We employ over 50 skilled professionals dedicated to delivering exceptional results.

© 2024 ABC Construction Company, Inc. All rights reserved.
`

const stubAboutMarkdown = `# About ABC Construction

ABC Construction Company, Inc. was founded in 2005 by John Smith with a vision to provide
top-quality construction services to homeowners and businesses.

## Our History
Starting as a small roofing crew, we've grown to become one of the region's most trusted
construction companies with over 50 employees and $15M in annual revenue.

## Certifications
- GAF Master Elite Contractor
- OSHA 30-Hour Certified
- BBB Accredited Business (A+ Rating)
- EPA Lead-Safe Certified

## Service Area
We proudly serve Springfield, IL and surrounding communities within a 50-mile radius,
including Decatur, Jacksonville, Lincoln, and Champaign.
`

const stubServicesMarkdown = `# Our Services

## Residential Roofing
Complete roof replacement and repair services for homes of all sizes. We work with
asphalt shingles, metal roofing, tile, and flat roof systems.

## Commercial Roofing
Full-service commercial roofing including TPO, EPDM, and built-up roofing systems.

## Siding & Exteriors
Vinyl siding, fiber cement, and wood siding installation and repair.

## Storm Restoration
24/7 emergency response for storm damage. We work directly with your insurance company.

## General Contracting
Kitchen and bathroom remodeling, additions, and whole-home renovations.

All work backed by our 20-year workmanship warranty.
`

const stubLinkedInContent = `Based on LinkedIn data, ABC Construction Company is a construction services firm
headquartered in Springfield, IL. The company specializes in residential and commercial
roofing, siding, and general contracting. Founded in 2005, the company has approximately
50 employees. The CEO is John Smith. The company is described as a full-service construction
company serving the central Illinois region. Industry: Construction. Company type: Privately held.`
