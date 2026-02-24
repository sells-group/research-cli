package pipeline

import (
	"context"
	"testing"

	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/perplexity"
)

func TestStubAnthropicClient_CreateMessage(t *testing.T) {
	client := &StubAnthropicClient{}
	resp, err := client.CreateMessage(context.Background(), anthropic.MessageRequest{
		Model:     "claude-haiku-4-5-20251001",
		MaxTokens: 1024,
		Messages:  []anthropic.Message{{Role: "user", Content: "Extract the company name."}},
	})
	if err != nil {
		t.Fatalf("CreateMessage() error: %v", err)
	}
	if resp.ID == "" {
		t.Error("expected non-empty response ID")
	}
	if len(resp.Content) == 0 {
		t.Error("expected at least one content block")
	}
	if resp.Usage.InputTokens == 0 {
		t.Error("expected non-zero input tokens")
	}
}

func TestStubAnthropicClient_ClassifyDetection(t *testing.T) {
	client := &StubAnthropicClient{}
	resp, err := client.CreateMessage(context.Background(), anthropic.MessageRequest{
		Model:     "claude-haiku-4-5-20251001",
		MaxTokens: 1024,
		System:    []anthropic.SystemBlock{{Text: "Classify the page_type of this page."}},
		Messages:  []anthropic.Message{{Role: "user", Content: "Homepage content here"}},
	})
	if err != nil {
		t.Fatalf("CreateMessage() error: %v", err)
	}
	if len(resp.Content) == 0 {
		t.Fatal("expected at least one content block")
	}
	text := resp.Content[0].Text
	if text == "" {
		t.Error("expected non-empty classification response")
	}
}

func TestStubAnthropicClient_Batch(t *testing.T) {
	client := &StubAnthropicClient{}

	batchResp, err := client.CreateBatch(context.Background(), anthropic.BatchRequest{
		Requests: []anthropic.BatchRequestItem{
			{CustomID: "req-1", Params: anthropic.MessageRequest{Model: "haiku"}},
			{CustomID: "req-2", Params: anthropic.MessageRequest{Model: "haiku"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateBatch() error: %v", err)
	}
	if batchResp.ProcessingStatus != "ended" {
		t.Errorf("expected status ended, got %s", batchResp.ProcessingStatus)
	}
	if batchResp.RequestCounts.Succeeded != 2 {
		t.Errorf("expected 2 succeeded, got %d", batchResp.RequestCounts.Succeeded)
	}

	getResp, err := client.GetBatch(context.Background(), batchResp.ID)
	if err != nil {
		t.Fatalf("GetBatch() error: %v", err)
	}
	if getResp.ProcessingStatus != "ended" {
		t.Errorf("expected ended, got %s", getResp.ProcessingStatus)
	}

	iter, err := client.GetBatchResults(context.Background(), batchResp.ID)
	if err != nil {
		t.Fatalf("GetBatchResults() error: %v", err)
	}
	defer iter.Close() //nolint:errcheck
	if iter.Err() != nil {
		t.Errorf("unexpected iterator error: %v", iter.Err())
	}
}

func TestStubFirecrawlClient(t *testing.T) {
	client := &StubFirecrawlClient{}
	ctx := context.Background()

	crawlResp, err := client.Crawl(ctx, firecrawl.CrawlRequest{URL: "https://example.com"})
	if err != nil {
		t.Fatalf("Crawl() error: %v", err)
	}
	if !crawlResp.Success {
		t.Error("expected crawl success")
	}

	status, err := client.GetCrawlStatus(ctx, crawlResp.ID)
	if err != nil {
		t.Fatalf("GetCrawlStatus() error: %v", err)
	}
	if status.Status != "completed" {
		t.Errorf("expected completed, got %s", status.Status)
	}
	if len(status.Data) < 3 {
		t.Errorf("expected at least 3 pages, got %d", len(status.Data))
	}

	scrapeResp, err := client.Scrape(ctx, firecrawl.ScrapeRequest{URL: "https://example.com"})
	if err != nil {
		t.Fatalf("Scrape() error: %v", err)
	}
	if !scrapeResp.Success {
		t.Error("expected scrape success")
	}
}

func TestStubJinaClient(t *testing.T) {
	client := &StubJinaClient{}

	resp, err := client.Read(context.Background(), "https://example.com")
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected code 200, got %d", resp.Code)
	}
	if resp.Data.Content == "" {
		t.Error("expected non-empty content")
	}
	if resp.Data.Usage.Tokens == 0 {
		t.Error("expected non-zero tokens")
	}

	// Test Search method.
	searchResp, err := client.Search(context.Background(), "test query")
	if err != nil {
		t.Fatalf("Search() error: %v", err)
	}
	if searchResp.Code != 200 {
		t.Errorf("expected code 200, got %d", searchResp.Code)
	}
	if len(searchResp.Data) == 0 {
		t.Error("expected non-empty search results")
	}
	// Verify results contain expected sources.
	hasBBB := false
	for _, r := range searchResp.Data {
		if r.URL != "" && len(r.Title) > 0 {
			hasBBB = true
		}
	}
	if !hasBBB {
		t.Error("expected search results with URLs and titles")
	}
}

func TestStubPerplexityClient(t *testing.T) {
	client := &StubPerplexityClient{}
	resp, err := client.ChatCompletion(context.Background(), perplexity.ChatCompletionRequest{
		Messages: []perplexity.Message{{Role: "user", Content: "Tell me about this company"}},
	})
	if err != nil {
		t.Fatalf("ChatCompletion() error: %v", err)
	}
	if len(resp.Choices) == 0 {
		t.Error("expected at least one choice")
	}
	if resp.Usage.CompletionTokens == 0 {
		t.Error("expected non-zero completion tokens")
	}
}

func TestStubSalesforceClient(t *testing.T) {
	client := &StubSalesforceClient{}
	ctx := context.Background()

	if err := client.Query(ctx, "SELECT Id FROM Account", nil); err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	id, err := client.InsertOne(ctx, "Account", map[string]any{"Name": "Test"})
	if err != nil {
		t.Fatalf("InsertOne() error: %v", err)
	}
	if id == "" {
		t.Error("expected non-empty ID")
	}
	if err := client.UpdateOne(ctx, "Account", id, map[string]any{"Name": "Updated"}); err != nil {
		t.Fatalf("UpdateOne() error: %v", err)
	}
}

func TestStubNotionClient(t *testing.T) {
	client := &StubNotionClient{}
	ctx := context.Background()

	resp, err := client.QueryDatabase(ctx, "db-id", nil)
	if err != nil {
		t.Fatalf("QueryDatabase() error: %v", err)
	}
	if resp == nil {
		t.Error("expected non-nil response")
	}

	page, err := client.CreatePage(ctx, nil)
	if err != nil {
		t.Fatalf("CreatePage() error: %v", err)
	}
	if page == nil {
		t.Error("expected non-nil page")
	}
}

func TestStubPPPClient(t *testing.T) {
	client := &StubPPPClient{}
	matches, err := client.FindLoans(context.Background(), "Test Inc", "IL", "Springfield")
	if err != nil {
		t.Fatalf("FindLoans() error: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
	// Should not panic.
	client.Close()
}
