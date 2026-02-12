package pipeline

import (
	"context"
	"time"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
	"github.com/sells-group/research-cli/pkg/salesforce"
)

// --- Jina Mock ---

type mockJinaClient struct {
	mock.Mock
}

func (m *mockJinaClient) Read(ctx context.Context, targetURL string) (*jina.ReadResponse, error) {
	args := m.Called(ctx, targetURL)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*jina.ReadResponse), args.Error(1)
}

// --- Firecrawl Mock ---

type mockFirecrawlClient struct {
	mock.Mock
}

func (m *mockFirecrawlClient) Crawl(ctx context.Context, req firecrawl.CrawlRequest) (*firecrawl.CrawlResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*firecrawl.CrawlResponse), args.Error(1)
}

func (m *mockFirecrawlClient) GetCrawlStatus(ctx context.Context, id string) (*firecrawl.CrawlStatusResponse, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*firecrawl.CrawlStatusResponse), args.Error(1)
}

func (m *mockFirecrawlClient) Scrape(ctx context.Context, req firecrawl.ScrapeRequest) (*firecrawl.ScrapeResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*firecrawl.ScrapeResponse), args.Error(1)
}

func (m *mockFirecrawlClient) BatchScrape(ctx context.Context, req firecrawl.BatchScrapeRequest) (*firecrawl.BatchScrapeResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*firecrawl.BatchScrapeResponse), args.Error(1)
}

func (m *mockFirecrawlClient) GetBatchScrapeStatus(ctx context.Context, id string) (*firecrawl.BatchScrapeStatusResponse, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*firecrawl.BatchScrapeStatusResponse), args.Error(1)
}

// --- Perplexity Mock ---

type mockPerplexityClient struct {
	mock.Mock
}

func (m *mockPerplexityClient) ChatCompletion(ctx context.Context, req perplexity.ChatCompletionRequest) (*perplexity.ChatCompletionResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*perplexity.ChatCompletionResponse), args.Error(1)
}

// --- Anthropic Mock ---

type mockAnthropicClient struct {
	mock.Mock
}

func (m *mockAnthropicClient) CreateMessage(ctx context.Context, req anthropic.MessageRequest) (*anthropic.MessageResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*anthropic.MessageResponse), args.Error(1)
}

func (m *mockAnthropicClient) CreateBatch(ctx context.Context, req anthropic.BatchRequest) (*anthropic.BatchResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*anthropic.BatchResponse), args.Error(1)
}

func (m *mockAnthropicClient) GetBatch(ctx context.Context, batchID string) (*anthropic.BatchResponse, error) {
	args := m.Called(ctx, batchID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*anthropic.BatchResponse), args.Error(1)
}

func (m *mockAnthropicClient) GetBatchResults(ctx context.Context, batchID string) (anthropic.BatchResultIterator, error) {
	args := m.Called(ctx, batchID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(anthropic.BatchResultIterator), args.Error(1)
}

// --- Salesforce Mock ---

type mockSalesforceClient struct {
	mock.Mock
}

func (m *mockSalesforceClient) Query(ctx context.Context, soql string, out any) error {
	args := m.Called(ctx, soql, out)
	return args.Error(0)
}

func (m *mockSalesforceClient) InsertOne(ctx context.Context, sObjectName string, record map[string]any) (string, error) {
	args := m.Called(ctx, sObjectName, record)
	return args.String(0), args.Error(1)
}

func (m *mockSalesforceClient) UpdateOne(ctx context.Context, sObjectName string, id string, fields map[string]any) error {
	args := m.Called(ctx, sObjectName, id, fields)
	return args.Error(0)
}

func (m *mockSalesforceClient) UpdateCollection(ctx context.Context, sObjectName string, records []salesforce.CollectionRecord) ([]salesforce.CollectionResult, error) {
	args := m.Called(ctx, sObjectName, records)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]salesforce.CollectionResult), args.Error(1)
}

func (m *mockSalesforceClient) DescribeSObject(ctx context.Context, name string) (*salesforce.SObjectDescription, error) {
	args := m.Called(ctx, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*salesforce.SObjectDescription), args.Error(1)
}

// --- Notion Mock ---

type mockNotionClient struct {
	mock.Mock
}

func (m *mockNotionClient) QueryDatabase(ctx context.Context, dbID string, req *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error) {
	args := m.Called(ctx, dbID, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*notionapi.DatabaseQueryResponse), args.Error(1)
}

func (m *mockNotionClient) CreatePage(ctx context.Context, req *notionapi.PageCreateRequest) (*notionapi.Page, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*notionapi.Page), args.Error(1)
}

func (m *mockNotionClient) UpdatePage(ctx context.Context, pageID string, req *notionapi.PageUpdateRequest) (*notionapi.Page, error) {
	args := m.Called(ctx, pageID, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*notionapi.Page), args.Error(1)
}

// --- Store Mock ---

type mockStore struct {
	mock.Mock
}

func (m *mockStore) CreateRun(ctx context.Context, company model.Company) (*model.Run, error) {
	args := m.Called(ctx, company)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Run), args.Error(1)
}

func (m *mockStore) UpdateRunStatus(ctx context.Context, runID string, status model.RunStatus) error {
	args := m.Called(ctx, runID, status)
	return args.Error(0)
}

func (m *mockStore) UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error {
	args := m.Called(ctx, runID, result)
	return args.Error(0)
}

func (m *mockStore) GetRun(ctx context.Context, runID string) (*model.Run, error) {
	args := m.Called(ctx, runID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Run), args.Error(1)
}

func (m *mockStore) ListRuns(ctx context.Context, filter store.RunFilter) ([]model.Run, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Run), args.Error(1)
}

func (m *mockStore) CreatePhase(ctx context.Context, runID string, name string) (*model.RunPhase, error) {
	args := m.Called(ctx, runID, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.RunPhase), args.Error(1)
}

func (m *mockStore) CompletePhase(ctx context.Context, phaseID string, result *model.PhaseResult) error {
	args := m.Called(ctx, phaseID, result)
	return args.Error(0)
}

func (m *mockStore) GetCachedCrawl(ctx context.Context, companyURL string) (*model.CrawlCache, error) {
	args := m.Called(ctx, companyURL)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.CrawlCache), args.Error(1)
}

func (m *mockStore) SetCachedCrawl(ctx context.Context, companyURL string, pages []model.CrawledPage, ttl time.Duration) error {
	args := m.Called(ctx, companyURL, pages, ttl)
	return args.Error(0)
}

func (m *mockStore) DeleteExpiredCrawls(ctx context.Context) (int, error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func (m *mockStore) Migrate(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- PPP Mock ---

type mockPPPClient struct {
	mock.Mock
}

func (m *mockPPPClient) FindLoans(ctx context.Context, name, state, city string) ([]ppp.LoanMatch, error) {
	args := m.Called(ctx, name, state, city)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]ppp.LoanMatch), args.Error(1)
}

func (m *mockPPPClient) Close() {}

// --- Batch Result Iterator Mock ---

type mockBatchResultIterator struct {
	items []anthropic.BatchResultItem
	idx   int
}

func newMockBatchIterator(items []anthropic.BatchResultItem) *mockBatchResultIterator {
	return &mockBatchResultIterator{items: items, idx: -1}
}

func (m *mockBatchResultIterator) Next() bool {
	m.idx++
	return m.idx < len(m.items)
}

func (m *mockBatchResultIterator) Item() anthropic.BatchResultItem {
	return m.items[m.idx]
}

func (m *mockBatchResultIterator) Err() error {
	return nil
}

func (m *mockBatchResultIterator) Close() error {
	return nil
}

// --- Ensure interface compliance ---
var (
	_ jina.Client                   = (*mockJinaClient)(nil)
	_ firecrawl.Client              = (*mockFirecrawlClient)(nil)
	_ perplexity.Client             = (*mockPerplexityClient)(nil)
	_ anthropic.Client              = (*mockAnthropicClient)(nil)
	_ salesforce.Client             = (*mockSalesforceClient)(nil)
	_ notion.Client                 = (*mockNotionClient)(nil)
	_ ppp.Querier                   = (*mockPPPClient)(nil)
	_ store.Store                   = (*mockStore)(nil)
	_ anthropic.BatchResultIterator = (*mockBatchResultIterator)(nil)
)
