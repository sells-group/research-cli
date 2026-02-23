//go:build !integration

package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
)

// mockNotionClient records UpdatePage calls for testing.
type mockNotionClient struct {
	notion.Client
	updateCalls []string // page IDs passed to UpdatePage
}

func (m *mockNotionClient) QueryDatabase(_ context.Context, _ string, _ *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error) {
	return nil, nil
}

func (m *mockNotionClient) CreatePage(_ context.Context, _ *notionapi.PageCreateRequest) (*notionapi.Page, error) {
	return nil, nil
}

func (m *mockNotionClient) UpdatePage(_ context.Context, pageID string, _ *notionapi.PageUpdateRequest) (*notionapi.Page, error) {
	m.updateCalls = append(m.updateCalls, pageID)
	return &notionapi.Page{}, nil
}

func makeFakeLeads(n int) []notionapi.Page {
	leads := make([]notionapi.Page, n)
	for i := range leads {
		leads[i] = notionapi.Page{
			ID: notionapi.ObjectID(fmt.Sprintf("page-%d", i)),
			Properties: notionapi.Properties{
				"Name": &notionapi.TitleProperty{
					Title: []notionapi.RichText{{PlainText: fmt.Sprintf("Company %d", i)}},
				},
				"URL": &notionapi.URLProperty{
					URL: fmt.Sprintf("https://example-%d.com", i),
				},
			},
		}
	}
	return leads
}

func TestProcessBatch_EmptyLeads(t *testing.T) {
	err := processBatch(context.Background(), nil, 10, 5, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		t.Fatal("enrichFunc should not be called for empty leads")
		return nil, nil
	})
	require.NoError(t, err)
}

func TestProcessBatch_EmptyLeadsSlice(t *testing.T) {
	err := processBatch(context.Background(), []notionapi.Page{}, 10, 5, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		t.Fatal("enrichFunc should not be called for empty leads")
		return nil, nil
	})
	require.NoError(t, err)
}

func TestProcessBatch_AllSucceed(t *testing.T) {
	leads := makeFakeLeads(3)
	var count atomic.Int64

	err := processBatch(context.Background(), leads, 0, 2, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		count.Add(1)
		return &model.EnrichmentResult{
			Score:   0.85,
			Answers: []model.ExtractionAnswer{{FieldKey: "test"}},
		}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count.Load())
}

func TestProcessBatch_AllFail(t *testing.T) {
	leads := makeFakeLeads(2)

	err := processBatch(context.Background(), leads, 0, 2, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		return nil, errors.New("enrichment error")
	})
	// Individual failures don't abort the batch.
	require.NoError(t, err)
}

func TestProcessBatch_MixedResults(t *testing.T) {
	leads := makeFakeLeads(4)
	var callCount atomic.Int64

	err := processBatch(context.Background(), leads, 0, 2, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		n := callCount.Add(1)
		if n%2 == 0 {
			return nil, errors.New("even-numbered call fails")
		}
		return &model.EnrichmentResult{Score: 0.9}, nil
	})
	require.NoError(t, err)
}

func TestProcessBatch_AppliesLimit(t *testing.T) {
	leads := makeFakeLeads(5)
	var count atomic.Int64

	err := processBatch(context.Background(), leads, 3, 2, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		count.Add(1)
		return &model.EnrichmentResult{Score: 0.8}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count.Load(), "should only process 3 leads due to limit")
}

func TestProcessBatch_LimitLargerThanLeads(t *testing.T) {
	leads := makeFakeLeads(2)
	var count atomic.Int64

	err := processBatch(context.Background(), leads, 10, 2, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		count.Add(1)
		return &model.EnrichmentResult{Score: 0.7}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count.Load(), "should process all 2 leads since limit > count")
}

func TestProcessBatch_ZeroLimit(t *testing.T) {
	// A limit of 0 means no limit.
	leads := makeFakeLeads(4)
	var count atomic.Int64

	err := processBatch(context.Background(), leads, 0, 5, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		count.Add(1)
		return &model.EnrichmentResult{Score: 0.9}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4), count.Load())
}

func TestProcessBatch_Concurrency1(t *testing.T) {
	leads := makeFakeLeads(3)
	var count atomic.Int64

	err := processBatch(context.Background(), leads, 0, 1, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		count.Add(1)
		return &model.EnrichmentResult{Score: 0.95}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count.Load())
}

func TestProcessBatch_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	leads := makeFakeLeads(2)

	// Even with cancelled context, processBatch should handle it gracefully.
	err := processBatch(ctx, leads, 0, 2, nil, nil, 0, func(ctx context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return &model.EnrichmentResult{Score: 0.5}, nil
	})
	// Individual failures are swallowed, so this should not return an error.
	assert.NoError(t, err)
}

func TestProcessBatch_FailureUpdatesNotion(t *testing.T) {
	leads := makeFakeLeads(3)
	mc := &mockNotionClient{}

	err := processBatch(context.Background(), leads, 0, 1, mc, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		return nil, errors.New("api timeout")
	})
	require.NoError(t, err)

	// All 3 leads fail, so Notion should be updated 3 times.
	assert.Len(t, mc.updateCalls, 3, "expected 3 Notion status updates for 3 failures")
}

func TestProcessBatch_FailureNilNotion(t *testing.T) {
	// With nil notion client, failures should not panic.
	leads := makeFakeLeads(2)

	err := processBatch(context.Background(), leads, 0, 1, nil, nil, 0, func(_ context.Context, _ model.Company) (*model.EnrichmentResult, error) {
		return nil, errors.New("some error")
	})
	require.NoError(t, err)
}
