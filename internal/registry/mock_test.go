package registry

import (
	"context"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/mock"
)

// mockNotionClient implements notion.Client for testing.
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
