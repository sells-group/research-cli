package notion

import (
	"context"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockClient implements Client for testing.
type MockClient struct {
	mock.Mock
}

func (m *MockClient) QueryDatabase(ctx context.Context, dbID string, req *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error) {
	args := m.Called(ctx, dbID, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*notionapi.DatabaseQueryResponse), args.Error(1)
}

func (m *MockClient) CreatePage(ctx context.Context, req *notionapi.PageCreateRequest) (*notionapi.Page, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*notionapi.Page), args.Error(1)
}

func (m *MockClient) UpdatePage(ctx context.Context, pageID string, req *notionapi.PageUpdateRequest) (*notionapi.Page, error) {
	args := m.Called(ctx, pageID, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*notionapi.Page), args.Error(1)
}

func TestMockClientSatisfiesInterface(t *testing.T) {
	t.Parallel()
	var _ Client = (*MockClient)(nil)
}

func TestQueryDatabase(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	expected := &notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "page-1"}},
		HasMore: false,
	}

	mc.On("QueryDatabase", ctx, "db-123", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(expected, nil)

	resp, err := mc.QueryDatabase(ctx, "db-123", &notionapi.DatabaseQueryRequest{})
	assert.NoError(t, err)
	assert.Len(t, resp.Results, 1)
	assert.Equal(t, notionapi.ObjectID("page-1"), resp.Results[0].ID)
	mc.AssertExpectations(t)
}

func TestCreatePage(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	expected := &notionapi.Page{ID: "new-page-1"}

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(expected, nil)

	page, err := mc.CreatePage(ctx, &notionapi.PageCreateRequest{})
	assert.NoError(t, err)
	assert.Equal(t, notionapi.ObjectID("new-page-1"), page.ID)
	mc.AssertExpectations(t)
}

func TestUpdatePage(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	expected := &notionapi.Page{ID: "page-1"}

	mc.On("UpdatePage", ctx, "page-1", mock.AnythingOfType("*notionapi.PageUpdateRequest")).
		Return(expected, nil)

	page, err := mc.UpdatePage(ctx, "page-1", &notionapi.PageUpdateRequest{})
	assert.NoError(t, err)
	assert.Equal(t, notionapi.ObjectID("page-1"), page.ID)
	mc.AssertExpectations(t)
}

func TestCreatePageError(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("CreatePage", ctx, mock.AnythingOfType("*notionapi.PageCreateRequest")).
		Return(nil, assert.AnError)

	page, err := mc.CreatePage(ctx, &notionapi.PageCreateRequest{})
	assert.Error(t, err)
	assert.Nil(t, page)
	mc.AssertExpectations(t)
}

func TestNewClientReturnsClient(t *testing.T) {
	c := NewClient("test-token")
	assert.NotNil(t, c)
	var _ Client = c //nolint:staticcheck // interface compliance check
}

func TestQueryDatabaseError(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-err", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(nil, assert.AnError)

	resp, err := mc.QueryDatabase(ctx, "db-err", &notionapi.DatabaseQueryRequest{})
	assert.Error(t, err)
	assert.Nil(t, resp)
	mc.AssertExpectations(t)
}

func TestUpdatePageError(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("UpdatePage", ctx, "page-err", mock.AnythingOfType("*notionapi.PageUpdateRequest")).
		Return(nil, assert.AnError)

	page, err := mc.UpdatePage(ctx, "page-err", &notionapi.PageUpdateRequest{})
	assert.Error(t, err)
	assert.Nil(t, page)
	mc.AssertExpectations(t)
}
