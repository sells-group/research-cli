package notion

import (
	"context"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueryAll_SinglePage(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-1", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(&notionapi.DatabaseQueryResponse{
			Results: []notionapi.Page{
				{ID: "p1"},
				{ID: "p2"},
			},
			HasMore: false,
		}, nil).Once()

	pages, err := QueryAll(ctx, mc, "db-1", nil)
	assert.NoError(t, err)
	assert.Len(t, pages, 2)
	mc.AssertExpectations(t)
}

func TestQueryAll_MultiPage(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	// First call returns page 1 with HasMore=true.
	mc.On("QueryDatabase", ctx, "db-1", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == ""
	})).Return(&notionapi.DatabaseQueryResponse{
		Results:    []notionapi.Page{{ID: "p1"}},
		HasMore:    true,
		NextCursor: notionapi.Cursor("cursor-abc"),
	}, nil).Once()

	// Second call uses the cursor and returns final page.
	mc.On("QueryDatabase", ctx, "db-1", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == notionapi.Cursor("cursor-abc")
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "p2"}},
		HasMore: false,
	}, nil).Once()

	pages, err := QueryAll(ctx, mc, "db-1", nil)
	assert.NoError(t, err)
	assert.Len(t, pages, 2)
	assert.Equal(t, notionapi.ObjectID("p1"), pages[0].ID)
	assert.Equal(t, notionapi.ObjectID("p2"), pages[1].ID)
	mc.AssertExpectations(t)
}

func TestQueryAll_WithFilter(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-1", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		// Verify the filter was passed through.
		if req.Filter == nil {
			return false
		}
		pf, ok := req.Filter.(notionapi.PropertyFilter)
		if !ok {
			return false
		}
		return pf.Property == "Status" && pf.Status != nil && pf.Status.Equals == "Active"
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "p1"}},
		HasMore: false,
	}, nil).Once()

	filter := &notionapi.DatabaseQueryRequest{
		Filter: notionapi.PropertyFilter{
			Property: "Status",
			Status: &notionapi.StatusFilterCondition{
				Equals: "Active",
			},
		},
	}

	pages, err := QueryAll(ctx, mc, "db-1", filter)
	assert.NoError(t, err)
	assert.Len(t, pages, 1)
	mc.AssertExpectations(t)
}

func TestQueryAll_Error(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-1", mock.AnythingOfType("*notionapi.DatabaseQueryRequest")).
		Return(nil, assert.AnError).Once()

	pages, err := QueryAll(ctx, mc, "db-1", nil)
	assert.Error(t, err)
	assert.Nil(t, pages)
	mc.AssertExpectations(t)
}

func TestQueryAll_ContextCancelled(t *testing.T) {
	mc := new(MockClient)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	pages, err := QueryAll(ctx, mc, "db-1", nil)
	assert.Error(t, err)
	assert.Nil(t, pages)
}

func TestQueryQueuedLeads(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-leads", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		pf, ok := req.Filter.(notionapi.PropertyFilter)
		if !ok {
			return false
		}
		return pf.Property == "Status" && pf.Status != nil && pf.Status.Equals == "Queued"
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "lead-1"}, {ID: "lead-2"}},
		HasMore: false,
	}, nil).Once()

	pages, err := QueryQueuedLeads(ctx, mc, "db-leads")
	assert.NoError(t, err)
	assert.Len(t, pages, 2)
	mc.AssertExpectations(t)
}
