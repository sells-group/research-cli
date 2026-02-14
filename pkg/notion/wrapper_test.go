package notion

import (
	"context"
	"testing"

	"github.com/jomei/notionapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestQueryQueuedLeads_Error verifies that QueryQueuedLeads propagates errors
// from the underlying QueryAll / QueryDatabase call.
func TestQueryQueuedLeads_Error(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-err", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		pf, ok := req.Filter.(notionapi.PropertyFilter)
		if !ok {
			return false
		}
		return pf.Property == "Status" && pf.Status != nil && pf.Status.Equals == "Queued"
	})).Return(nil, assert.AnError).Once()

	pages, err := QueryQueuedLeads(ctx, mc, "db-err")
	assert.Error(t, err)
	assert.Nil(t, pages)
	assert.Contains(t, err.Error(), "notion: query queued leads")
	mc.AssertExpectations(t)
}

// TestQueryQueuedLeads_Empty verifies QueryQueuedLeads returns an empty
// slice when no leads are queued.
func TestQueryQueuedLeads_Empty(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-empty", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		pf, ok := req.Filter.(notionapi.PropertyFilter)
		if !ok {
			return false
		}
		return pf.Property == "Status" && pf.Status != nil && pf.Status.Equals == "Queued"
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{},
		HasMore: false,
	}, nil).Once()

	pages, err := QueryQueuedLeads(ctx, mc, "db-empty")
	assert.NoError(t, err)
	assert.Empty(t, pages)
	mc.AssertExpectations(t)
}

// TestQueryQueuedLeads_MultiplePages verifies QueryQueuedLeads handles
// pagination correctly when there are multiple pages of results.
func TestQueryQueuedLeads_MultiplePages(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	// First page of results.
	mc.On("QueryDatabase", ctx, "db-paginated", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		pf, ok := req.Filter.(notionapi.PropertyFilter)
		if !ok {
			return false
		}
		return pf.Property == "Status" &&
			pf.Status != nil &&
			pf.Status.Equals == "Queued" &&
			req.StartCursor == ""
	})).Return(&notionapi.DatabaseQueryResponse{
		Results:    []notionapi.Page{{ID: "lead-1"}, {ID: "lead-2"}},
		HasMore:    true,
		NextCursor: notionapi.Cursor("cursor-xyz"),
	}, nil).Once()

	// Second page of results.
	mc.On("QueryDatabase", ctx, "db-paginated", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == notionapi.Cursor("cursor-xyz")
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "lead-3"}},
		HasMore: false,
	}, nil).Once()

	pages, err := QueryQueuedLeads(ctx, mc, "db-paginated")
	assert.NoError(t, err)
	assert.Len(t, pages, 3)
	assert.Equal(t, notionapi.ObjectID("lead-1"), pages[0].ID)
	assert.Equal(t, notionapi.ObjectID("lead-2"), pages[1].ID)
	assert.Equal(t, notionapi.ObjectID("lead-3"), pages[2].ID)
	mc.AssertExpectations(t)
}

// TestQueryAll_NilFilter verifies QueryAll works correctly when filter is nil.
func TestQueryAll_NilFilter(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-nil-filter", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		// Filter should be nil when no filter is passed.
		return req.Filter == nil
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "p1"}},
		HasMore: false,
	}, nil).Once()

	pages, err := QueryAll(ctx, mc, "db-nil-filter", nil)
	assert.NoError(t, err)
	assert.Len(t, pages, 1)
	mc.AssertExpectations(t)
}

// TestQueryAll_WithSorts verifies that sort parameters are passed through.
func TestQueryAll_WithSorts(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-sorted", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return len(req.Sorts) == 1 && req.Sorts[0].Property == "Name"
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "sorted-1"}},
		HasMore: false,
	}, nil).Once()

	filter := &notionapi.DatabaseQueryRequest{
		Sorts: []notionapi.SortObject{
			{Property: "Name", Direction: notionapi.SortOrderASC},
		},
	}

	pages, err := QueryAll(ctx, mc, "db-sorted", filter)
	assert.NoError(t, err)
	assert.Len(t, pages, 1)
	mc.AssertExpectations(t)
}

// TestQueryAll_WithPageSize verifies that page size is passed through.
func TestQueryAll_WithPageSize(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	mc.On("QueryDatabase", ctx, "db-paged", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.PageSize == 10
	})).Return(&notionapi.DatabaseQueryResponse{
		Results: []notionapi.Page{{ID: "p1"}, {ID: "p2"}},
		HasMore: false,
	}, nil).Once()

	filter := &notionapi.DatabaseQueryRequest{
		PageSize: 10,
	}

	pages, err := QueryAll(ctx, mc, "db-paged", filter)
	assert.NoError(t, err)
	assert.Len(t, pages, 2)
	mc.AssertExpectations(t)
}

// TestQueryAll_ErrorOnSecondPage verifies that an error on the second page
// of pagination is propagated correctly.
func TestQueryAll_ErrorOnSecondPage(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	// First page succeeds.
	mc.On("QueryDatabase", ctx, "db-err-p2", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == ""
	})).Return(&notionapi.DatabaseQueryResponse{
		Results:    []notionapi.Page{{ID: "p1"}},
		HasMore:    true,
		NextCursor: notionapi.Cursor("cursor-next"),
	}, nil).Once()

	// Second page fails.
	mc.On("QueryDatabase", ctx, "db-err-p2", mock.MatchedBy(func(req *notionapi.DatabaseQueryRequest) bool {
		return req.StartCursor == notionapi.Cursor("cursor-next")
	})).Return(nil, assert.AnError).Once()

	pages, err := QueryAll(ctx, mc, "db-err-p2", nil)
	assert.Error(t, err)
	assert.Nil(t, pages)
	assert.Contains(t, err.Error(), "notion: query all page")
	mc.AssertExpectations(t)
}

// TestImportCSV_ContextCancelled verifies that ImportCSV respects context
// cancellation.
func TestImportCSV_ContextCancelled(t *testing.T) {
	mc := new(MockClient)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	csvContent := "Name,URL\nAcme,https://acme.com\nBeta,https://beta.io\n"
	csvPath := writeTempCSV(t, csvContent)

	count, err := ImportCSV(ctx, mc, "db-1", csvPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
	assert.Equal(t, 0, count)
}

// TestBuildPageProperties_EmptyRow verifies that an empty row produces an
// empty properties map.
func TestBuildPageProperties_EmptyRow(t *testing.T) {
	props := buildPageProperties(map[string]string{})
	assert.Empty(t, props)
}

// TestBuildPageProperties_URLOnly verifies properties when only a URL field
// is present.
func TestBuildPageProperties_URLOnly(t *testing.T) {
	row := map[string]string{
		"URL": "https://test.co",
	}
	props := buildPageProperties(row)

	up, ok := props["URL"].(notionapi.URLProperty)
	assert.True(t, ok)
	assert.Equal(t, "https://test.co", up.URL)
	assert.Equal(t, notionapi.PropertyTypeURL, up.Type)
}

// TestBuildPageProperties_NameOnly verifies properties when only a Name field
// is present.
func TestBuildPageProperties_NameOnly(t *testing.T) {
	row := map[string]string{
		"Name": "Test Co",
	}
	props := buildPageProperties(row)

	tp, ok := props["Name"].(notionapi.TitleProperty)
	assert.True(t, ok)
	assert.Equal(t, notionapi.PropertyTypeTitle, tp.Type)
	assert.Len(t, tp.Title, 1)
	assert.Equal(t, "Test Co", tp.Title[0].Text.Content)
}

// TestBuildPageProperties_MixedCaseKeys verifies that "name" (lowercase) does
// NOT become a title property - only exact "Name" match does (strings.EqualFold).
func TestBuildPageProperties_MixedCaseKeys(t *testing.T) {
	row := map[string]string{
		"name": "lowercase name",
		"url":  "https://lowercase.com",
	}
	props := buildPageProperties(row)

	// "name" should be matched by EqualFold and become a TitleProperty.
	tp, ok := props["name"].(notionapi.TitleProperty)
	assert.True(t, ok, "lowercase 'name' should be matched as title property")
	assert.Equal(t, "lowercase name", tp.Title[0].Text.Content)

	// "url" should be matched by EqualFold and become a URLProperty.
	up, ok := props["url"].(notionapi.URLProperty)
	assert.True(t, ok, "lowercase 'url' should be matched as URL property")
	assert.Equal(t, "https://lowercase.com", up.URL)
}
