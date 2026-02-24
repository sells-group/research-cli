// Package notion wraps the Notion API for database queries and page management.
package notion

import (
	"context"
	"fmt"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
)

// Client defines the Notion API operations used by this application.
type Client interface {
	QueryDatabase(ctx context.Context, dbID string, req *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error)
	CreatePage(ctx context.Context, req *notionapi.PageCreateRequest) (*notionapi.Page, error)
	UpdatePage(ctx context.Context, pageID string, req *notionapi.PageUpdateRequest) (*notionapi.Page, error)
}

// notionClient implements Client by wrapping a *notionapi.Client.
type notionClient struct {
	inner *notionapi.Client
}

// NewClient creates a new Notion client with the given integration token.
func NewClient(token string) Client {
	return &notionClient{
		inner: notionapi.NewClient(notionapi.Token(token)),
	}
}

func (c *notionClient) QueryDatabase(ctx context.Context, dbID string, req *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error) {
	resp, err := c.inner.Database.Query(ctx, notionapi.DatabaseID(dbID), req)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("notion: query database %s", dbID))
	}
	return resp, nil
}

func (c *notionClient) CreatePage(ctx context.Context, req *notionapi.PageCreateRequest) (*notionapi.Page, error) {
	page, err := c.inner.Page.Create(ctx, req)
	if err != nil {
		return nil, eris.Wrap(err, "notion: create page")
	}
	return page, nil
}

func (c *notionClient) UpdatePage(ctx context.Context, pageID string, req *notionapi.PageUpdateRequest) (*notionapi.Page, error) {
	page, err := c.inner.Page.Update(ctx, notionapi.PageID(pageID), req)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("notion: update page %s", pageID))
	}
	return page, nil
}
