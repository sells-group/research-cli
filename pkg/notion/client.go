// Package notion wraps the Notion API for database queries and page management.
package notion

import (
	"context"
	"fmt"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"golang.org/x/time/rate"
)

// Client defines the Notion API operations used by this application.
type Client interface {
	QueryDatabase(ctx context.Context, dbID string, req *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error)
	CreatePage(ctx context.Context, req *notionapi.PageCreateRequest) (*notionapi.Page, error)
	UpdatePage(ctx context.Context, pageID string, req *notionapi.PageUpdateRequest) (*notionapi.Page, error)
}

// ClientOption configures the Notion client.
type ClientOption func(*notionClient)

// WithRateLimit overrides the default Notion rate limit (3 req/s).
func WithRateLimit(rps float64) ClientOption {
	return func(c *notionClient) {
		if rps > 0 {
			c.limiter = rate.NewLimiter(rate.Limit(rps), max(int(rps), 1))
		} else {
			c.limiter = nil
		}
	}
}

// notionClient implements Client by wrapping a *notionapi.Client.
type notionClient struct {
	inner   *notionapi.Client
	limiter *rate.Limiter
}

// NewClient creates a new Notion client with the given integration token.
// By default, API calls are throttled to 3 req/s (Notion's rate limit).
func NewClient(token string, opts ...ClientOption) Client {
	c := &notionClient{
		inner:   notionapi.NewClient(notionapi.Token(token)),
		limiter: rate.NewLimiter(3, 1),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// wait blocks until the rate limiter allows one event, or ctx is cancelled.
func (c *notionClient) wait(ctx context.Context) error {
	if c.limiter == nil {
		return nil
	}
	return c.limiter.Wait(ctx)
}

func (c *notionClient) QueryDatabase(ctx context.Context, dbID string, req *notionapi.DatabaseQueryRequest) (*notionapi.DatabaseQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "notion: rate limit")
	}
	resp, err := c.inner.Database.Query(ctx, notionapi.DatabaseID(dbID), req)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("notion: query database %s", dbID))
	}
	return resp, nil
}

func (c *notionClient) CreatePage(ctx context.Context, req *notionapi.PageCreateRequest) (*notionapi.Page, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "notion: rate limit")
	}
	page, err := c.inner.Page.Create(ctx, req)
	if err != nil {
		return nil, eris.Wrap(err, "notion: create page")
	}
	return page, nil
}

func (c *notionClient) UpdatePage(ctx context.Context, pageID string, req *notionapi.PageUpdateRequest) (*notionapi.Page, error) {
	if err := c.wait(ctx); err != nil {
		return nil, eris.Wrap(err, "notion: rate limit")
	}
	page, err := c.inner.Page.Update(ctx, notionapi.PageID(pageID), req)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("notion: update page %s", pageID))
	}
	return page, nil
}
