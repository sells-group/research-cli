package notion

import (
	"context"
	"time"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
)

// QueryAll fetches all pages from a Notion database, handling pagination and
// respecting the 3 req/s rate limit via a 334ms ticker.
func QueryAll(ctx context.Context, c Client, dbID string, filter *notionapi.DatabaseQueryRequest) ([]notionapi.Page, error) {
	var all []notionapi.Page

	// Notion rate limit: 3 requests per second -> 334ms between requests.
	ticker := time.NewTicker(334 * time.Millisecond)
	defer ticker.Stop()

	req := &notionapi.DatabaseQueryRequest{}
	if filter != nil {
		req.Filter = filter.Filter
		req.Sorts = filter.Sorts
		req.PageSize = filter.PageSize
	}

	for {
		select {
		case <-ctx.Done():
			return nil, eris.Wrap(ctx.Err(), "notion: query all cancelled")
		case <-ticker.C:
		}

		resp, err := c.QueryDatabase(ctx, dbID, req)
		if err != nil {
			return nil, eris.Wrap(err, "notion: query all page")
		}

		all = append(all, resp.Results...)

		if !resp.HasMore {
			break
		}
		req.StartCursor = resp.NextCursor
	}

	return all, nil
}

// QueryQueuedLeads fetches all pages with Status = "Queued" from the given database.
func QueryQueuedLeads(ctx context.Context, c Client, dbID string) ([]notionapi.Page, error) {
	filter := &notionapi.DatabaseQueryRequest{
		Filter: notionapi.PropertyFilter{
			Property: "Status",
			Status: &notionapi.StatusFilterCondition{
				Equals: "Queued",
			},
		},
	}
	pages, err := QueryAll(ctx, c, dbID, filter)
	if err != nil {
		return nil, eris.Wrap(err, "notion: query queued leads")
	}
	return pages, nil
}
