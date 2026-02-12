package pipeline

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
)

// ExternalSource defines an external data source to scrape.
type ExternalSource struct {
	Name    string
	URLFunc func(company model.Company) string
}

// DefaultExternalSources returns the standard external sources (GP, BBB, PPP, SoS).
func DefaultExternalSources(company model.Company) []ExternalSource {
	return []ExternalSource{
		{
			Name: "google_places",
			URLFunc: func(c model.Company) string {
				return fmt.Sprintf("https://www.google.com/maps/place/%s", c.Name)
			},
		},
		{
			Name: "bbb",
			URLFunc: func(c model.Company) string {
				return fmt.Sprintf("https://www.bbb.org/search?find_text=%s", c.Name)
			},
		},
		{
			Name: "ppp",
			URLFunc: func(c model.Company) string {
				return fmt.Sprintf("https://www.usaspending.gov/search/?hash=&fy=all&keyword=%s", c.Name)
			},
		},
		{
			Name: "sos",
			URLFunc: func(c model.Company) string {
				return fmt.Sprintf("https://www.google.com/search?q=%s+secretary+of+state+business+filing", c.Name)
			},
		},
	}
}

// ScrapePhase implements Phase 1B: fetch external sources via Jina (primary)
// with Firecrawl scrape fallback.
func ScrapePhase(ctx context.Context, company model.Company, jinaClient jina.Client, fcClient firecrawl.Client) []model.CrawledPage {
	sources := DefaultExternalSources(company)
	var pages []model.CrawledPage

	for _, src := range sources {
		select {
		case <-ctx.Done():
			return pages
		default:
		}

		targetURL := src.URLFunc(company)

		page, err := scrapeViaJina(ctx, targetURL, src.Name, jinaClient)
		if err == nil && page != nil {
			pages = append(pages, *page)
			continue
		}

		if err != nil {
			zap.L().Debug("scrape: jina failed for external source, trying firecrawl",
				zap.String("source", src.Name),
				zap.String("url", targetURL),
				zap.Error(err),
			)
		}

		// Firecrawl fallback.
		page, err = scrapeViaFirecrawl(ctx, targetURL, src.Name, fcClient)
		if err != nil {
			zap.L().Warn("scrape: firecrawl also failed for external source",
				zap.String("source", src.Name),
				zap.String("url", targetURL),
				zap.Error(err),
			)
			continue
		}
		if page != nil {
			pages = append(pages, *page)
		}
	}

	return pages
}

func scrapeViaJina(ctx context.Context, targetURL, sourceName string, client jina.Client) (*model.CrawledPage, error) {
	resp, err := client.Read(ctx, targetURL)
	if err != nil {
		return nil, err
	}

	if ValidateJinaResponse(resp) {
		return nil, eris.Errorf("scrape: jina response invalid for %s", sourceName)
	}

	return &model.CrawledPage{
		URL:        resp.Data.URL,
		Title:      fmt.Sprintf("[%s] %s", sourceName, resp.Data.Title),
		Markdown:   resp.Data.Content,
		StatusCode: resp.Code,
	}, nil
}

func scrapeViaFirecrawl(ctx context.Context, targetURL, sourceName string, client firecrawl.Client) (*model.CrawledPage, error) {
	resp, err := client.Scrape(ctx, firecrawl.ScrapeRequest{
		URL:     targetURL,
		Formats: []string{"markdown"},
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, eris.Errorf("scrape: firecrawl not successful for %s", sourceName)
	}
	return &model.CrawledPage{
		URL:        resp.Data.URL,
		Title:      fmt.Sprintf("[%s] %s", sourceName, resp.Data.Title),
		Markdown:   resp.Data.Markdown,
		StatusCode: resp.Data.StatusCode,
	}, nil
}
