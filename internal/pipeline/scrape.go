package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
)

// ExternalSource defines an external data source to scrape.
type ExternalSource struct {
	Name    string
	URLFunc func(company model.Company) string
}

// DefaultExternalSources returns the standard external sources (GP, BBB, SoS).
// PPP data is fetched via direct Postgres lookup in Phase 1D, not web scraping.
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
			Name: "sos",
			URLFunc: func(c model.Company) string {
				return fmt.Sprintf("https://www.google.com/search?q=%s+secretary+of+state+business+filing", c.Name)
			},
		},
	}
}

// ScrapePhase implements Phase 1B: fetch external sources via Jina (primary)
// with Firecrawl scrape fallback. Sources are fetched in parallel.
func ScrapePhase(ctx context.Context, company model.Company, jinaClient jina.Client, fcClient firecrawl.Client) []model.CrawledPage {
	sources := DefaultExternalSources(company)

	var (
		mu    sync.Mutex
		pages []model.CrawledPage
	)

	g, gCtx := errgroup.WithContext(ctx)

	for _, src := range sources {
		g.Go(func() error {
			targetURL := src.URLFunc(company)

			page, err := scrapeViaJina(gCtx, targetURL, src.Name, jinaClient)
			if err == nil && page != nil {
				mu.Lock()
				pages = append(pages, *page)
				mu.Unlock()
				return nil
			}

			if err != nil {
				zap.L().Debug("scrape: jina failed for external source, trying firecrawl",
					zap.String("source", src.Name),
					zap.String("url", targetURL),
					zap.Error(err),
				)
			}

			// Firecrawl fallback.
			page, err = scrapeViaFirecrawl(gCtx, targetURL, src.Name, fcClient)
			if err != nil {
				zap.L().Warn("scrape: firecrawl also failed for external source",
					zap.String("source", src.Name),
					zap.String("url", targetURL),
					zap.Error(err),
				)
				return nil
			}
			if page != nil {
				mu.Lock()
				pages = append(pages, *page)
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()
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
