package pipeline

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/scrape"
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

// ScrapePhase implements Phase 1B: fetch external sources via scrape chain.
// Sources are fetched in parallel.
func ScrapePhase(ctx context.Context, company model.Company, chain *scrape.Chain) []model.CrawledPage {
	sources := DefaultExternalSources(company)

	var (
		mu    sync.Mutex
		pages []model.CrawledPage
	)

	g, gCtx := errgroup.WithContext(ctx)

	for _, src := range sources {
		g.Go(func() error {
			targetURL := src.URLFunc(company)

			result, err := chain.Scrape(gCtx, targetURL)
			if err != nil {
				zap.L().Warn("scrape: chain failed for external source",
					zap.String("source", src.Name),
					zap.String("url", targetURL),
					zap.Error(err),
				)
				return nil
			}
			if result != nil {
				page := result.Page
				page.Title = fmt.Sprintf("[%s] %s", src.Name, page.Title)
				mu.Lock()
				pages = append(pages, page)
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()
	return pages
}
