package geoscraper

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
	"github.com/sells-group/research-cli/internal/geospatial"
	"github.com/sells-group/research-cli/internal/temporal/sdk"
)

// Activities holds dependencies for geo scraper Temporal activities.
type Activities struct {
	sdk.SyncLogActivities

	pool    db.Pool
	fetcher fetcher.Fetcher
	reg     *geoscraper.Registry
	queue   *geospatial.GeocodeQueue
	tempDir string
	cfg     *config.Config
}

// NewActivities creates a new geo scraper Activities instance.
func NewActivities(pool db.Pool, f fetcher.Fetcher, syncLog *fedsync.SyncLog, reg *geoscraper.Registry, queue *geospatial.GeocodeQueue, tempDir string, cfg *config.Config) *Activities {
	return &Activities{
		SyncLogActivities: sdk.SyncLogActivities{SyncLog: syncLog},
		pool:              pool,
		fetcher:           f,
		reg:               reg,
		queue:             queue,
		tempDir:           tempDir,
		cfg:               cfg,
	}
}

// SelectScrapersParams is the input for SelectScrapers.
type SelectScrapersParams struct {
	Category *string  `json:"category,omitempty"`
	Sources  []string `json:"sources,omitempty"`
	States   []string `json:"states,omitempty"`
	Force    bool     `json:"force"`
}

// SelectScrapersResult is the output of SelectScrapers.
type SelectScrapersResult struct {
	ScraperNames []string `json:"scraper_names"`
}

// SelectScrapers determines which scrapers need running based on category, names, and scheduling.
func (a *Activities) SelectScrapers(ctx context.Context, params SelectScrapersParams) (*SelectScrapersResult, error) {
	now := time.Now().UTC()

	var category *geoscraper.Category
	if params.Category != nil {
		c, err := geoscraper.ParseCategory(*params.Category)
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidCategory", err)
		}
		category = &c
	}

	scrapers, err := a.reg.Select(category, params.Sources, params.States)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "SelectionError", err)
	}

	var names []string
	for _, s := range scrapers {
		if params.Force {
			names = append(names, s.Name())
			continue
		}
		lastSync, err := a.SyncLog.LastSuccess(ctx, s.Name())
		if err != nil {
			return nil, eris.Wrapf(err, "check last sync for %s", s.Name())
		}
		if s.ShouldRun(now, lastSync) {
			names = append(names, s.Name())
		}
	}

	return &SelectScrapersResult{ScraperNames: names}, nil
}

// SyncScraperParams is the input for SyncScraper.
type SyncScraperParams struct {
	Scraper string `json:"scraper"`
}

// SyncScraperResult is the output of SyncScraper.
type SyncScraperResult = sdk.SyncItemResult

// SyncScraper runs the actual data download, parse, and load for a single scraper.
// It sends heartbeats every 30 seconds for liveness detection.
func (a *Activities) SyncScraper(ctx context.Context, params SyncScraperParams) (*SyncScraperResult, error) {
	log := zap.L().With(zap.String("scraper", params.Scraper))

	s, err := a.reg.Get(params.Scraper)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unknown scraper: %s", params.Scraper),
			"UnknownScraper", err)
	}

	var result *geoscraper.SyncResult
	syncErr := sdk.RunWithHeartbeat(ctx, fmt.Sprintf("syncing %s", params.Scraper), 30*time.Second, func(ctx context.Context) error {
		log.Info("running scraper sync via Temporal")
		var err error
		result, err = s.Sync(ctx, a.pool, a.fetcher, a.tempDir)
		return err
	})

	if syncErr != nil {
		return nil, eris.Wrapf(syncErr, "sync scraper %s", params.Scraper)
	}

	// PostSync: enqueue addresses for geocoding if applicable.
	if a.queue != nil {
		if ap, ok := s.(geoscraper.AddressProducer); ok && ap.HasAddresses() {
			if psErr := geoscraper.PostSyncGeocode(ctx, a.pool, a.queue, s.Table(), result); psErr != nil {
				log.Warn("postsync geocode failed", zap.Error(psErr))
			}
		}
	}

	return &SyncScraperResult{
		RowsSynced: result.RowsSynced,
		Metadata:   result.Metadata,
	}, nil
}
