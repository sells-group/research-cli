package geoscraper

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geospatial"
)

// Engine orchestrates geo scraper runs.
type Engine struct {
	pool    db.Pool
	fetcher fetcher.Fetcher
	syncLog *fedsync.SyncLog
	reg     *Registry
	queue   *geospatial.GeocodeQueue
	tempDir string
}

// RunOpts configures which scrapers to run and how.
type RunOpts struct {
	Category *Category // restrict to a specific category
	Sources  []string  // restrict to specific scraper names
	States   []string  // filter StateScraper by state FIPS
	Force    bool      // ignore ShouldRun() scheduling
}

// NewEngine creates a new geo scraper engine.
func NewEngine(pool db.Pool, f fetcher.Fetcher, syncLog *fedsync.SyncLog, reg *Registry, queue *geospatial.GeocodeQueue, tempDir string) *Engine {
	return &Engine{
		pool:    pool,
		fetcher: f,
		syncLog: syncLog,
		reg:     reg,
		queue:   queue,
		tempDir: tempDir,
	}
}

// Run iterates over selected scrapers, checks scheduling, and runs syncs in parallel.
func (e *Engine) Run(ctx context.Context, opts RunOpts) error {
	log := zap.L().With(zap.String("component", "geoscraper.engine"))
	now := time.Now().UTC()

	scrapers, err := e.reg.Select(opts.Category, opts.Sources, opts.States)
	if err != nil {
		return err
	}

	if len(scrapers) == 0 {
		log.Info("no scrapers selected")
		return nil
	}

	log.Info("selected scrapers", zap.Int("count", len(scrapers)))

	var synced, skipped, failed atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, s := range scrapers {
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			sLog := log.With(
				zap.String("scraper", s.Name()),
				zap.String("category", s.Category().String()),
			)

			if !opts.Force {
				lastSync, err := e.syncLog.LastSuccess(gctx, s.Name())
				if err != nil {
					return eris.Wrapf(err, "engine: check last sync for %s", s.Name())
				}

				if !s.ShouldRun(now, lastSync) {
					sLog.Debug("skipping (not due)")
					skipped.Add(1)
					return nil
				}
			}

			sLog.Info("starting sync")
			syncID, err := e.syncLog.Start(gctx, s.Name())
			if err != nil {
				return eris.Wrapf(err, "engine: start sync log for %s", s.Name())
			}

			start := time.Now()
			syncCtx, syncCancel := context.WithTimeout(gctx, 60*time.Minute)
			result, err := s.Sync(syncCtx, e.pool, e.fetcher, e.tempDir)
			syncCancel()
			elapsed := time.Since(start)

			if syncCtx.Err() == context.DeadlineExceeded {
				sLog.Warn("sync timed out after 60 minutes", zap.Duration("elapsed", elapsed))
			}

			if err != nil {
				sLog.Error("sync failed", zap.Error(err), zap.Duration("elapsed", elapsed))
				if logErr := e.syncLog.Fail(gctx, syncID, err.Error()); logErr != nil {
					sLog.Error("failed to record sync failure", zap.Error(logErr))
				}
				failed.Add(1)
				return nil // don't abort other scrapers on individual failure
			}

			fsResult := &fedsync.SyncResult{
				RowsSynced: result.RowsSynced,
				Metadata:   result.Metadata,
			}

			if err := e.syncLog.Complete(gctx, syncID, fsResult); err != nil {
				sLog.Error("failed to record sync completion", zap.Error(err))
			}

			// PostSync: enqueue addresses for geocoding if applicable.
			if e.queue != nil {
				if ap, ok := s.(AddressProducer); ok && ap.HasAddresses() {
					if psErr := PostSyncGeocode(gctx, e.pool, e.queue, s.Table(), result); psErr != nil {
						sLog.Warn("postsync geocode failed", zap.Error(psErr))
					}
				}
			}

			sLog.Info("sync complete",
				zap.Int64("rows", result.RowsSynced),
				zap.Duration("elapsed", elapsed),
			)
			synced.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("engine run complete",
		zap.Int64("synced", synced.Load()),
		zap.Int64("skipped", skipped.Load()),
		zap.Int64("failed", failed.Load()),
	)
	return nil
}
