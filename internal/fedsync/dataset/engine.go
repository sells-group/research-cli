package dataset

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
)

// Engine orchestrates dataset sync runs.
type Engine struct {
	pool    db.Pool
	fetcher fetcher.Fetcher
	syncLog *fedsync.SyncLog
	reg     *Registry
	tempDir string
}

// RunOpts configures which datasets to sync and how.
type RunOpts struct {
	Phase    *Phase   // restrict to a specific phase
	Datasets []string // restrict to specific dataset names
	Force    bool     // ignore ShouldRun() scheduling
	Full     bool     // full reload instead of incremental
}

// NewEngine creates a new sync engine.
func NewEngine(pool db.Pool, f fetcher.Fetcher, syncLog *fedsync.SyncLog, reg *Registry, tempDir string) *Engine {
	return &Engine{
		pool:    pool,
		fetcher: f,
		syncLog: syncLog,
		reg:     reg,
		tempDir: tempDir,
	}
}

// Run iterates over the selected datasets, checks if each needs syncing,
// and runs the sync in parallel. Results are recorded in the sync log.
func (e *Engine) Run(ctx context.Context, opts RunOpts) error {
	log := zap.L().With(zap.String("component", "fedsync.engine"))
	now := time.Now().UTC()

	datasets, err := e.reg.Select(opts.Phase, opts.Datasets)
	if err != nil {
		return err
	}

	if len(datasets) == 0 {
		log.Info("no datasets selected")
		return nil
	}

	log.Info("selected datasets", zap.Int("count", len(datasets)))

	var synced, skipped, failed atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, ds := range datasets {
		ds := ds // capture loop variable
		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			dsLog := log.With(zap.String("dataset", ds.Name()), zap.String("phase", ds.Phase().String()))

			if !opts.Force {
				lastSync, err := e.syncLog.LastSuccess(gctx, ds.Name())
				if err != nil {
					return eris.Wrapf(err, "engine: check last sync for %s", ds.Name())
				}

				if !ds.ShouldRun(now, lastSync) {
					dsLog.Debug("skipping (not due)")
					skipped.Add(1)
					return nil
				}
			}

			dsLog.Info("starting sync")
			syncID, err := e.syncLog.Start(gctx, ds.Name())
			if err != nil {
				return eris.Wrapf(err, "engine: start sync log for %s", ds.Name())
			}

			start := time.Now()
			result, err := ds.Sync(gctx, e.pool, e.fetcher, e.tempDir)
			elapsed := time.Since(start)

			if err != nil {
				dsLog.Error("sync failed", zap.Error(err), zap.Duration("elapsed", elapsed))
				if logErr := e.syncLog.Fail(gctx, syncID, err.Error()); logErr != nil {
					dsLog.Error("failed to record sync failure", zap.Error(logErr))
				}
				failed.Add(1)
				return nil // don't abort other datasets on individual failure
			}

			fsResult := &fedsync.SyncResult{
				RowsSynced: result.RowsSynced,
				Metadata:   result.Metadata,
			}

			if err := e.syncLog.Complete(gctx, syncID, fsResult); err != nil {
				dsLog.Error("failed to record sync completion", zap.Error(err))
			}

			dsLog.Info("sync complete",
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
