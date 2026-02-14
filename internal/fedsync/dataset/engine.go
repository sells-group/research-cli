package dataset

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// Engine orchestrates dataset sync runs.
type Engine struct {
	pool    *pgxpool.Pool
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
func NewEngine(pool *pgxpool.Pool, f fetcher.Fetcher, syncLog *fedsync.SyncLog, reg *Registry, tempDir string) *Engine {
	return &Engine{
		pool:    pool,
		fetcher: f,
		syncLog: syncLog,
		reg:     reg,
		tempDir: tempDir,
	}
}

// Run iterates over the selected datasets, checks if each needs syncing,
// and runs the sync. Results are recorded in the sync log.
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

	var synced, skipped, failed int

	for _, ds := range datasets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dsLog := log.With(zap.String("dataset", ds.Name()), zap.String("phase", ds.Phase().String()))

		if !opts.Force {
			lastSync, err := e.syncLog.LastSuccess(ctx, ds.Name())
			if err != nil {
				return eris.Wrapf(err, "engine: check last sync for %s", ds.Name())
			}

			if !ds.ShouldRun(now, lastSync) {
				dsLog.Debug("skipping (not due)")
				skipped++
				continue
			}
		}

		dsLog.Info("starting sync")
		syncID, err := e.syncLog.Start(ctx, ds.Name())
		if err != nil {
			return eris.Wrapf(err, "engine: start sync log for %s", ds.Name())
		}

		start := time.Now()
		result, err := ds.Sync(ctx, e.pool, e.fetcher, e.tempDir)
		elapsed := time.Since(start)

		if err != nil {
			dsLog.Error("sync failed", zap.Error(err), zap.Duration("elapsed", elapsed))
			if logErr := e.syncLog.Fail(ctx, syncID, err.Error()); logErr != nil {
				dsLog.Error("failed to record sync failure", zap.Error(logErr))
			}
			failed++
			continue
		}

		// Convert dataset.SyncResult to fedsync.SyncResult for the sync log.
		fsResult := &fedsync.SyncResult{
			RowsSynced: result.RowsSynced,
			Metadata:   result.Metadata,
		}

		if err := e.syncLog.Complete(ctx, syncID, fsResult); err != nil {
			dsLog.Error("failed to record sync completion", zap.Error(err))
		}

		dsLog.Info("sync complete",
			zap.Int64("rows", result.RowsSynced),
			zap.Duration("elapsed", elapsed),
		)
		synced++
	}

	log.Info("engine run complete",
		zap.Int("synced", synced),
		zap.Int("skipped", skipped),
		zap.Int("failed", failed),
	)
	return nil
}
