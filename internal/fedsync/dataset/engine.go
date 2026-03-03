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
	var entitySynced atomic.Bool

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, ds := range datasets {
		// capture loop variable
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
			syncCtx, syncCancel := context.WithTimeout(gctx, 60*time.Minute)
			var result *SyncResult
			if opts.Full {
				if fs, ok := ds.(FullSyncer); ok {
					dsLog.Info("running full sync")
					result, err = fs.SyncFull(syncCtx, e.pool, e.fetcher, e.tempDir)
				} else {
					result, err = ds.Sync(syncCtx, e.pool, e.fetcher, e.tempDir)
				}
			} else {
				result, err = ds.Sync(syncCtx, e.pool, e.fetcher, e.tempDir)
			}
			syncCancel()
			elapsed := time.Since(start)

			if syncCtx.Err() == context.DeadlineExceeded {
				dsLog.Warn("sync timed out after 60 minutes", zap.Duration("elapsed", elapsed))
			}

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

			if entityBearingDatasets[ds.Name()] {
				entitySynced.Store(true)
			}
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

	// Auto-trigger entity cross-reference rebuild when entity-bearing
	// datasets were synced so new records are immediately linked into
	// the relationship web.
	if entitySynced.Load() && !e.xrefInSelection(datasets) {
		log.Info("auto-triggering entity_xref rebuild after entity-bearing sync")
		if err := e.runXref(ctx, log); err != nil {
			log.Error("entity_xref auto-rebuild failed", zap.Error(err))
		}
	}

	return nil
}

// entityBearingDatasets lists dataset names whose records contain firm/company/
// entity-level data with identifiers (CRD, CIK, EIN, DUNS, UEI) or names and
// geography. When any of these syncs successfully, the engine auto-triggers an
// entity_xref rebuild to link new records into the relationship web.
//
// IMPORTANT: When adding a new entity-bearing dataset, add its Name() here and
// add corresponding cross-reference passes to resolve/multi_xref.go.
// See CLAUDE.md "Entity Cross-Reference Checklist" for the full procedure.
var entityBearingDatasets = map[string]bool{
	"adv_part1":         true,
	"ia_compilation":    true,
	"brokercheck":       true,
	"form_bd":           true,
	"edgar_submissions": true,
	"form_d":            true,
	"ncen":              true,
	"form_5500":         true,
	"eo_bmf":            true,
	"fdic_bankfind":     true,
	"usaspending":       true,
	"fpds":              true,
	"ppp":               true,
	"osha_ita":          true,
	"epa_echo":          true,
	"sba_7a_504":        true,
}

// xrefInSelection returns true if entity_xref is already part of the dataset
// selection, so we don't trigger it twice.
func (e *Engine) xrefInSelection(datasets []Dataset) bool {
	for _, ds := range datasets {
		if ds.Name() == "entity_xref" {
			return true
		}
	}
	return false
}

// runXref runs the entity cross-reference builder and records the result
// in the sync log.
func (e *Engine) runXref(ctx context.Context, log *zap.Logger) error {
	xref := &EntityXref{}
	syncID, err := e.syncLog.Start(ctx, xref.Name())
	if err != nil {
		return eris.Wrap(err, "engine: start entity_xref sync log")
	}

	start := time.Now()
	result, err := xref.Sync(ctx, e.pool, e.fetcher, e.tempDir)
	if err != nil {
		if logErr := e.syncLog.Fail(ctx, syncID, err.Error()); logErr != nil {
			log.Error("failed to record entity_xref failure", zap.Error(logErr))
		}
		return eris.Wrap(err, "engine: entity_xref sync")
	}

	fsResult := &fedsync.SyncResult{
		RowsSynced: result.RowsSynced,
		Metadata:   result.Metadata,
	}
	if err := e.syncLog.Complete(ctx, syncID, fsResult); err != nil {
		log.Error("failed to record entity_xref completion", zap.Error(err))
	}

	log.Info("entity_xref auto-rebuild complete",
		zap.Int64("rows", result.RowsSynced),
		zap.Duration("elapsed", time.Since(start)),
	)
	return nil
}
