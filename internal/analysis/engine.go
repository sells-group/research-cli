package analysis

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
)

// Engine orchestrates analysis runs with dependency-aware parallel execution.
type Engine struct {
	pool db.Pool
	log  *Log
	reg  *Registry
}

// NewEngine creates a new analysis engine.
func NewEngine(pool db.Pool, alog *Log, reg *Registry) *Engine {
	return &Engine{
		pool: pool,
		log:  alog,
		reg:  reg,
	}
}

// Run selects analyzers, validates dependencies, and executes them in
// topological order with parallelism within each dependency level.
func (e *Engine) Run(ctx context.Context, opts RunOpts) error {
	log := zap.L().With(zap.String("component", "analysis.engine"))

	analyzers, err := e.reg.Select(opts.Category, opts.Analyzers)
	if err != nil {
		return err
	}

	if len(analyzers) == 0 {
		log.Info("no analyzers selected")
		return nil
	}

	log.Info("selected analyzers", zap.Int("count", len(analyzers)))

	levels := buildLevels(analyzers)

	var completed, skipped, failed atomic.Int64
	completedSet := &sync.Map{}

	for i, level := range levels {
		log.Info("starting level",
			zap.Int("level", i+1),
			zap.Int("analyzers", len(level)),
		)

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(5)

		for _, a := range level {
			g.Go(func() error {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				aLog := log.With(
					zap.String("analyzer", a.Name()),
					zap.String("category", a.Category().String()),
				)

				// Check all dependencies completed successfully.
				for _, dep := range a.Dependencies() {
					if _, ok := completedSet.Load(dep); !ok {
						aLog.Warn("skipping: dependency not satisfied",
							zap.String("missing_dep", dep))
						skipped.Add(1)
						return nil
					}
				}

				// Validate source data unless --force.
				if !opts.Force {
					if err := a.Validate(gctx, e.pool); err != nil {
						aLog.Warn("skipping: validation failed", zap.Error(err))
						skipped.Add(1)
						return nil
					}
				}

				aLog.Info("starting analysis")
				runID, err := e.log.Start(gctx, a.Name())
				if err != nil {
					return eris.Wrapf(err, "engine: start analysis log for %s", a.Name())
				}

				start := time.Now()
				runCtx, runCancel := context.WithTimeout(gctx, 60*time.Minute)
				result, err := a.Run(runCtx, e.pool, opts)
				runCancel()
				elapsed := time.Since(start)

				if runCtx.Err() == context.DeadlineExceeded {
					aLog.Warn("analysis timed out after 60 minutes",
						zap.Duration("elapsed", elapsed))
				}

				if err != nil {
					aLog.Error("analysis failed",
						zap.Error(err), zap.Duration("elapsed", elapsed))
					if logErr := e.log.Fail(gctx, runID, err.Error()); logErr != nil {
						aLog.Error("failed to record analysis failure", zap.Error(logErr))
					}
					failed.Add(1)
					return nil // don't abort other analyzers
				}

				if err := e.log.Complete(gctx, runID, result); err != nil {
					aLog.Error("failed to record analysis completion", zap.Error(err))
				}

				aLog.Info("analysis complete",
					zap.Int64("rows", result.RowsAffected),
					zap.Duration("elapsed", elapsed),
				)
				completed.Add(1)
				completedSet.Store(a.Name(), true)
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}
	}

	log.Info("engine run complete",
		zap.Int64("completed", completed.Load()),
		zap.Int64("skipped", skipped.Load()),
		zap.Int64("failed", failed.Load()),
	)
	return nil
}

// buildLevels groups analyzers into parallel execution levels.
// Level 0: analyzers with no dependencies.
// Level N: analyzers whose dependencies are all in levels < N.
func buildLevels(sorted []Analyzer) [][]Analyzer {
	if len(sorted) == 0 {
		return nil
	}

	levelOf := make(map[string]int, len(sorted))

	for _, a := range sorted {
		maxDepLevel := -1
		for _, dep := range a.Dependencies() {
			if l, ok := levelOf[dep]; ok && l > maxDepLevel {
				maxDepLevel = l
			}
		}
		levelOf[a.Name()] = maxDepLevel + 1
	}

	maxLevel := 0
	for _, l := range levelOf {
		if l > maxLevel {
			maxLevel = l
		}
	}

	levels := make([][]Analyzer, maxLevel+1)
	for _, a := range sorted {
		l := levelOf[a.Name()]
		levels[l] = append(levels[l], a)
	}

	return levels
}
