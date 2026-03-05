package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
)

var fedsyncSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync federal datasets",
	Long: `Sync federal datasets into fed_data.* tables.

By default, syncs all datasets whose ShouldRun() returns true.
Use --phase to restrict to a specific phase, or --datasets for specific datasets.
Use --force to ignore ShouldRun() scheduling logic.
Use --full to perform a full reload instead of incremental sync.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "fedsync.sync"))

		if shouldUseTemporal(cmd) {
			return runFedsyncViaTemporal(ctx, cmd, log)
		}

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Ensure schema is current via Atlas.
		if err := ensureSchema(ctx); err != nil {
			return eris.Wrap(err, "fedsync sync: ensure schema")
		}

		// Parse flags.
		opts, err := parseSyncOpts(cmd)
		if err != nil {
			return err
		}

		// Create temp directory with a unique run-specific subdirectory.
		tempDir := cfg.Fedsync.TempDir
		if err := os.MkdirAll(tempDir, 0o750); err != nil {
			return eris.Wrapf(err, "fedsync sync: create temp dir %s", tempDir)
		}
		runDir := filepath.Join(tempDir, fmt.Sprintf("run-%d", time.Now().UnixNano()))
		if err := os.MkdirAll(runDir, 0o750); err != nil {
			return eris.Wrapf(err, "fedsync sync: create run dir %s", runDir)
		}
		defer os.RemoveAll(runDir) //nolint:errcheck

		// Build fetcher.
		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			UserAgent:  cfg.Fedsync.EDGARUserAgent,
			MaxRetries: 3,
			Timeout:    30 * time.Minute,
		})

		// Build engine.
		syncLog := fedsync.NewSyncLog(pool)
		reg := dataset.NewRegistry(cfg)
		engine := dataset.NewEngine(pool, f, syncLog, reg, runDir)

		log.Info("starting fedsync",
			zap.Any("phase", opts.Phase),
			zap.Strings("datasets", opts.Datasets),
			zap.Bool("force", opts.Force),
			zap.Bool("full", opts.Full),
		)

		if err := engine.Run(ctx, opts); err != nil {
			return eris.Wrap(err, "fedsync sync")
		}

		zap.L().Info("sync complete")
		return nil
	},
}

func init() {
	fedsyncSyncCmd.Flags().String("phase", "", "restrict to phase: 1, 1b, 2, 3")
	fedsyncSyncCmd.Flags().String("datasets", "", "comma-separated dataset names (e.g., cbp,fpds)")
	fedsyncSyncCmd.Flags().Bool("force", false, "ignore ShouldRun() scheduling logic")
	fedsyncSyncCmd.Flags().Bool("full", false, "full reload instead of incremental sync")
	addDirectFlag(fedsyncSyncCmd)
	fedsyncSyncCmd.Flags().Bool("wait", true, "wait for Temporal workflow completion (only with --temporal)")
	fedsyncCmd.AddCommand(fedsyncSyncCmd)
}

// runFedsyncViaTemporal starts a FedsyncRunWorkflow on Temporal.
func runFedsyncViaTemporal(ctx context.Context, cmd *cobra.Command, _ *zap.Logger) error {
	opts, err := parseSyncOpts(cmd)
	if err != nil {
		return err
	}

	params := temporalfedsync.RunParams{
		Force: opts.Force,
		Full:  opts.Full,
	}
	if opts.Phase != nil {
		ps := opts.Phase.String()
		params.Phase = &ps
	}
	if len(opts.Datasets) > 0 {
		params.Datasets = opts.Datasets
	}

	wait, _ := cmd.Flags().GetBool("wait")
	return startFedsyncWorkflow(ctx, params, wait)
}

// startFedsyncWorkflow starts a fedsync RunWorkflow on Temporal and optionally waits for completion.
func startFedsyncWorkflow(ctx context.Context, params temporalfedsync.RunParams, wait bool) error {
	c, err := temporalpkg.NewClient(cfg.Temporal)
	if err != nil {
		return err
	}
	defer c.Close()

	zap.L().Info("starting fedsync via Temporal",
		zap.Any("phase", params.Phase),
		zap.Strings("datasets", params.Datasets),
		zap.Bool("force", params.Force),
	)

	workflowID := fmt.Sprintf("fedsync-run-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporalpkg.FedsyncTaskQueue,
	}, temporalfedsync.RunWorkflow, params)
	if err != nil {
		return eris.Wrap(err, "start fedsync workflow")
	}

	zap.L().Info("fedsync workflow started",
		zap.String("workflow_id", run.GetID()),
		zap.String("run_id", run.GetRunID()),
	)

	if !wait {
		fmt.Printf("Workflow started: %s (run: %s)\n", run.GetID(), run.GetRunID())
		return nil
	}

	var result temporalfedsync.RunResult
	if err := run.Get(ctx, &result); err != nil {
		return eris.Wrap(err, "fedsync workflow failed")
	}

	fmt.Printf("Fedsync complete: %d synced, %d failed\n", result.Synced, result.Failed)
	return nil
}

// parseSyncOpts extracts dataset.RunOpts from the cobra command flags.
func parseSyncOpts(cmd *cobra.Command) (dataset.RunOpts, error) {
	phaseStr, _ := cmd.Flags().GetString("phase")
	datasetsStr, _ := cmd.Flags().GetString("datasets")
	force, _ := cmd.Flags().GetBool("force")
	full, _ := cmd.Flags().GetBool("full")

	opts := dataset.RunOpts{
		Force: force,
		Full:  full,
	}

	if phaseStr != "" {
		p, err := dataset.ParsePhase(phaseStr)
		if err != nil {
			return dataset.RunOpts{}, err
		}
		opts.Phase = &p
	}

	if datasetsStr != "" {
		opts.Datasets = strings.Split(datasetsStr, ",")
		for i := range opts.Datasets {
			opts.Datasets[i] = strings.TrimSpace(opts.Datasets[i])
		}
	}

	return opts, nil
}
