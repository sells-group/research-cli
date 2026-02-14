package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
)

var fedsyncSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync federal datasets",
	Long: `Sync federal datasets into fed_data.* tables.

By default, syncs all datasets whose ShouldRun() returns true.
Use --phase to restrict to a specific phase, or --datasets for specific datasets.
Use --force to ignore ShouldRun() scheduling logic.
Use --full to perform a full reload instead of incremental sync.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		log := zap.L().With(zap.String("command", "fedsync.sync"))

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Ensure migrations are current.
		if err := fedsync.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "fedsync sync: migrate")
		}

		// Parse flags.
		opts, err := parseSyncOpts(cmd)
		if err != nil {
			return err
		}

		// Create temp directory.
		tempDir := cfg.Fedsync.TempDir
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return eris.Wrapf(err, "fedsync sync: create temp dir %s", tempDir)
		}

		// Build fetcher.
		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			UserAgent:  cfg.Fedsync.EDGARUserAgent,
			MaxRetries: 3,
		})

		// Build engine.
		syncLog := fedsync.NewSyncLog(pool)
		reg := dataset.NewRegistry(cfg)
		engine := dataset.NewEngine(pool, f, syncLog, reg, tempDir)

		log.Info("starting fedsync",
			zap.Any("phase", opts.Phase),
			zap.Strings("datasets", opts.Datasets),
			zap.Bool("force", opts.Force),
			zap.Bool("full", opts.Full),
		)

		if err := engine.Run(ctx, opts); err != nil {
			return eris.Wrap(err, "fedsync sync")
		}

		fmt.Println("Sync complete")
		return nil
	},
}

func init() {
	fedsyncSyncCmd.Flags().String("phase", "", "restrict to phase: 1, 1b, 2, 3")
	fedsyncSyncCmd.Flags().String("datasets", "", "comma-separated dataset names (e.g., cbp,fpds)")
	fedsyncSyncCmd.Flags().Bool("force", false, "ignore ShouldRun() scheduling logic")
	fedsyncSyncCmd.Flags().Bool("full", false, "full reload instead of incremental sync")
	fedsyncCmd.AddCommand(fedsyncSyncCmd)
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
