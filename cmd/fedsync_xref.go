package main

import (
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
)

var fedsyncXrefCmd = &cobra.Command{
	Use:   "xref",
	Short: "Build entity cross-reference table",
	Long: `Runs the entity_xref dataset to build cross-reference linkages across all federal datasets.

Stage 1: CRD↔CIK matching between ADV firms and EDGAR entities (3-pass).
Stage 2: Multi-dataset matching across ADV, EDGAR, BrokerCheck, Form BD, OSHA,
         EPA, FPDS, PPP, and Form D using direct CRD, direct CIK, exact name+zip,
         exact name+state, and fuzzy name+state strategies.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		log := zap.L().With(zap.String("command", "fedsync.xref"))

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := fedsync.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "fedsync xref: migrate")
		}

		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			UserAgent:  cfg.Fedsync.EDGARUserAgent,
			MaxRetries: 3,
			Timeout:    2 * time.Minute,
		})

		syncLog := fedsync.NewSyncLog(pool)
		reg := dataset.NewRegistry(cfg)
		engine := dataset.NewEngine(pool, f, syncLog, reg, cfg.Fedsync.TempDir)

		log.Info("building entity cross-reference")

		opts := dataset.RunOpts{
			Datasets: []string{"entity_xref"},
			Force:    true,
		}
		if err := engine.Run(ctx, opts); err != nil {
			return eris.Wrap(err, "fedsync xref")
		}

		zap.L().Info("entity cross-reference build complete")
		return nil
	},
}

func init() {
	fedsyncCmd.AddCommand(fedsyncXrefCmd)
}
