package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfillADVCmd = &cobra.Command{
	Use:   "backfill-adv",
	Short: "Create stub companies for ADV firms and geocode them",
	Long: `Creates stub company records for ADV firms that don't yet exist in
public.companies, geocodes their addresses via PostGIS TIGER, associates
them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.adv_firms and the geo pipeline so
the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.SourceADV,
			"geo backfill-adv",
			"No unlinked ADV firms found",
			"ADV",
			true,
			false,
		)
	},
}

func init() {
	f := geoBackfillADVCmd.Flags()
	f.Int("limit", 1000, "maximum number of ADV firms to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfillADVCmd)
}
