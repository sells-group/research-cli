package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfillUsaspendingCmd = &cobra.Command{
	Use:   "backfill-usaspending",
	Short: "Create stub companies for USAspending recipients and geocode them",
	Long: `Creates stub company records for USAspending award recipients that don't yet
exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.usaspending_awards and the geo pipeline
so the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.SourceUSAspending,
			"geo backfill-usaspending",
			"No unlinked USAspending recipients found",
			"USAspending",
			false,
			false,
		)
	},
}

func init() {
	f := geoBackfillUsaspendingCmd.Flags()
	f.Int("limit", 10000, "maximum number of USAspending recipients to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	geoCmd.AddCommand(geoBackfillUsaspendingCmd)
}
