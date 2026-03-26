package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfill5500Cmd = &cobra.Command{
	Use:   "backfill-5500",
	Short: "Create stub companies for Form 5500 plan sponsors and geocode them",
	Long: `Creates stub company records for Form 5500 plan sponsors that don't yet
exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.form_5500 / form_5500_sf and the
geo pipeline so the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.Source5500,
			"geo backfill-5500",
			"No unlinked Form 5500 sponsors found",
			"Form 5500",
			true,
			false,
		)
	},
}

func init() {
	f := geoBackfill5500Cmd.Flags()
	f.Int("limit", 10000, "maximum number of Form 5500 sponsors to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfill5500Cmd)
}
