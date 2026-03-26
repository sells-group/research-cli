package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfillFDICCmd = &cobra.Command{
	Use:   "backfill-fdic",
	Short: "Create stub companies for FDIC institutions and geocode them",
	Long: `Creates stub company records for FDIC institutions that don't yet exist in
public.companies, geocodes their addresses (using FDIC-provided lat/lng when
available, PostGIS TIGER otherwise), creates branch sub-locations, associates
them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.fdic_institutions and the geo pipeline
so the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.SourceFDIC,
			"geo backfill-fdic",
			"No unlinked FDIC institutions found",
			"FDIC",
			true,
			false,
		)
	},
}

func init() {
	f := geoBackfillFDICCmd.Flags()
	f.Int("limit", 1000, "maximum number of FDIC institutions to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("skip-branches", false, "skip branch location creation")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfillFDICCmd)
}
