package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfillSBACmd = &cobra.Command{
	Use:   "backfill-sba",
	Short: "Create stub companies for SBA 7(a)/504 loan borrowers and geocode them",
	Long: `Creates stub company records for SBA 7(a) and 504 loan borrowers that don't
yet exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.sba_loans and the geo pipeline so the
scorer can use MSA-aware geo_match scoring. Borrowers are aggregated by l2locid
and ordered by most recent approval year, largest loan first.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.SourceSBA,
			"geo backfill-sba",
			"No unlinked SBA loan borrowers found",
			"SBA",
			false,
			true,
		)
	},
}

func init() {
	f := geoBackfillSBACmd.Flags()
	f.Int("limit", 10000, "maximum number of SBA loan borrowers to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-geocode", false, "skip geocoding step")
	f.Bool("skip-msa", false, "skip MSA association step")
	geoCmd.AddCommand(geoBackfillSBACmd)
}
