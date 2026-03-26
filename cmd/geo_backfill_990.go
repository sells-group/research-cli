package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfill990Cmd = &cobra.Command{
	Use:   "backfill-990",
	Short: "Create stub companies for IRS EO BMF organizations and geocode them",
	Long: `Creates stub company records for IRS Exempt Organization BMF entries that
don't yet exist in public.companies, geocodes their addresses via PostGIS TIGER,
associates them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.eo_bmf and the geo pipeline so the
scorer can use MSA-aware geo_match scoring. Organizations are processed in
descending asset order so the largest nonprofits/foundations are linked first.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.Source990,
			"geo backfill-990",
			"No unlinked EO BMF organizations found",
			"EO BMF",
			true,
			false,
		)
	},
}

func init() {
	f := geoBackfill990Cmd.Flags()
	f.Int("limit", 10000, "maximum number of EO BMF organizations to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfill990Cmd)
}
