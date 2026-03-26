package main

import (
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/geobackfill"
)

var geoBackfillNCUACmd = &cobra.Command{
	Use:   "backfill-ncua",
	Short: "Create stub companies for NCUA credit unions and geocode them",
	Long: `Creates stub company records for NCUA credit unions that don't yet exist in
public.companies, geocodes their addresses via PostGIS TIGER, associates
them with MSAs, and links them via company_matches.

This bridges the gap between fed_data.ncua_call_reports and the geo pipeline
so the scorer can use MSA-aware geo_match scoring.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		return runGeoEntityBackfill(
			ctx,
			cmd,
			geobackfill.SourceNCUA,
			"geo backfill-ncua",
			"No unlinked NCUA credit unions found",
			"NCUA",
			true,
			false,
		)
	},
}

func init() {
	f := geoBackfillNCUACmd.Flags()
	f.Int("limit", 10000, "maximum number of credit unions to process")
	f.Int("batch-size", 100, "batch size for processing")
	f.Int("concurrency", 10, "maximum parallel geocode calls")
	f.Bool("skip-msa", false, "skip MSA association step")
	f.Bool("temporal", false, "run via Temporal workflow")
	geoCmd.AddCommand(geoBackfillNCUACmd)
}
