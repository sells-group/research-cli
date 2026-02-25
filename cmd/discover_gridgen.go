package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/discovery"
)

var discoverGridGenCmd = &cobra.Command{
	Use:   "grid-gen",
	Short: "Generate MSA grid cells for organic search",
	Long:  "Import CBSA shapefiles and/or generate grid cells for a target MSA.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if err := cfg.Validate("discovery"); err != nil {
			return err
		}

		log := zap.L().With(zap.String("command", "discover.grid-gen"))

		pool, err := discoveryPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		importShapefiles, _ := cmd.Flags().GetBool("import-shapefiles")
		cbsa, _ := cmd.Flags().GetString("cbsa")
		cellKM, _ := cmd.Flags().GetFloat64("cell-km")

		if importShapefiles {
			shapefile, _ := cmd.Flags().GetString("shapefile")
			if shapefile == "" {
				return eris.New("--shapefile path is required with --import-shapefiles")
			}
			count, err := discovery.ImportCBSA(ctx, pool, shapefile)
			if err != nil {
				return eris.Wrap(err, "grid-gen: import CBSA shapefiles")
			}
			log.Info("CBSA import complete", zap.Int64("areas", count))
		}

		if cbsa != "" {
			count, err := discovery.GenerateCells(ctx, pool, cbsa, cellKM)
			if err != nil {
				return eris.Wrap(err, "grid-gen: generate cells")
			}
			log.Info("grid generation complete",
				zap.String("cbsa", cbsa),
				zap.Float64("cell_km", cellKM),
				zap.Int64("cells", count),
			)
		}

		if !importShapefiles && cbsa == "" {
			return eris.New("specify --cbsa and/or --import-shapefiles")
		}

		return nil
	},
}

func init() {
	discoverGridGenCmd.Flags().String("cbsa", "", "CBSA code for target MSA")
	discoverGridGenCmd.Flags().Float64("cell-km", 2.0, "grid cell size in kilometers")
	discoverGridGenCmd.Flags().Bool("import-shapefiles", false, "import Census TIGER CBSA shapefiles")
	discoverGridGenCmd.Flags().String("shapefile", "", "path to CBSA shapefile (.shp)")
	discoverCmd.AddCommand(discoverGridGenCmd)
}
