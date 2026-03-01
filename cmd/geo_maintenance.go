package main

import (
	"fmt"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/geospatial"
)

var geoMaintenanceCmd = &cobra.Command{
	Use:   "maintenance",
	Short: "Run geo schema maintenance tasks",
	Long:  "Run VACUUM ANALYZE, CLUSTER, REINDEX, and report table statistics for the geo schema.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if err := cfg.Validate("fedsync"); err != nil {
			return err
		}

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		vacuum, _ := cmd.Flags().GetBool("vacuum")
		cluster, _ := cmd.Flags().GetBool("cluster")
		reindex, _ := cmd.Flags().GetBool("reindex")
		stats, _ := cmd.Flags().GetBool("stats")

		// Default: show stats if no specific action requested.
		if !vacuum && !cluster && !reindex && !stats {
			stats = true
		}

		if vacuum {
			zap.L().Info("running VACUUM ANALYZE on geo tables")
			if err := geospatial.VacuumAnalyze(ctx, pool); err != nil {
				return eris.Wrap(err, "geo maintenance vacuum")
			}
			zap.L().Info("VACUUM ANALYZE complete")
		}

		if cluster {
			zap.L().Info("clustering geo tables by spatial indexes")
			if err := geospatial.ClusterSpatialIndexes(ctx, pool); err != nil {
				return eris.Wrap(err, "geo maintenance cluster")
			}
			zap.L().Info("CLUSTER complete")
		}

		if reindex {
			zap.L().Info("reindexing geo schema")
			if err := geospatial.ReindexSpatial(ctx, pool); err != nil {
				return eris.Wrap(err, "geo maintenance reindex")
			}
			zap.L().Info("REINDEX complete")
		}

		if stats {
			tableStats, err := geospatial.GetTableStats(ctx, pool)
			if err != nil {
				return eris.Wrap(err, "geo maintenance stats")
			}
			fmt.Printf("%-30s %10s %12s %12s %8s\n", "Table", "Rows", "Total Size", "Index Size", "Spatial")
			fmt.Println("------------------------------------------------------------------------------------")
			for _, s := range tableStats {
				spatial := "no"
				if s.HasSpatial {
					spatial = "yes"
				}
				fmt.Printf("%-30s %10d %12s %12s %8s\n", s.TableName, s.RowCount, s.TotalSize, s.IndexSize, spatial)
			}
		}

		return nil
	},
}

func init() {
	geoCmd.AddCommand(geoMaintenanceCmd)
	geoMaintenanceCmd.Flags().Bool("vacuum", false, "Run VACUUM ANALYZE on all geo tables")
	geoMaintenanceCmd.Flags().Bool("cluster", false, "Cluster geo tables by spatial indexes")
	geoMaintenanceCmd.Flags().Bool("reindex", false, "Reindex the entire geo schema")
	geoMaintenanceCmd.Flags().Bool("stats", false, "Show table statistics")
}
