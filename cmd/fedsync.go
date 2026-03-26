package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

var fedsyncCmd = &cobra.Command{
	Use:   "fedsync",
	Short: "Federal data sync pipeline",
	Long:  datasetCommandLong(),
}

func init() {
	rootCmd.AddCommand(fedsyncCmd)
}

func datasetCommandLong() string {
	summary := dataset.BuildSummary(nil)
	return fmt.Sprintf("Incrementally syncs %d federal datasets into fed_data.* Postgres tables.", summary.Total)
}

// fedsyncPool creates a pgxpool.Pool for the fedsync subsystem.
// Uses cfg.Fedsync.DatabaseURL, falling back to cfg.Store.DatabaseURL.
func fedsyncPool(ctx context.Context) (*pgxpool.Pool, error) {
	return openReadModelPool(ctx)
}
