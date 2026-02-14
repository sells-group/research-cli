package main

import (
	"fmt"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/fedsync"
)

var fedsyncMigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply fedsync schema migrations",
	Long:  "Applies all pending SQL migrations to the fed_data schema in lexicographic order.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := fedsync.Migrate(ctx, pool); err != nil {
			return eris.Wrap(err, "fedsync migrate")
		}

		fmt.Println("All migrations applied successfully")
		return nil
	},
}

func init() {
	fedsyncCmd.AddCommand(fedsyncMigrateCmd)
}
