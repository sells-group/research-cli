package main

import (
	"fmt"
	"os/signal"
	"syscall"
	"text/tabwriter"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/analysis"
)

var geoAnalyzeStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show analysis run history",
	Long:  "Display the analysis log showing recent analyzer run results.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Ensure schema is current via Atlas.
		if err := ensureSchema(ctx); err != nil {
			return eris.Wrap(err, "geo analyze status: ensure schema")
		}

		alog := analysis.NewLog(pool)
		entries, err := alog.ListAll(ctx)
		if err != nil {
			return eris.Wrap(err, "geo analyze status")
		}

		if len(entries) == 0 {
			fmt.Println("No analysis runs recorded.")
			return nil
		}

		fmt.Println("=== Analysis Run History ===")
		fmt.Println()

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "ID\tAnalyzer\tStatus\tStarted\tRows\tError")
		_, _ = fmt.Fprintln(w, "--\t--------\t------\t-------\t----\t-----")

		for _, e := range entries {
			errMsg := ""
			if e.Error != "" {
				errMsg = truncate(e.Error, 40)
			}
			_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%s\n",
				e.ID,
				e.Analyzer,
				e.Status,
				e.StartedAt.Format("2006-01-02 15:04"),
				e.RowsAffected,
				errMsg,
			)
		}
		return w.Flush()
	},
}

func init() {
	geoAnalyzeCmd.AddCommand(geoAnalyzeStatusCmd)
}
