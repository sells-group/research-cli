package main

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync"
)

var fedsyncStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show fedsync sync log",
	Long:  "Displays the sync history for all federal datasets.",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		sl := fedsync.NewSyncLog(pool)
		entries, err := sl.ListAll(ctx)
		if err != nil {
			return eris.Wrap(err, "fedsync status")
		}

		if len(entries) == 0 {
			zap.L().Info("no sync entries found, run 'fedsync sync' to start syncing datasets")
			return nil
		}

		formatStatusEntries(os.Stdout, entries)
		return nil
	},
}

func init() {
	fedsyncCmd.AddCommand(fedsyncStatusCmd)
}

// formatStatusEntries writes a tabular representation of sync entries to w.
func formatStatusEntries(out io.Writer, entries []fedsync.SyncEntry) {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tDATASET\tSTATUS\tSTARTED\tDURATION\tROWS\tERROR")
	_, _ = fmt.Fprintln(w, "--\t-------\t------\t-------\t--------\t----\t-----")

	for _, e := range entries {
		dur := "-"
		if e.CompletedAt != nil {
			d := e.CompletedAt.Sub(e.StartedAt).Round(time.Second)
			dur = d.String()
		}

		errMsg := ""
		if e.Error != "" {
			errMsg = truncate(e.Error, 60)
		}

		_, _ = fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%d\t%s\n",
			e.ID,
			e.Dataset,
			e.Status,
			e.StartedAt.Format("2006-01-02 15:04"),
			dur,
			e.RowsSynced,
			errMsg,
		)
	}
	_ = w.Flush()
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
