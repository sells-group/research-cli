package main

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type discoveryRunRow struct {
	ID          string
	Strategy    string
	Status      string
	Found       int
	Qualified   int
	Cost        float64
	StartedAt   string
	CompletedAt *string
	Error       *string
}

var discoverStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show discovery run status",
	Long:  "Display summary of discovery runs including candidates found, qualified, and cost.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		pool, err := discoveryPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		runID, _ := cmd.Flags().GetString("run-id")

		var query string
		var args []any
		if runID != "" {
			query = `SELECT id, strategy, status, candidates_found, candidates_qualified, cost_usd, started_at, completed_at, error
				FROM discovery_runs WHERE id = $1 ORDER BY started_at DESC`
			args = []any{runID}
		} else {
			query = `SELECT id, strategy, status, candidates_found, candidates_qualified, cost_usd, started_at, completed_at, error
				FROM discovery_runs ORDER BY started_at DESC LIMIT 20`
		}

		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return eris.Wrap(err, "discover status: query runs")
		}
		defer rows.Close()

		var results []discoveryRunRow
		for rows.Next() {
			var r discoveryRunRow
			var completedAt, errMsg *string
			if err := rows.Scan(&r.ID, &r.Strategy, &r.Status, &r.Found, &r.Qualified,
				&r.Cost, &r.StartedAt, &completedAt, &errMsg); err != nil {
				return eris.Wrap(err, "discover status: scan row")
			}
			r.CompletedAt = completedAt
			r.Error = errMsg
			results = append(results, r)
		}
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "discover status: iterate rows")
		}

		if len(results) == 0 {
			zap.L().Info("no discovery runs found")
			return nil
		}

		formatDiscoveryRuns(os.Stdout, results)
		return nil
	},
}

func init() {
	discoverStatusCmd.Flags().String("run-id", "", "filter by run ID")
	discoverCmd.AddCommand(discoverStatusCmd)
}

func formatDiscoveryRuns(out io.Writer, runs []discoveryRunRow) {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tSTRATEGY\tSTATUS\tFOUND\tQUALIFIED\tCOST\tERROR")
	_, _ = fmt.Fprintln(w, "--\t--------\t------\t-----\t---------\t----\t-----")

	for _, r := range runs {
		errMsg := ""
		if r.Error != nil {
			errMsg = *r.Error
			if len(errMsg) > 50 {
				errMsg = errMsg[:47] + "..."
			}
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t$%.2f\t%s\n",
			shortUUID(r.ID),
			r.Strategy,
			r.Status,
			r.Found,
			r.Qualified,
			r.Cost,
			errMsg,
		)
	}
	_ = w.Flush()
}

func shortUUID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
