package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
)

var runsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Inspect enrichment run history",
	Long:  "Commands for listing, viewing, and summarizing enrichment runs.",
}

// -- runs list --

var runsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List enrichment runs",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		st, err := initStore(ctx)
		if err != nil {
			return err
		}
		defer st.Close() //nolint:errcheck
		if err := st.Migrate(ctx); err != nil {
			return err
		}

		status, _ := cmd.Flags().GetString("status")
		company, _ := cmd.Flags().GetString("company")
		errCat, _ := cmd.Flags().GetString("error-category")
		limit, _ := cmd.Flags().GetInt("limit")

		filter := store.RunFilter{
			Status:        model.RunStatus(status),
			CompanyURL:    company,
			ErrorCategory: model.ErrorCategory(errCat),
			Limit:         limit,
		}

		runs, err := st.ListRuns(ctx, filter)
		if err != nil {
			return eris.Wrap(err, "runs list")
		}

		if len(runs) == 0 {
			fmt.Fprintln(os.Stderr, "No runs found.")
			return nil
		}

		formatRunsList(os.Stdout, runs)
		return nil
	},
}

// -- runs show --

var runsShowCmd = &cobra.Command{
	Use:   "show <run-id>",
	Short: "Show full details of a run",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		st, err := initStore(ctx)
		if err != nil {
			return err
		}
		defer st.Close() //nolint:errcheck
		if err := st.Migrate(ctx); err != nil {
			return err
		}

		run, err := st.GetRun(ctx, args[0])
		if err != nil {
			return eris.Wrap(err, "runs show")
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(run)
	},
}

// -- runs stats --

var runsStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show aggregate run statistics",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		st, err := initStore(ctx)
		if err != nil {
			return err
		}
		defer st.Close() //nolint:errcheck
		if err := st.Migrate(ctx); err != nil {
			return err
		}

		since, _ := cmd.Flags().GetDuration("since")
		filter := store.RunFilter{}
		if since > 0 {
			filter.CreatedAfter = time.Now().Add(-since)
		}
		filter.Limit = 10000 // high limit for stats

		runs, err := st.ListRuns(ctx, filter)
		if err != nil {
			return eris.Wrap(err, "runs stats")
		}

		stats := computeRunStats(runs)
		formatRunStats(os.Stdout, stats)
		return nil
	},
}

func init() {
	runsListCmd.Flags().String("status", "", "filter by run status (queued, crawling, complete, failed, ...)")
	runsListCmd.Flags().String("company", "", "filter by company URL")
	runsListCmd.Flags().String("error-category", "", "filter by error category (transient, permanent)")
	runsListCmd.Flags().Int("limit", 50, "max number of runs to display")

	runsStatsCmd.Flags().Duration("since", 24*time.Hour, "time window for stats (e.g. 24h, 72h, 168h)")

	runsCmd.AddCommand(runsListCmd)
	runsCmd.AddCommand(runsShowCmd)
	runsCmd.AddCommand(runsStatsCmd)
	rootCmd.AddCommand(runsCmd)
}

// runStats holds aggregate statistics computed from a set of runs.
type runStats struct {
	Total      int
	Complete   int
	Failed     int
	Transient  int
	Permanent  int
	Other      int
	AvgDurSecs float64
}

// computeRunStats computes aggregate statistics from a list of runs.
func computeRunStats(runs []model.Run) runStats {
	var s runStats
	s.Total = len(runs)

	var totalDur time.Duration
	var durCount int

	for _, r := range runs {
		switch r.Status {
		case model.RunStatusComplete:
			s.Complete++
			dur := r.UpdatedAt.Sub(r.CreatedAt)
			totalDur += dur
			durCount++
		case model.RunStatusFailed:
			s.Failed++
			if r.Error != nil {
				switch r.Error.Category {
				case model.ErrorCategoryTransient:
					s.Transient++
				case model.ErrorCategoryPermanent:
					s.Permanent++
				default:
					s.Other++
				}
			} else {
				s.Other++
			}
		default:
			s.Other++
		}
	}

	if durCount > 0 {
		s.AvgDurSecs = totalDur.Seconds() / float64(durCount)
	}
	return s
}

// formatRunsList writes a tabular list of runs to w.
func formatRunsList(out io.Writer, runs []model.Run) {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tCOMPANY\tSTATUS\tERROR_CAT\tCREATED\tDURATION")
	_, _ = fmt.Fprintln(w, "--\t-------\t------\t---------\t-------\t--------")

	for _, r := range runs {
		dur := r.UpdatedAt.Sub(r.CreatedAt).Round(time.Second).String()

		errCat := ""
		if r.Error != nil {
			errCat = string(r.Error.Category)
		}

		company := r.Company.URL
		if r.Company.Name != "" {
			company = r.Company.Name
		}
		if len(company) > 30 {
			company = company[:27] + "..."
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			truncateID(r.ID),
			company,
			r.Status,
			errCat,
			r.CreatedAt.Format("2006-01-02 15:04"),
			dur,
		)
	}
	_ = w.Flush()
}

// formatRunStats writes aggregate stats to w.
func formatRunStats(out io.Writer, s runStats) {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "Total runs:\t%d\n", s.Total)
	_, _ = fmt.Fprintf(w, "Complete:\t%d\n", s.Complete)
	_, _ = fmt.Fprintf(w, "Failed:\t%d\n", s.Failed)
	_, _ = fmt.Fprintf(w, "  Transient:\t%d\n", s.Transient)
	_, _ = fmt.Fprintf(w, "  Permanent:\t%d\n", s.Permanent)
	_, _ = fmt.Fprintf(w, "  Unclassified:\t%d\n", s.Failed-s.Transient-s.Permanent)
	_, _ = fmt.Fprintf(w, "Other:\t%d\n", s.Other)
	if s.AvgDurSecs > 0 {
		_, _ = fmt.Fprintf(w, "Avg duration:\t%.1fs\n", s.AvgDurSecs)
	}
	_ = w.Flush()
}

// truncateID returns the first 8 characters of a UUID for compact display.
func truncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
