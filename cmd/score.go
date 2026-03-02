package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/scorer"
)

var scoreCmd = &cobra.Command{
	Use:   "score",
	Short: "Score firms using multi-pass funnel",
	Long: `Score SEC-registered RIAs using data from fed_data tables.

Pass 0 (ADV Score): Queries mv_firm_combined, adv_computed_metrics, and
brochure/CRS text for keyword matches. Produces a 0-100 score based on
configurable weights (AUM fit, growth, client quality, service fit,
geography, industry, regulatory cleanliness, succession signals).

Pass 1 (Website Score): Crawls firm websites and refines ADV scores with
website quality signals, succession language, technology mentions, and
team depth.

Examples:
  # Score all firms in default AUM range
  score --pass 0

  # Score Texas and Florida firms with custom AUM range
  score --pass 0 --min-aum 200000000 --max-aum 2000000000 --states TX,FL

  # Export top 100 to CSV
  score --pass 0 --limit 100 --format csv --output scores.csv

  # Run website scoring on Pass 0 survivors
  score --pass 1 --min-score 60

  # Score a single firm
  score --pass 0 --crd 12345`,
	RunE: runScore,
}

func init() {
	f := scoreCmd.Flags()
	f.Int("pass", 0, "scoring pass to run (0=ADV, 1=Website)")
	f.Int("crd", 0, "score a single firm by CRD number")
	f.Int64("min-aum", 0, "minimum AUM filter (overrides config)")
	f.Int64("max-aum", 0, "maximum AUM filter (overrides config)")
	f.String("states", "", "comma-separated state codes (e.g., TX,FL,CA)")
	f.String("acquirer-cbsas", "", "comma-separated CBSA codes for acquirer offices (e.g., 12420,19100)")
	f.String("target-cbsas", "", "comma-separated CBSA codes for target markets (e.g., 47900,35620)")
	f.String("geo-keywords", "", "comma-separated geography keywords (overrides config)")
	f.String("industry-keywords", "", "comma-separated industry keywords (overrides config)")
	f.Float64("min-score", 0, "minimum score threshold (overrides config)")
	f.Int("limit", 0, "maximum number of results (0=use config default)")
	f.String("output", "", "output file path (default: stdout)")
	f.String("format", "table", "output format: table or csv")
	f.Bool("save", false, "save results to fed_data.firm_scores")

	rootCmd.AddCommand(scoreCmd)
}

func runScore(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cfg.Validate("fedsync"); err != nil {
		return err
	}

	log := zap.L().With(zap.String("command", "score"))

	pool, err := fedsyncPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Run migrations to ensure firm_scores table exists.
	if err := fedsync.Migrate(ctx, pool); err != nil {
		return eris.Wrap(err, "score: migrate")
	}

	// Parse flags.
	pass, _ := cmd.Flags().GetInt("pass")
	crd, _ := cmd.Flags().GetInt("crd")
	outputPath, _ := cmd.Flags().GetString("output")
	format, _ := cmd.Flags().GetString("format")
	save, _ := cmd.Flags().GetBool("save")

	if pass < 0 || pass > 1 {
		return eris.Errorf("score: --pass must be 0 or 1 (got %d)", pass)
	}
	if format != "table" && format != "csv" {
		return eris.Errorf("score: --format must be table or csv (got %q)", format)
	}

	// Build scorer config from global config with CLI overrides.
	scorerCfg := applyScorerOverrides(cmd, cfg.Scorer)
	if err := scorer.ValidateConfig(scorerCfg); err != nil {
		return err
	}

	// Single-firm mode.
	if crd > 0 && pass == 0 {
		advScorer := scorer.NewADVScorer(pool, scorerCfg)
		result, err := advScorer.ScoreOne(ctx, crd)
		if err != nil {
			return eris.Wrapf(err, "score: CRD %d", crd)
		}
		printSingleScore(result)
		if save {
			if err := scorer.SaveScores(ctx, pool, []scorer.FirmScore{*result}, pass); err != nil {
				return eris.Wrap(err, "score: save")
			}
			fmt.Println("Score saved to fed_data.firm_scores")
		}
		return nil
	}

	// Bulk scoring.
	switch pass {
	case 0:
		advScorer := scorer.NewADVScorer(pool, scorerCfg)
		filters := &scorer.ScoreFilters{
			MinAUM:   scorerCfg.MinAUM,
			MaxAUM:   scorerCfg.MaxAUM,
			States:   scorerCfg.TargetStates,
			MinScore: scorerCfg.MinScore,
			Limit:    scorerCfg.MaxFirms,
		}

		log.Info("starting ADV scoring (Pass 0)",
			zap.Int64("min_aum", filters.MinAUM),
			zap.Int64("max_aum", filters.MaxAUM),
			zap.Strings("states", filters.States),
			zap.Float64("min_score", filters.MinScore),
			zap.Int("limit", filters.Limit),
		)

		results, err := advScorer.Score(ctx, filters)
		if err != nil {
			return eris.Wrap(err, "score: ADV scoring")
		}

		log.Info("ADV scoring complete", zap.Int("total", len(results)))

		if err := outputScoreResults(results, format, outputPath); err != nil {
			return err
		}
		if save && len(results) > 0 {
			if err := scorer.SaveScores(ctx, pool, results, pass); err != nil {
				return eris.Wrap(err, "score: save")
			}
			fmt.Printf("Saved %d scores to fed_data.firm_scores\n", len(results))
		}

		printScoreSummary(results)

	case 1:
		// Load Pass 0 results from firm_scores table.
		pass0Results, err := scorer.LoadPassResults(ctx, pool, 0, scorerCfg.MinScore)
		if err != nil {
			return eris.Wrap(err, "score: load Pass 0 results")
		}
		if len(pass0Results) == 0 {
			fmt.Println("No Pass 0 results found. Run 'score --pass 0 --save' first.")
			return nil
		}

		log.Info("starting website scoring (Pass 1)",
			zap.Int("candidates", len(pass0Results)),
		)

		webScorer := scorer.NewWebsiteScorer(nil, nil, scorerCfg)
		var webResults []scorer.FirmScore
		for i, firm := range pass0Results {
			ws, err := webScorer.Score(ctx, &firm)
			if err != nil {
				log.Warn("website scoring failed",
					zap.Int("crd_number", firm.CRDNumber),
					zap.Error(err),
				)
				continue
			}
			webResults = append(webResults, ws.FirmScore)
			if (i+1)%50 == 0 {
				log.Info("website scoring progress",
					zap.Int("completed", i+1),
					zap.Int("total", len(pass0Results)),
				)
			}
		}

		log.Info("website scoring complete", zap.Int("scored", len(webResults)))

		if err := outputScoreResults(webResults, format, outputPath); err != nil {
			return err
		}
		if save && len(webResults) > 0 {
			if err := scorer.SaveScores(ctx, pool, webResults, pass); err != nil {
				return eris.Wrap(err, "score: save")
			}
			fmt.Printf("Saved %d scores to fed_data.firm_scores\n", len(webResults))
		}

		printScoreSummary(webResults)
	}

	return nil
}

// applyScorerOverrides returns a copy of the base config with CLI flag overrides applied.
func applyScorerOverrides(cmd *cobra.Command, base config.ScorerConfig) config.ScorerConfig {
	c := base

	if v, _ := cmd.Flags().GetInt64("min-aum"); v > 0 {
		c.MinAUM = v
	}
	if v, _ := cmd.Flags().GetInt64("max-aum"); v > 0 {
		c.MaxAUM = v
	}
	if v, _ := cmd.Flags().GetString("states"); v != "" {
		c.TargetStates = splitAndTrim(v)
	}
	if v, _ := cmd.Flags().GetString("acquirer-cbsas"); v != "" {
		c.AcquirerCBSAs = splitAndTrim(v)
	}
	if v, _ := cmd.Flags().GetString("target-cbsas"); v != "" {
		c.TargetCBSAs = splitAndTrim(v)
	}
	if v, _ := cmd.Flags().GetString("geo-keywords"); v != "" {
		c.GeoKeywords = splitAndTrim(v)
	}
	if v, _ := cmd.Flags().GetString("industry-keywords"); v != "" {
		c.IndustryKeywords = splitAndTrim(v)
	}
	if v, _ := cmd.Flags().GetFloat64("min-score"); v > 0 {
		c.MinScore = v
	}
	if v, _ := cmd.Flags().GetInt("limit"); v > 0 {
		c.MaxFirms = v
	}

	return c
}

func printSingleScore(s *scorer.FirmScore) {
	fmt.Printf("CRD:    %d\n", s.CRDNumber)
	fmt.Printf("Firm:   %s\n", s.FirmName)
	fmt.Printf("State:  %s\n", s.State)
	fmt.Printf("AUM:    $%s\n", formatMoney(s.AUM))
	fmt.Printf("Score:  %.1f / 100\n", s.Score)
	fmt.Printf("Passed: %v\n", s.Passed)
	if len(s.ComponentScores) > 0 {
		fmt.Println("\nComponents:")
		for k, v := range s.ComponentScores {
			fmt.Printf("  %-25s %.2f\n", k, v)
		}
	}
	if len(s.MatchedKeywords) > 0 {
		fmt.Println("\nMatched Keywords:")
		for k, v := range s.MatchedKeywords {
			fmt.Printf("  %-15s %s\n", k, strings.Join(v, ", "))
		}
	}
}

func printScoreSummary(results []scorer.FirmScore) {
	if len(results) == 0 {
		fmt.Println("No results.")
		return
	}
	var passed, total int
	var sumScore float64
	var maxScore, minScore float64
	minScore = 101
	for _, r := range results {
		total++
		sumScore += r.Score
		if r.Score > maxScore {
			maxScore = r.Score
		}
		if r.Score < minScore {
			minScore = r.Score
		}
		if r.Passed {
			passed++
		}
	}
	fmt.Printf("\n--- Summary ---\n")
	fmt.Printf("Total scored:  %d\n", total)
	fmt.Printf("Passed:        %d (%.1f%%)\n", passed, float64(passed)/float64(total)*100)
	fmt.Printf("Score range:   %.1f – %.1f\n", minScore, maxScore)
	fmt.Printf("Average score: %.1f\n", sumScore/float64(total))
}

func outputScoreResults(results []scorer.FirmScore, format, outputPath string) error {
	var w *os.File
	if outputPath != "" {
		var err error
		w, err = os.Create(outputPath)
		if err != nil {
			return eris.Wrapf(err, "score: create output file %s", outputPath)
		}
		defer w.Close() //nolint:errcheck
	} else {
		w = os.Stdout
	}

	switch format {
	case "csv":
		return writeScoreCSV(w, results)
	case "table":
		return writeScoreTable(w, results)
	default:
		return eris.Errorf("score: unsupported format %q", format)
	}
}

func writeScoreCSV(w *os.File, results []scorer.FirmScore) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	header := []string{"crd_number", "firm_name", "state", "aum", "score", "passed", "website"}
	if err := cw.Write(header); err != nil {
		return eris.Wrap(err, "score: write CSV header")
	}

	for _, r := range results {
		row := []string{
			fmt.Sprintf("%d", r.CRDNumber),
			r.FirmName,
			r.State,
			fmt.Sprintf("%d", r.AUM),
			fmt.Sprintf("%.1f", r.Score),
			fmt.Sprintf("%v", r.Passed),
			r.Website,
		}
		if err := cw.Write(row); err != nil {
			return eris.Wrap(err, "score: write CSV row")
		}
	}
	return nil
}

func writeScoreTable(w *os.File, results []scorer.FirmScore) error {
	header := fmt.Sprintf("%-8s %-50s %-5s %15s %7s %6s\n",
		"CRD", "Firm Name", "State", "AUM", "Score", "Pass")
	if _, err := fmt.Fprint(w, header); err != nil {
		return eris.Wrap(err, "score: write table header")
	}
	if _, err := fmt.Fprintln(w, strings.Repeat("-", 95)); err != nil {
		return eris.Wrap(err, "score: write table separator")
	}

	for _, r := range results {
		name := r.FirmName
		if len(name) > 50 {
			name = name[:47] + "..."
		}
		line := fmt.Sprintf("%-8d %-50s %-5s %15s %7.1f %6v\n",
			r.CRDNumber, name, r.State, formatMoney(r.AUM), r.Score, r.Passed)
		if _, err := fmt.Fprint(w, line); err != nil {
			return eris.Wrap(err, "score: write table row")
		}
	}
	return nil
}

func formatMoney(amount int64) string {
	if amount == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", amount)
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
