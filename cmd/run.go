package main

import (
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
)

var (
	runURL  string
	runSFID string
)

// writeRunResult logs the enrichment result and writes it as indented JSON.
func writeRunResult(w io.Writer, company model.Company, result *model.EnrichmentResult) error {
	zap.L().Info("enrichment complete",
		zap.String("company", company.URL),
		zap.Float64("score", result.Score),
		zap.Int("fields_found", len(result.Answers)),
		zap.Int("total_tokens", result.TotalTokens),
	)

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run enrichment for a single company",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		env, err := initPipeline(ctx)
		if err != nil {
			return err
		}
		defer env.Close()

		company := model.Company{
			URL:          runURL,
			SalesforceID: runSFID,
		}

		result, err := env.Pipeline.Run(ctx, company)
		if err != nil {
			return eris.Wrap(err, "pipeline run")
		}

		return writeRunResult(os.Stdout, company, result)
	},
}

func init() {
	runCmd.Flags().StringVar(&runURL, "url", "", "company website URL (required)")
	runCmd.Flags().StringVar(&runSFID, "sf-id", "", "Salesforce account ID")
	_ = runCmd.MarkFlagRequired("url")
	rootCmd.AddCommand(runCmd)
}
