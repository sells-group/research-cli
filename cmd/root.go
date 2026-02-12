package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
)

var cfg *config.Config

var rootCmd = &cobra.Command{
	Use:   "research-cli",
	Short: "Automated account enrichment pipeline",
	Long:  "Crawls company websites, classifies pages, extracts structured data via tiered Claude models, writes to Salesforce.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		c, err := config.Load()
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		cfg = c

		if err := config.InitLogger(cfg.Log); err != nil {
			return fmt.Errorf("init logger: %w", err)
		}

		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		_ = zap.L().Sync()
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
