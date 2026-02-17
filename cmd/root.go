package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

		// Apply CLI model overrides after config load.
		if v, _ := cmd.Flags().GetString("haiku-model"); v != "" {
			cfg.Anthropic.HaikuModel = v
		}
		if v, _ := cmd.Flags().GetString("sonnet-model"); v != "" {
			cfg.Anthropic.SonnetModel = v
		}
		if v, _ := cmd.Flags().GetString("opus-model"); v != "" {
			cfg.Anthropic.OpusModel = v
		}

		// --with-t3 overrides tier3_gate to "always".
		if withT3, _ := cmd.Flags().GetBool("with-t3"); withT3 {
			cfg.Pipeline.Tier3Gate = "always"
		}

		if err := config.InitLogger(cfg.Log); err != nil {
			return fmt.Errorf("init logger: %w", err)
		}

		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		_ = zap.L().Sync()
	},
}

func init() {
	rootCmd.PersistentFlags().Bool("no-batch", false, "use Messages API instead of Batch API for all Anthropic calls")
	_ = viper.BindPFlag("anthropic.no_batch", rootCmd.PersistentFlags().Lookup("no-batch"))

	rootCmd.PersistentFlags().Bool("with-t3", false, "enable Tier 3 (Opus) extraction (expensive, disabled by default)")

	rootCmd.PersistentFlags().String("haiku-model", "", "override Haiku model name (e.g. claude-haiku-4-5-20251001)")
	rootCmd.PersistentFlags().String("sonnet-model", "", "override Sonnet model name (e.g. claude-sonnet-4-5-20250929)")
	rootCmd.PersistentFlags().String("opus-model", "", "override Opus model name (e.g. claude-opus-4-6)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
