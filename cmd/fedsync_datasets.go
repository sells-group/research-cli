package main

import (
	"github.com/spf13/cobra"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

var fedsyncDatasetsCmd = &cobra.Command{
	Use:   "datasets",
	Short: "List the live fedsync dataset inventory",
	RunE: func(cmd *cobra.Command, _ []string) error {
		format, _ := cmd.Flags().GetString("format")

		switch format {
		case "json":
			payload, err := dataset.RenderCatalogJSON(cfg)
			if err != nil {
				return err
			}
			printOutputf(cmd, "%s", payload)
		case "markdown":
			printOutputf(cmd, "%s\n", dataset.RenderMarkdownSummary(cfg))
		default:
			printOutputf(cmd, "%s", dataset.RenderTextSummary(cfg))
		}

		return nil
	},
}

func init() {
	fedsyncDatasetsCmd.Flags().String("format", "text", "output format: text, json, markdown")
	fedsyncCmd.AddCommand(fedsyncDatasetsCmd)
}
