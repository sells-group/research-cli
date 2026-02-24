package main

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/pkg/notion"
)

var importCSVPath string

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import leads from CSV into Notion",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		if cfg.Notion.Token == "" {
			return eris.New("notion token is required (RESEARCH_NOTION_TOKEN)")
		}
		if cfg.Notion.LeadDB == "" {
			return eris.New("notion lead DB ID is required (RESEARCH_NOTION_LEAD_DB)")
		}

		notionClient := notion.NewClient(cfg.Notion.Token)

		created, err := notion.ImportCSV(ctx, notionClient, cfg.Notion.LeadDB, importCSVPath)
		if err != nil {
			return eris.Wrap(err, "import csv")
		}

		zap.L().Info("import complete",
			zap.Int("created", created),
			zap.String("csv", importCSVPath),
		)
		return nil
	},
}

func init() {
	importCmd.Flags().StringVar(&importCSVPath, "csv", "", "path to CSV file (required)")
	_ = importCmd.MarkFlagRequired("csv")
	rootCmd.AddCommand(importCmd)
}
