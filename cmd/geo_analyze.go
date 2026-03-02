package main

import "github.com/spf13/cobra"

var geoAnalyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Run geospatial analysis pipeline",
	Long:  "Batch analysis of geospatial data: proximity matrix, parcel scoring, correlation, ranking.",
}

func init() { geoCmd.AddCommand(geoAnalyzeCmd) }
