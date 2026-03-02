package main

import (
	"github.com/spf13/cobra"
)

var discoverCmd = &cobra.Command{
	Use:   "discover",
	Short: "Lead discovery via PPP + Google Places",
	Long:  "Discover new leads using PPP loan records and Google Places API organic search.",
}

func init() {
	rootCmd.AddCommand(discoverCmd)
}
