package main

import "github.com/spf13/cobra"

var geoCmd = &cobra.Command{
	Use:   "geo",
	Short: "Geocoding and MSA association",
	Long:  "Geocode company addresses and associate with metropolitan statistical areas.",
}

func init() { rootCmd.AddCommand(geoCmd) }
