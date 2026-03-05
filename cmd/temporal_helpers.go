package main

import "github.com/spf13/cobra"

// shouldUseTemporal returns true unless --direct is set or config disables Temporal.
func shouldUseTemporal(cmd *cobra.Command) bool {
	if direct, _ := cmd.Flags().GetBool("direct"); direct {
		return false
	}
	return cfg.Temporal.ShouldUseTemporal()
}

// addDirectFlag registers the --direct flag on cmd.
func addDirectFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("direct", false, "run directly without Temporal")
}
