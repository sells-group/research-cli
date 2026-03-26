package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

func commandOutputWriter(cmd *cobra.Command) io.Writer {
	if cmd != nil {
		return cmd.OutOrStdout()
	}
	return rootCmd.OutOrStdout()
}

func printOutputf(cmd *cobra.Command, format string, args ...any) {
	_, _ = fmt.Fprintf(commandOutputWriter(cmd), format, args...)
}

func printOutputln(cmd *cobra.Command, args ...any) {
	_, _ = fmt.Fprintln(commandOutputWriter(cmd), args...)
}
