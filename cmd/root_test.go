package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootCommand_HasSubcommands(t *testing.T) {
	cmds := rootCmd.Commands()

	// Collect subcommand names.
	names := make(map[string]bool)
	for _, c := range cmds {
		names[c.Name()] = true
	}

	// Verify expected subcommands are registered.
	expected := []string{"run", "batch", "serve", "fedsync", "import"}
	for _, name := range expected {
		assert.True(t, names[name], "expected subcommand %q not found", name)
	}
}

func TestRootCommand_Metadata(t *testing.T) {
	assert.Equal(t, "research-cli", rootCmd.Use)
	assert.NotEmpty(t, rootCmd.Short)
	assert.NotEmpty(t, rootCmd.Long)
}

func TestRunCommand_RequiredFlags(t *testing.T) {
	flag := runCmd.Flags().Lookup("url")
	require.NotNil(t, flag, "run command should have --url flag")

	sfFlag := runCmd.Flags().Lookup("sf-id")
	require.NotNil(t, sfFlag, "run command should have --sf-id flag")
}

func TestBatchCommand_Flags(t *testing.T) {
	flag := batchCmd.Flags().Lookup("limit")
	require.NotNil(t, flag, "batch command should have --limit flag")
	assert.Equal(t, "100", flag.DefValue)
}

func TestServeCommand_Flags(t *testing.T) {
	flag := serveCmd.Flags().Lookup("port")
	require.NotNil(t, flag, "serve command should have --port flag")
	assert.Equal(t, "0", flag.DefValue)
}

func TestFedsyncCommand_HasSubcommands(t *testing.T) {
	cmds := fedsyncCmd.Commands()
	names := make(map[string]bool)
	for _, c := range cmds {
		names[c.Name()] = true
	}

	expected := []string{"sync", "status", "migrate", "xref"}
	for _, name := range expected {
		assert.True(t, names[name], "fedsync should have subcommand %q", name)
	}
}

func TestFedsyncSyncCommand_Flags(t *testing.T) {
	for _, flagName := range []string{"phase", "datasets", "force", "full"} {
		flag := fedsyncSyncCmd.Flags().Lookup(flagName)
		assert.NotNil(t, flag, "fedsync sync should have --%s flag", flagName)
	}
}
