//go:build !integration

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

// newSyncFlagsCmd creates a fresh cobra.Command with the same flags as
// fedsyncSyncCmd, so tests don't share mutable flag state.
func newSyncFlagsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "test-sync"}
	cmd.Flags().String("phase", "", "")
	cmd.Flags().String("datasets", "", "")
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().Bool("full", false, "")
	return cmd
}

func TestParseSyncOpts_Defaults(t *testing.T) {
	cmd := newSyncFlagsCmd()

	opts, err := parseSyncOpts(cmd)
	require.NoError(t, err)
	assert.Nil(t, opts.Phase)
	assert.Nil(t, opts.Datasets)
	assert.False(t, opts.Force)
	assert.False(t, opts.Full)
}

func TestParseSyncOpts_WithPhase(t *testing.T) {
	tests := []struct {
		name     string
		phase    string
		expected dataset.Phase
	}{
		{"phase 1", "1", dataset.Phase1},
		{"phase 1B lowercase", "1b", dataset.Phase1B},
		{"phase 1B uppercase", "1B", dataset.Phase1B},
		{"phase 2", "2", dataset.Phase2},
		{"phase 3", "3", dataset.Phase3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newSyncFlagsCmd()
			require.NoError(t, cmd.Flags().Set("phase", tt.phase))

			opts, err := parseSyncOpts(cmd)
			require.NoError(t, err)
			require.NotNil(t, opts.Phase)
			assert.Equal(t, tt.expected, *opts.Phase)
		})
	}
}

func TestParseSyncOpts_InvalidPhase(t *testing.T) {
	cmd := newSyncFlagsCmd()
	require.NoError(t, cmd.Flags().Set("phase", "99"))

	_, err := parseSyncOpts(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown phase")
}

func TestParseSyncOpts_WithDatasets(t *testing.T) {
	cmd := newSyncFlagsCmd()
	require.NoError(t, cmd.Flags().Set("datasets", "cbp,fpds,oews"))

	opts, err := parseSyncOpts(cmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"cbp", "fpds", "oews"}, opts.Datasets)
}

func TestParseSyncOpts_WithDatasets_WhitespaceHandling(t *testing.T) {
	cmd := newSyncFlagsCmd()
	require.NoError(t, cmd.Flags().Set("datasets", " cbp , fpds , oews "))

	opts, err := parseSyncOpts(cmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"cbp", "fpds", "oews"}, opts.Datasets)
}

func TestParseSyncOpts_ForceFull(t *testing.T) {
	cmd := newSyncFlagsCmd()
	require.NoError(t, cmd.Flags().Set("force", "true"))
	require.NoError(t, cmd.Flags().Set("full", "true"))

	opts, err := parseSyncOpts(cmd)
	require.NoError(t, err)
	assert.True(t, opts.Force)
	assert.True(t, opts.Full)
}

func TestParseSyncOpts_AllFlags(t *testing.T) {
	cmd := newSyncFlagsCmd()
	require.NoError(t, cmd.Flags().Set("phase", "2"))
	require.NoError(t, cmd.Flags().Set("datasets", "cbp,fpds"))
	require.NoError(t, cmd.Flags().Set("force", "true"))
	require.NoError(t, cmd.Flags().Set("full", "true"))

	opts, err := parseSyncOpts(cmd)
	require.NoError(t, err)
	require.NotNil(t, opts.Phase)
	assert.Equal(t, dataset.Phase2, *opts.Phase)
	assert.Equal(t, []string{"cbp", "fpds"}, opts.Datasets)
	assert.True(t, opts.Force)
	assert.True(t, opts.Full)
}

func TestParseSyncOpts_SingleDataset(t *testing.T) {
	cmd := newSyncFlagsCmd()
	require.NoError(t, cmd.Flags().Set("datasets", "cbp"))

	opts, err := parseSyncOpts(cmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"cbp"}, opts.Datasets)
}
