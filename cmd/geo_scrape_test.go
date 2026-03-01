//go:build !integration

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

// newScrapeFlagsCmd creates a fresh cobra.Command with the same flags as
// geoScrapeCmd, so tests don't share mutable flag state.
func newScrapeFlagsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "test-scrape"}
	cmd.Flags().String("category", "", "")
	cmd.Flags().String("sources", "", "")
	cmd.Flags().String("states", "", "")
	cmd.Flags().Bool("force", false, "")
	return cmd
}

func TestParseScrapeOpts_Defaults(t *testing.T) {
	cmd := newScrapeFlagsCmd()

	opts, err := parseScrapeOpts(cmd)
	require.NoError(t, err)
	assert.Nil(t, opts.Category)
	assert.Nil(t, opts.Sources)
	assert.Nil(t, opts.States)
	assert.False(t, opts.Force)
}

func TestParseScrapeOpts_WithCategory(t *testing.T) {
	tests := []struct {
		name     string
		category string
		expected geoscraper.Category
	}{
		{"national", "national", geoscraper.National},
		{"state", "state", geoscraper.State},
		{"on_demand", "on_demand", geoscraper.OnDemand},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newScrapeFlagsCmd()
			require.NoError(t, cmd.Flags().Set("category", tt.category))

			opts, err := parseScrapeOpts(cmd)
			require.NoError(t, err)
			require.NotNil(t, opts.Category)
			assert.Equal(t, tt.expected, *opts.Category)
		})
	}
}

func TestParseScrapeOpts_InvalidCategory(t *testing.T) {
	cmd := newScrapeFlagsCmd()
	require.NoError(t, cmd.Flags().Set("category", "bogus"))

	_, err := parseScrapeOpts(cmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown category")
}

func TestParseScrapeOpts_WithSources(t *testing.T) {
	cmd := newScrapeFlagsCmd()
	require.NoError(t, cmd.Flags().Set("sources", "hifld,fema_flood,epa_echo"))

	opts, err := parseScrapeOpts(cmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"hifld", "fema_flood", "epa_echo"}, opts.Sources)
}

func TestParseScrapeOpts_WithSources_WhitespaceHandling(t *testing.T) {
	cmd := newScrapeFlagsCmd()
	require.NoError(t, cmd.Flags().Set("sources", " hifld , fema_flood "))

	opts, err := parseScrapeOpts(cmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"hifld", "fema_flood"}, opts.Sources)
}

func TestParseScrapeOpts_WithStates(t *testing.T) {
	cmd := newScrapeFlagsCmd()
	require.NoError(t, cmd.Flags().Set("states", "48,12,06"))

	opts, err := parseScrapeOpts(cmd)
	require.NoError(t, err)
	assert.Equal(t, []string{"48", "12", "06"}, opts.States)
}

func TestParseScrapeOpts_Force(t *testing.T) {
	cmd := newScrapeFlagsCmd()
	require.NoError(t, cmd.Flags().Set("force", "true"))

	opts, err := parseScrapeOpts(cmd)
	require.NoError(t, err)
	assert.True(t, opts.Force)
}

func TestParseScrapeOpts_AllFlags(t *testing.T) {
	cmd := newScrapeFlagsCmd()
	require.NoError(t, cmd.Flags().Set("category", "national"))
	require.NoError(t, cmd.Flags().Set("sources", "hifld,fema_flood"))
	require.NoError(t, cmd.Flags().Set("states", "48"))
	require.NoError(t, cmd.Flags().Set("force", "true"))

	opts, err := parseScrapeOpts(cmd)
	require.NoError(t, err)
	require.NotNil(t, opts.Category)
	assert.Equal(t, geoscraper.National, *opts.Category)
	assert.Equal(t, []string{"hifld", "fema_flood"}, opts.Sources)
	assert.Equal(t, []string{"48"}, opts.States)
	assert.True(t, opts.Force)
}
