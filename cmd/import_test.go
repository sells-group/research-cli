//go:build !integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
)

func TestImportCmd_Metadata(t *testing.T) {
	assert.Equal(t, "import", importCmd.Use)
	assert.NotEmpty(t, importCmd.Short)

	csvFlag := importCmd.Flags().Lookup("csv")
	require.NotNil(t, csvFlag)
}

func TestImportCmd_MissingNotionToken(t *testing.T) {
	cfg = &config.Config{
		Notion: config.NotionConfig{
			Token:  "",
			LeadDB: "some-db-id",
		},
	}

	err := importCmd.RunE(importCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "notion token is required")
}

func TestImportCmd_MissingLeadDB(t *testing.T) {
	cfg = &config.Config{
		Notion: config.NotionConfig{
			Token:  "fake-token",
			LeadDB: "",
		},
	}

	err := importCmd.RunE(importCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "notion lead DB ID is required")
}

func TestImportCmd_BadCSVPath(t *testing.T) {
	cfg = &config.Config{
		Notion: config.NotionConfig{
			Token:  "fake-token",
			LeadDB: "fake-lead-db",
		},
	}

	importCmd.SetContext(context.Background())
	defer importCmd.SetContext(context.TODO())

	// Set importCSVPath to a nonexistent file.
	oldCSV := importCSVPath
	importCSVPath = "/nonexistent/path/to/leads.csv"
	defer func() { importCSVPath = oldCSV }()

	err := importCmd.RunE(importCmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "import csv")
}
