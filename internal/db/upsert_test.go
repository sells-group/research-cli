package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkUpsert_EmptyRows(t *testing.T) {
	n, err := BulkUpsert(nil, nil, UpsertConfig{
		Table:        "fed_data.test",
		Columns:      []string{"id", "name"},
		ConflictKeys: []string{"id"},
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestBulkUpsert_NoColumns(t *testing.T) {
	_, err := BulkUpsert(nil, nil, UpsertConfig{
		Table:        "fed_data.test",
		ConflictKeys: []string{"id"},
	}, [][]any{{1, "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no columns specified")
}

func TestBulkUpsert_NoConflictKeys(t *testing.T) {
	_, err := BulkUpsert(nil, nil, UpsertConfig{
		Table:   "fed_data.test",
		Columns: []string{"id", "name"},
	}, [][]any{{1, "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no conflict keys specified")
}

func TestSanitizeTable(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", `"simple"`},
		{"fed_data.cbp_data", `"fed_data"."cbp_data"`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeTable(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestQuoteAndJoin(t *testing.T) {
	result := quoteAndJoin([]string{"id", "name", "value"})
	assert.Equal(t, `"id", "name", "value"`, result)
}
