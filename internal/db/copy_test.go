package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyFrom_EmptyRows(t *testing.T) {
	// CopyFrom with empty rows should return 0 without touching the pool.
	n, err := CopyFrom(nil, nil, "test_table", []string{"a", "b"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestCopyFromSchema_EmptyRows(t *testing.T) {
	n, err := CopyFromSchema(nil, nil, "fed_data", "test_table", []string{"a"}, [][]any{})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}
