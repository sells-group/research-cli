//go:build integration

package tiger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDBF_RealTIGER(t *testing.T) {
	// Uses a real TIGER ADDR file downloaded to /tmp.
	const path = "/tmp/test_addr_wy/tl_2024_56045_addr.dbf"

	product, ok := ProductByName("ADDR")
	require.True(t, ok)

	rows, err := ParseDBF(path, product)
	require.NoError(t, err)
	assert.True(t, len(rows) > 0, "expected rows from real DBF file")

	t.Logf("Parsed %d rows", len(rows))
	if len(rows) > 0 {
		t.Logf("First row: %v", rows[0])
	}
}
