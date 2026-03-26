package dataset

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func readTestFixture(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func readTestdataString(t *testing.T, name string) string {
	t.Helper()
	return string(readTestFixture(t, filepath.Join("testdata", name)))
}

func writeTestFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

func copyTestFixture(t *testing.T, srcPath, destPath string) {
	t.Helper()
	writeTestFixture(t, destPath, readTestFixture(t, srcPath))
}
