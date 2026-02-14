package fetcher

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadXLSX_FileNotFound(t *testing.T) {
	_, err := ReadXLSX("/nonexistent/path/file.xlsx", XLSXOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "xlsx: open file")
}

func TestReadXLSX_InvalidFile(t *testing.T) {
	// Create a non-XLSX file
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.xlsx")
	require.NoError(t, writeTestFile(path, "this is not an xlsx file"))

	_, err := ReadXLSX(path, XLSXOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "xlsx: open file")
}

func TestStreamXLSX_FileNotFound(t *testing.T) {
	rowCh, errCh := StreamXLSX(context.Background(), "/nonexistent/path/file.xlsx", XLSXOptions{})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "xlsx: open file")
	assert.Empty(t, rows)
}

func TestStreamXLSX_InvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.xlsx")
	require.NoError(t, writeTestFile(path, "this is not an xlsx file"))

	rowCh, errCh := StreamXLSX(context.Background(), path, XLSXOptions{})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "xlsx: open file")
}

func TestStreamXLSX_SheetNotFound(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {{"a", "b"}},
	})

	rowCh, errCh := StreamXLSX(context.Background(), path, XLSXOptions{SheetName: "Missing"})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "not found")
}

func TestStreamXLSX_SheetIndexOutOfRange(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {{"a", "b"}},
	})

	rowCh, errCh := StreamXLSX(context.Background(), path, XLSXOptions{SheetIndex: 10})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "out of range")
}

func TestStreamXLSX_HeaderSendContextCancelled(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {
			{"Header1", "Header2"},
			{"data1", "data2"},
			{"data3", "data4"},
		},
	})

	// Unbuffered header channel that will block
	headerCh := make(chan []string)

	ctx, cancel := context.WithCancel(context.Background())

	rowCh, errCh := StreamXLSX(ctx, path, XLSXOptions{
		HeaderCh: headerCh,
	})

	// Cancel immediately before reading from headerCh
	cancel()

	// Drain
	for range rowCh {
	}
	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr != nil {
		assert.Contains(t, gotErr.Error(), "context cancelled")
	}
}

func TestStreamXLSX_RowSendContextCancelled(t *testing.T) {
	// Create many rows to increase chance of context cancellation during send
	sheetData := make([][]string, 200)
	for i := range sheetData {
		sheetData[i] = []string{"a", "b", "c"}
	}
	path := createTestXLSX(t, map[string][][]string{"Sheet1": sheetData})

	ctx, cancel := context.WithCancel(context.Background())
	rowCh, errCh := StreamXLSX(ctx, path, XLSXOptions{})

	// Read one row then cancel
	<-rowCh
	cancel()

	// Drain
	for range rowCh {
	}
	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr != nil {
		assert.Contains(t, gotErr.Error(), "context cancelled")
	}
}

func TestReadXLSX_EmptySheet(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {},
	})

	rows, err := ReadXLSX(path, XLSXOptions{})
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestStreamXLSX_EmptySheet(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {},
	})

	rowCh, errCh := StreamXLSX(context.Background(), path, XLSXOptions{})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Empty(t, rows)
}
