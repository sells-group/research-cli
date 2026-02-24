package fetcher

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tealeg/xlsx/v2"
)

func createTestXLSX(t *testing.T, sheets map[string][][]string) string {
	t.Helper()
	f := xlsx.NewFile()
	for name, rows := range sheets {
		sheet, err := f.AddSheet(name)
		require.NoError(t, err)
		for _, rowData := range rows {
			row := sheet.AddRow()
			for _, cellData := range rowData {
				cell := row.AddCell()
				cell.SetString(cellData)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "test.xlsx")
	err := f.Save(path)
	require.NoError(t, err)
	return path
}

func TestReadXLSX_Basic(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {
			{"Name", "Age", "City"},
			{"Alice", "30", "NYC"},
			{"Bob", "25", "LA"},
		},
	})

	rows, err := ReadXLSX(path, XLSXOptions{})
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, []string{"Name", "Age", "City"}, rows[0])
	assert.Equal(t, []string{"Alice", "30", "NYC"}, rows[1])
	assert.Equal(t, []string{"Bob", "25", "LA"}, rows[2])
}

func TestReadXLSX_SkipRows(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {
			{"Header1", "Header2"},
			{"a", "b"},
			{"c", "d"},
		},
	})

	rows, err := ReadXLSX(path, XLSXOptions{SkipRows: 1})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"a", "b"}, rows[0])
	assert.Equal(t, []string{"c", "d"}, rows[1])
}

func TestReadXLSX_SheetName(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"First":  {{"a", "b"}},
		"Second": {{"x", "y"}, {"1", "2"}},
	})

	rows, err := ReadXLSX(path, XLSXOptions{SheetName: "Second"})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"x", "y"}, rows[0])
	assert.Equal(t, []string{"1", "2"}, rows[1])
}

func TestReadXLSX_SheetNameNotFound(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {{"a"}},
	})

	_, err := ReadXLSX(path, XLSXOptions{SheetName: "Missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestReadXLSX_SheetIndexOutOfRange(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {{"a"}},
	})

	_, err := ReadXLSX(path, XLSXOptions{SheetIndex: 5})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestReadXLSX_WithHeaderCh(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {
			{"Name", "Value"},
			{"a", "1"},
		},
	})

	headerCh := make(chan []string, 1)
	rows, err := ReadXLSX(path, XLSXOptions{
		SkipRows: 1,
		HeaderCh: headerCh,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, []string{"a", "1"}, rows[0])

	header := <-headerCh
	assert.Equal(t, []string{"Name", "Value"}, header)
}

func TestStreamXLSX_Basic(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {
			{"a", "b"},
			{"1", "2"},
			{"3", "4"},
		},
	})

	rowCh, errCh := StreamXLSX(context.Background(), path, XLSXOptions{})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, rows, 3)
	assert.Equal(t, []string{"a", "b"}, rows[0])
	assert.Equal(t, []string{"1", "2"}, rows[1])
	assert.Equal(t, []string{"3", "4"}, rows[2])
}

func TestStreamXLSX_WithSkipAndHeader(t *testing.T) {
	path := createTestXLSX(t, map[string][][]string{
		"Sheet1": {
			{"Header", "Row"},
			{"data", "here"},
		},
	})

	headerCh := make(chan []string, 1)
	rowCh, errCh := StreamXLSX(context.Background(), path, XLSXOptions{
		SkipRows: 1,
		HeaderCh: headerCh,
	})

	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, rows, 1)
	assert.Equal(t, []string{"data", "here"}, rows[0])

	header := <-headerCh
	assert.Equal(t, []string{"Header", "Row"}, header)
}

func TestStreamXLSX_ContextCancellation(t *testing.T) {
	// Create a file with many rows
	sheetData := make([][]string, 1000)
	for i := range sheetData {
		sheetData[i] = []string{"a", "b", "c"}
	}
	path := createTestXLSX(t, map[string][][]string{"Sheet1": sheetData})

	ctx, cancel := context.WithCancel(context.Background())
	rowCh, errCh := StreamXLSX(ctx, path, XLSXOptions{})

	count := 0
	for range rowCh {
		count++
		if count >= 5 {
			cancel()
			break
		}
	}
	for range rowCh { //nolint:revive // drain
	}
	for range errCh { //nolint:revive // drain
	}
	cancel() // ensure cleanup
}
