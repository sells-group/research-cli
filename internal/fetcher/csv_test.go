package fetcher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collectRows(t *testing.T, rowCh <-chan []string, errCh <-chan error) ([][]string, error) {
	t.Helper()
	var rows [][]string
	for row := range rowCh {
		rows = append(rows, row)
	}
	// Drain error channel
	for err := range errCh {
		if err != nil {
			return rows, err
		}
	}
	return rows, nil
}

func TestStreamCSV_Basic(t *testing.T) {
	input := "a,b,c\n1,2,3\n4,5,6\n"
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{})
	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, []string{"a", "b", "c"}, rows[0])
	assert.Equal(t, []string{"1", "2", "3"}, rows[1])
	assert.Equal(t, []string{"4", "5", "6"}, rows[2])
}

func TestStreamCSV_PipeDelimited(t *testing.T) {
	input := "a|b|c\n1|2|3\n"
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		Delimiter: '|',
	})
	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"a", "b", "c"}, rows[0])
	assert.Equal(t, []string{"1", "2", "3"}, rows[1])
}

func TestStreamCSV_WithHeader(t *testing.T) {
	input := "name,age\nalice,30\nbob,25\n"
	headerCh := make(chan []string, 1)

	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		HasHeader: true,
		HeaderCh:  headerCh,
	})

	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)

	// Data rows should not include header
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"alice", "30"}, rows[0])
	assert.Equal(t, []string{"bob", "25"}, rows[1])

	// Header should be received
	header := <-headerCh
	assert.Equal(t, []string{"name", "age"}, header)
}

func TestStreamCSV_ContextCancellation(t *testing.T) {
	// Large input that takes time to process
	var sb strings.Builder
	for range 10000 {
		sb.WriteString("a,b,c\n")
	}

	ctx, cancel := context.WithCancel(context.Background())
	rowCh, errCh := StreamCSV(ctx, strings.NewReader(sb.String()), CSVOptions{})

	// Read a few rows then cancel
	count := 0
	for range rowCh {
		count++
		if count >= 5 {
			cancel()
			break
		}
	}

	// Drain remaining
	for range rowCh {
	}

	// Check that we got a cancellation error or channel closed cleanly
	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	// Either we get a context cancelled error or the goroutine finished before noticing
	if gotErr != nil {
		assert.Contains(t, gotErr.Error(), "context cancelled")
	}
	cancel() // ensure cancel is called even if we didn't enter the if above
}

func TestStreamCSV_Empty(t *testing.T) {
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(""), CSVOptions{})
	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestStreamCSV_LazyQuotes(t *testing.T) {
	// Malformed CSV with quotes in unquoted field
	input := `a,b,c
1,"hello "world",3
`
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		LazyQuotes: true,
	})
	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"a", "b", "c"}, rows[0])
}

func TestStreamCSV_TrimSpace(t *testing.T) {
	input := " a , b , c \n 1 , 2 , 3 \n"
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		TrimSpace: true,
	})
	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"a", "b", "c"}, rows[0])
	assert.Equal(t, []string{"1", "2", "3"}, rows[1])
}

func TestStreamCSV_Comment(t *testing.T) {
	input := "# this is a comment\na,b\n1,2\n# another comment\n3,4\n"
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		Comment: '#',
	})
	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, []string{"a", "b"}, rows[0])
	assert.Equal(t, []string{"1", "2"}, rows[1])
	assert.Equal(t, []string{"3", "4"}, rows[2])
}

func TestStreamCSV_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	input := "a,b,c\n1,2,3\n"
	rowCh, errCh := StreamCSV(ctx, strings.NewReader(input), CSVOptions{})

	for range rowCh {
	}
	// May get 0 rows due to cancellation
	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	if gotErr != nil {
		assert.Contains(t, gotErr.Error(), "context")
	}
}
