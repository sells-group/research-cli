package fetcher

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamCSV_ReadError(t *testing.T) {
	// Reader that returns an error after some data
	r := &failingReader{
		data:    "a,b,c\n1,2,3\n",
		failAt:  10,
		failErr: io.ErrUnexpectedEOF,
	}

	rowCh, errCh := StreamCSV(context.Background(), r, CSVOptions{})

	for range rowCh { //nolint:revive // drain
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	// We should get an error from the reader
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "csv: read row")
}

// failingReader returns an error after reading failAt bytes.
type failingReader struct {
	data    string
	pos     int
	failAt  int
	failErr error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAt {
		return 0, r.failErr
	}
	remaining := r.data[r.pos:]
	n := copy(p, remaining)
	if r.pos+n >= r.failAt {
		n = r.failAt - r.pos
		r.pos = r.failAt
		return n, nil
	}
	r.pos += n
	return n, nil
}

func TestStreamCSV_HeaderSendContextCancelled(t *testing.T) {
	input := "name,age\nalice,30\nbob,25\n"

	// Create a header channel that blocks (unbuffered) and cancel context before it can send
	headerCh := make(chan []string) // unbuffered - will block

	ctx, cancel := context.WithCancel(context.Background())

	rowCh, errCh := StreamCSV(ctx, strings.NewReader(input), CSVOptions{
		HasHeader: true,
		HeaderCh:  headerCh,
	})

	// Cancel context immediately, before reading from headerCh
	cancel()

	// Drain row channel
	for range rowCh { //nolint:revive // drain
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

func TestStreamCSV_HasHeaderNoHeaderCh(t *testing.T) {
	// HasHeader=true but no HeaderCh - header row should just be skipped
	input := "name,age\nalice,30\nbob,25\n"
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		HasHeader: true,
	})

	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, []string{"alice", "30"}, rows[0])
	assert.Equal(t, []string{"bob", "25"}, rows[1])
}

func TestStreamCSV_RowSendContextCancelled(t *testing.T) {
	// Create a large CSV input
	var sb strings.Builder
	for range 100 {
		sb.WriteString("a,b,c\n")
	}

	ctx, cancel := context.WithCancel(context.Background())
	rowCh, errCh := StreamCSV(ctx, strings.NewReader(sb.String()), CSVOptions{})

	// Read one row then cancel
	<-rowCh
	cancel()

	// Drain remaining
	for range rowCh { //nolint:revive // drain
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

func TestStreamCSV_TrimSpaceWithHeader(t *testing.T) {
	input := " Name , Age \n Alice , 30 \n"
	headerCh := make(chan []string, 1)
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{
		TrimSpace: true,
		HasHeader: true,
		HeaderCh:  headerCh,
	})

	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, []string{"Alice", "30"}, rows[0])

	header := <-headerCh
	assert.Equal(t, []string{"Name", "Age"}, header)
}

func TestStreamCSV_VariableFields(t *testing.T) {
	// CSV with variable number of fields per row
	input := "a,b,c\n1,2\n3,4,5,6\n"
	rowCh, errCh := StreamCSV(context.Background(), strings.NewReader(input), CSVOptions{})

	rows, err := collectRows(t, rowCh, errCh)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Len(t, rows[0], 3)
	assert.Len(t, rows[1], 2)
	assert.Len(t, rows[2], 4)
}
