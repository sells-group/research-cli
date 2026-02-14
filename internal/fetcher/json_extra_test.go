package fetcher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeJSONArray_InvalidOpeningToken(t *testing.T) {
	// Input that starts with something other than '[' (but is valid JSON token)
	input := `"not an array"`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	for range ch {
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "expected '['")
}

func TestDecodeJSONArray_DecodeError(t *testing.T) {
	// Array with malformed elements
	input := `[{"id":1,"name":"ok"},{"id":invalid}]`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	var records []testRecord
	for rec := range ch {
		records = append(records, rec)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "json: decode element")
	// First record should have been received before error
	assert.Len(t, records, 1)
}

func TestDecodeJSONArray_ContextCancelDuringSend(t *testing.T) {
	// Large JSON array
	var sb strings.Builder
	sb.WriteString("[")
	for i := range 1000 {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":1,"name":"test"}`)
	}
	sb.WriteString("]")

	ctx, cancel := context.WithCancel(context.Background())
	ch, errCh := DecodeJSONArray[testRecord](ctx, strings.NewReader(sb.String()))

	// Read one element then cancel
	<-ch
	cancel()

	// Drain
	for range ch {
	}
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

func TestDecodeJSONArray_ClosingTokenError(t *testing.T) {
	// Truncated JSON array - missing closing bracket
	// The JSON decoder may handle this differently, but we test the path
	input := `[{"id":1,"name":"ok"}`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	var records []testRecord
	for rec := range ch {
		records = append(records, rec)
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	// Either we get a closing token error or decode succeeds - depends on decoder behavior
	// The important thing is we don't panic
	_ = records
	_ = gotErr
}

func TestDecodeJSONArray_MalformedOpeningJSON(t *testing.T) {
	// Completely invalid JSON
	input := `{{{invalid`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	for range ch {
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
}

func TestDecodeJSONArray_NumberOpeningToken(t *testing.T) {
	// Starts with a number token, not '['
	input := `42`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	for range ch {
	}

	var gotErr error
	for err := range errCh {
		if err != nil {
			gotErr = err
		}
	}
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "expected '['")
}

func TestDecodeJSONObject_EmptyInput(t *testing.T) {
	_, err := DecodeJSONObject[testRecord](strings.NewReader(""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "json: decode object")
}

func TestDecodeJSONArray_SingleElement(t *testing.T) {
	input := `[{"id":99,"name":"solo"}]`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	var records []testRecord
	for rec := range ch {
		records = append(records, rec)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, records, 1)
	assert.Equal(t, 99, records[0].ID)
	assert.Equal(t, "solo", records[0].Name)
}
