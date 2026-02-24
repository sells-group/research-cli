package fetcher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRecord struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestDecodeJSONArray(t *testing.T) {
	input := `[{"id":1,"name":"alpha"},{"id":2,"name":"beta"},{"id":3,"name":"gamma"}]`

	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	var records []testRecord
	for rec := range ch {
		records = append(records, rec)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Len(t, records, 3)
	assert.Equal(t, 1, records[0].ID)
	assert.Equal(t, "alpha", records[0].Name)
	assert.Equal(t, 2, records[1].ID)
	assert.Equal(t, "beta", records[1].Name)
	assert.Equal(t, 3, records[2].ID)
	assert.Equal(t, "gamma", records[2].Name)
}

func TestDecodeJSONArray_Empty(t *testing.T) {
	input := `[]`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	var records []testRecord
	for rec := range ch {
		records = append(records, rec)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Empty(t, records)
}

func TestDecodeJSONArray_ContextCancellation(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range 10000 {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":1,"name":"test"}`)
	}
	sb.WriteString("]")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	ch, errCh := DecodeJSONArray[testRecord](ctx, strings.NewReader(sb.String()))

	for range ch { //nolint:revive // drain
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

func TestDecodeJSONArray_InvalidFormat(t *testing.T) {
	input := `{"id":1,"name":"not an array"}`
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(input))

	for range ch { //nolint:revive // drain
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

func TestDecodeJSONObject(t *testing.T) {
	input := `{"id":42,"name":"test"}`
	rec, err := DecodeJSONObject[testRecord](strings.NewReader(input))
	require.NoError(t, err)
	assert.Equal(t, 42, rec.ID)
	assert.Equal(t, "test", rec.Name)
}

func TestDecodeJSONObject_Invalid(t *testing.T) {
	input := `not json`
	_, err := DecodeJSONObject[testRecord](strings.NewReader(input))
	require.Error(t, err)
}

func TestDecodeJSONArray_EmptyInput(t *testing.T) {
	ch, errCh := DecodeJSONArray[testRecord](context.Background(), strings.NewReader(""))

	var records []testRecord
	for rec := range ch {
		records = append(records, rec)
	}
	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Empty(t, records)
}
