package sdk

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchFlusher_AutoFlush(t *testing.T) {
	flushCount := 0
	var flushedItems int
	bf := NewBatchFlusher(3, func(items [][]any) error {
		flushCount++
		flushedItems += len(items)
		return nil
	})

	for i := range 5 {
		require.NoError(t, bf.Add([]any{i}))
	}
	// 5 items, batchSize=3 → 1 auto-flush at item 3
	require.Equal(t, 1, flushCount)
	require.Equal(t, 3, flushedItems)

	// Flush remaining 2
	require.NoError(t, bf.Flush())
	require.Equal(t, 2, flushCount)
	require.Equal(t, 5, flushedItems)
	require.Equal(t, 5, bf.Count())
}

func TestBatchFlusher_ExactBatch(t *testing.T) {
	flushCount := 0
	bf := NewBatchFlusher(2, func(_ [][]any) error {
		flushCount++
		return nil
	})

	require.NoError(t, bf.Add([]any{1}))
	require.NoError(t, bf.Add([]any{2}))
	require.Equal(t, 1, flushCount)

	// Flush with no pending items — should be a no-op.
	require.NoError(t, bf.Flush())
	require.Equal(t, 1, flushCount)
	require.Equal(t, 2, bf.Count())
}

func TestBatchFlusher_FlushError(t *testing.T) {
	bf := NewBatchFlusher(2, func(_ [][]any) error {
		return fmt.Errorf("upsert failed")
	})

	require.NoError(t, bf.Add([]any{1}))
	err := bf.Add([]any{2}) // triggers auto-flush
	require.Error(t, err)
	require.Contains(t, err.Error(), "upsert failed")
}

func TestBatchFlusher_DefaultBatchSize(t *testing.T) {
	bf := NewBatchFlusher(0, func(_ [][]any) error { return nil })
	require.Equal(t, 0, bf.Count())

	// Add 500 items — should trigger one flush (default batchSize=500).
	for i := range 500 {
		require.NoError(t, bf.Add([]any{i}))
	}
	require.Equal(t, 500, bf.Count())
}

func TestBatchFlusher_EmptyFlush(t *testing.T) {
	called := false
	bf := NewBatchFlusher(10, func(_ [][]any) error {
		called = true
		return nil
	})

	require.NoError(t, bf.Flush())
	require.False(t, called)
	require.Equal(t, 0, bf.Count())
}
