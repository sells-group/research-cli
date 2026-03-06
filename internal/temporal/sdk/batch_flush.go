package sdk

// BatchFlusher accumulates items and auto-flushes when the batch threshold is reached.
// Designed for workflow contexts where items are collected from parallel activities
// and periodically flushed via upsert activities.
type BatchFlusher struct {
	batchSize int
	items     [][]any
	flushFn   func(items [][]any) error
	flushed   int
}

// NewBatchFlusher creates a BatchFlusher that calls flushFn when batchSize items accumulate.
func NewBatchFlusher(batchSize int, flushFn func(items [][]any) error) *BatchFlusher {
	if batchSize <= 0 {
		batchSize = 500
	}
	return &BatchFlusher{
		batchSize: batchSize,
		items:     make([][]any, 0, batchSize),
		flushFn:   flushFn,
	}
}

// Add appends a row and auto-flushes if the batch threshold is reached.
func (b *BatchFlusher) Add(row []any) error {
	b.items = append(b.items, row)
	if len(b.items) >= b.batchSize {
		return b.doFlush()
	}
	return nil
}

// Flush writes any remaining items. Should be called after all items are added.
func (b *BatchFlusher) Flush() error {
	if len(b.items) > 0 {
		return b.doFlush()
	}
	return nil
}

// Count returns the total number of items flushed plus pending items.
func (b *BatchFlusher) Count() int {
	return b.flushed + len(b.items)
}

func (b *BatchFlusher) doFlush() error {
	if err := b.flushFn(b.items); err != nil {
		return err
	}
	b.flushed += len(b.items)
	b.items = b.items[:0]
	return nil
}
