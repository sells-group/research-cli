package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
)

func setupBatchIterator(t *testing.T, items []anthropic.BatchResultItem) *anthropicmocks.MockBatchResultIterator {
	iter := anthropicmocks.NewMockBatchResultIterator(t)
	for _, item := range items {
		iter.On("Next").Return(true).Once()
		iter.On("Item").Return(item).Once()
	}
	iter.On("Next").Return(false).Once()
	iter.On("Err").Return(nil)
	iter.On("Close").Return(nil)
	return iter
}
