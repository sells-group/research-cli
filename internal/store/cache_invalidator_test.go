package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
)

func TestWithAPICache_InvalidatesRunsDomainOnCreateRun(t *testing.T) {
	base := &storemocks.MockStore{}
	base.EXPECT().CreateRun(context.Background(), model.Company{URL: "https://acme.com"}).Return(&model.Run{ID: "run-1"}, nil).Once()
	base.EXPECT().Close().Return(nil).Once()

	cache := apicache.NewMemory()
	require.NoError(t, cache.Set(apicache.KeyQueueStatus, map[string]int{"queued": 1}, time.Minute))

	wrapped := store.WithAPICache(base, cache)
	_, err := wrapped.CreateRun(context.Background(), model.Company{URL: "https://acme.com"})
	require.NoError(t, err)

	_, ok := cache.Get(apicache.KeyQueueStatus)
	assert.False(t, ok)
	require.NoError(t, wrapped.Close())
}
