package apicache

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryCache_SetGetDeleteDomain(t *testing.T) {
	cache := NewMemory()
	defer cache.Close() //nolint:errcheck

	require.NoError(t, cache.Set(KeyQueueStatus, map[string]int{"queued": 1}, time.Minute))
	require.NoError(t, cache.Set(KeyDataTables, map[string]any{"tables": []string{"cbp"}}, time.Minute))

	payload, ok := cache.Get(KeyQueueStatus)
	require.True(t, ok)
	assert.Contains(t, string(payload), `"queued":1`)

	require.NoError(t, cache.DeleteDomains(DomainRuns))

	_, ok = cache.Get(KeyQueueStatus)
	assert.False(t, ok)
	_, ok = cache.Get(KeyDataTables)
	assert.True(t, ok)
}

func TestRedisCache_SetGetDeleteDomain(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	cache, err := NewRedis(RedisConfig{
		URL:       "redis://" + server.Addr(),
		KeyPrefix: "research-cli:test",
	})
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	require.NoError(t, cache.Set(KeyFedsyncStatuses, map[string]any{"datasets": []string{"cbp"}}, time.Minute))

	payload, ok := cache.Get(KeyFedsyncStatuses)
	require.True(t, ok)
	assert.Contains(t, string(payload), `"datasets":["cbp"]`)

	require.NoError(t, cache.DeleteDomains(DomainFedsync))
	_, ok = cache.Get(KeyFedsyncStatuses)
	assert.False(t, ok)
}
