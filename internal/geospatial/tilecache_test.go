package geospatial

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTileCache_BasicGetPut(t *testing.T) {
	cache := NewTileCache(100, time.Hour)

	// Miss on empty cache.
	assert.Nil(t, cache.Get("poi", 10, 512, 256))

	// Put and get.
	data := []byte("mvt-tile-data")
	cache.Put("poi", 10, 512, 256, data)
	got := cache.Get("poi", 10, 512, 256)
	assert.Equal(t, data, got)

	// Different key is still a miss.
	assert.Nil(t, cache.Get("poi", 10, 512, 257))
}

func TestTileCache_TTLExpiration(t *testing.T) {
	cache := NewTileCache(100, 50*time.Millisecond)

	cache.Put("poi", 1, 0, 0, []byte("tile"))
	assert.NotNil(t, cache.Get("poi", 1, 0, 0))

	time.Sleep(60 * time.Millisecond)
	assert.Nil(t, cache.Get("poi", 1, 0, 0))

	// Expired entry should be removed from the map.
	cache.mu.RLock()
	_, exists := cache.entries[tileKey("poi", 1, 0, 0)]
	cache.mu.RUnlock()
	assert.False(t, exists)
}

func TestTileCache_LRUEviction(t *testing.T) {
	cache := NewTileCache(3, time.Hour)

	cache.Put("a", 0, 0, 0, []byte("1"))
	cache.Put("b", 0, 0, 0, []byte("2"))
	cache.Put("c", 0, 0, 0, []byte("3"))

	// Cache is full. Adding a fourth should evict "a" (oldest).
	cache.Put("d", 0, 0, 0, []byte("4"))

	assert.Nil(t, cache.Get("a", 0, 0, 0))
	assert.NotNil(t, cache.Get("b", 0, 0, 0))
	assert.NotNil(t, cache.Get("c", 0, 0, 0))
	assert.NotNil(t, cache.Get("d", 0, 0, 0))
}

func TestTileCache_LRUEviction_AccessOrder(t *testing.T) {
	cache := NewTileCache(3, time.Hour)

	cache.Put("a", 0, 0, 0, []byte("1"))
	cache.Put("b", 0, 0, 0, []byte("2"))
	cache.Put("c", 0, 0, 0, []byte("3"))

	// Access "a" to move it to back.
	cache.Get("a", 0, 0, 0)

	// Now "b" is the oldest. Adding "d" should evict "b".
	cache.Put("d", 0, 0, 0, []byte("4"))

	assert.NotNil(t, cache.Get("a", 0, 0, 0))
	assert.Nil(t, cache.Get("b", 0, 0, 0))
	assert.NotNil(t, cache.Get("c", 0, 0, 0))
	assert.NotNil(t, cache.Get("d", 0, 0, 0))
}

func TestTileCache_ConcurrentAccess(t *testing.T) {
	cache := NewTileCache(1000, time.Hour)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			cache.Put("layer", n, 0, 0, []byte("data"))
			cache.Get("layer", n, 0, 0)
		}(i)
	}
	wg.Wait()

	stats := cache.Stats()
	assert.LessOrEqual(t, stats.Entries, 1000)
	assert.True(t, stats.Hits+stats.Misses > 0)
}

func TestTileCache_Invalidate(t *testing.T) {
	cache := NewTileCache(100, time.Hour)

	cache.Put("poi", 1, 0, 0, []byte("a"))
	cache.Put("poi", 2, 0, 0, []byte("b"))
	cache.Put("counties", 1, 0, 0, []byte("c"))

	cache.Invalidate("poi")

	assert.Nil(t, cache.Get("poi", 1, 0, 0))
	assert.Nil(t, cache.Get("poi", 2, 0, 0))
	assert.NotNil(t, cache.Get("counties", 1, 0, 0))

	cache.mu.RLock()
	assert.Len(t, cache.entries, 1)
	cache.mu.RUnlock()
}

func TestTileCache_Stats(t *testing.T) {
	cache := NewTileCache(100, time.Hour)

	cache.Put("a", 0, 0, 0, []byte("1"))
	cache.Put("b", 0, 0, 0, []byte("2"))

	cache.Get("a", 0, 0, 0) // hit
	cache.Get("b", 0, 0, 0) // hit
	cache.Get("c", 0, 0, 0) // miss

	stats := cache.Stats()
	assert.Equal(t, 2, stats.Entries)
	assert.Equal(t, 100, stats.MaxEntries)
	assert.Equal(t, int64(2), stats.Hits)
	assert.Equal(t, int64(1), stats.Misses)
	require.InDelta(t, 0.6667, stats.HitRate, 0.01)
}

func TestTileCache_UpdateExistingKey(t *testing.T) {
	cache := NewTileCache(100, time.Hour)

	cache.Put("a", 0, 0, 0, []byte("old"))
	cache.Put("a", 0, 0, 0, []byte("new"))

	got := cache.Get("a", 0, 0, 0)
	assert.Equal(t, []byte("new"), got)

	// Should still only have one entry.
	cache.mu.RLock()
	assert.Len(t, cache.entries, 1)
	cache.mu.RUnlock()
}
