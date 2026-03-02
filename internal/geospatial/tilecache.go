package geospatial

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TileCache is a concurrent-safe LRU cache for MVT tiles with TTL expiration.
type TileCache struct {
	mu         sync.RWMutex
	entries    map[string]*tileCacheEntry
	order      []string // LRU order: front=oldest, back=newest
	maxEntries int
	ttl        time.Duration
	hits       atomic.Int64
	misses     atomic.Int64
}

type tileCacheEntry struct {
	data      []byte
	createdAt time.Time
}

// CacheStats contains cache performance statistics.
type CacheStats struct {
	Entries    int     `json:"entries"`
	MaxEntries int     `json:"max_entries"`
	Hits       int64   `json:"hits"`
	Misses     int64   `json:"misses"`
	HitRate    float64 `json:"hit_rate"`
}

// NewTileCache creates a new TileCache with the given capacity and TTL.
func NewTileCache(maxEntries int, ttl time.Duration) *TileCache {
	return &TileCache{
		entries:    make(map[string]*tileCacheEntry),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

// tileKey builds the cache key for a tile.
func tileKey(layer string, z, x, y int) string {
	return fmt.Sprintf("%s/%d/%d/%d", layer, z, x, y)
}

// Get retrieves a cached tile. Returns nil on miss or expiration.
func (c *TileCache) Get(layer string, z, x, y int) []byte {
	key := tileKey(layer, z, x, y)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		c.misses.Add(1)
		return nil
	}

	// Check TTL.
	if time.Since(entry.createdAt) > c.ttl {
		delete(c.entries, key)
		c.removeFromOrder(key)
		c.misses.Add(1)
		return nil
	}

	// Move to back (most recently used).
	c.removeFromOrder(key)
	c.order = append(c.order, key)
	c.hits.Add(1)
	return entry.data
}

// Put stores a tile in the cache, evicting the oldest entry if at capacity.
func (c *TileCache) Put(layer string, z, x, y int, data []byte) {
	key := tileKey(layer, z, x, y)

	c.mu.Lock()
	defer c.mu.Unlock()

	// If key already exists, update in place and move to back.
	if _, ok := c.entries[key]; ok {
		c.entries[key] = &tileCacheEntry{data: data, createdAt: time.Now()}
		c.removeFromOrder(key)
		c.order = append(c.order, key)
		return
	}

	// Evict from front if at capacity.
	for len(c.entries) >= c.maxEntries && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}

	c.entries[key] = &tileCacheEntry{data: data, createdAt: time.Now()}
	c.order = append(c.order, key)
}

// Invalidate removes all cached entries whose key starts with the given layer prefix.
func (c *TileCache) Invalidate(layer string) {
	prefix := layer + "/"

	c.mu.Lock()
	defer c.mu.Unlock()

	var remaining []string
	for _, key := range c.order {
		if strings.HasPrefix(key, prefix) {
			delete(c.entries, key)
		} else {
			remaining = append(remaining, key)
		}
	}
	c.order = remaining
}

// Stats returns cache performance statistics.
func (c *TileCache) Stats() CacheStats {
	c.mu.RLock()
	entries := len(c.entries)
	maxEntries := c.maxEntries
	c.mu.RUnlock()

	hits := c.hits.Load()
	misses := c.misses.Load()

	var hitRate float64
	if total := hits + misses; total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return CacheStats{
		Entries:    entries,
		MaxEntries: maxEntries,
		Hits:       hits,
		Misses:     misses,
		HitRate:    hitRate,
	}
}

// removeFromOrder removes a key from the LRU order slice.
func (c *TileCache) removeFromOrder(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}
