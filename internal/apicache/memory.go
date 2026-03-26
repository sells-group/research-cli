package apicache

import (
	"sync"
	"time"

	"github.com/sells-group/research-cli/internal/opsmetrics"
)

type memoryCache struct {
	mu    sync.RWMutex
	items map[string]cachedValue
}

// NewMemory creates an in-memory API cache.
func NewMemory() Cache {
	return &memoryCache{
		items: make(map[string]cachedValue),
	}
}

// Get implements Cache.
func (c *memoryCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || nowUTC().After(item.ExpiresAt) {
		if ok {
			_ = c.Delete(key)
		}
		opsmetrics.RecordCacheEvent("miss", key, "memory")
		return nil, false
	}
	opsmetrics.RecordCacheEvent("hit", key, "memory")
	return append([]byte(nil), item.Payload...), true
}

// Set implements Cache.
func (c *memoryCache) Set(key string, value any, ttl time.Duration) error {
	payload, err := marshalValue(value)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.items[key] = cachedValue{
		Payload:   payload,
		ExpiresAt: nowUTC().Add(ttl),
	}
	c.mu.Unlock()
	opsmetrics.RecordCacheEvent("set", key, "memory")
	return nil
}

// Delete implements Cache.
func (c *memoryCache) Delete(keys ...string) error {
	c.mu.Lock()
	for _, key := range keys {
		delete(c.items, key)
		opsmetrics.RecordCacheEvent("delete", key, "memory")
	}
	c.mu.Unlock()
	return nil
}

// DeleteDomains implements Cache.
func (c *memoryCache) DeleteDomains(domains ...Domain) error {
	return c.Delete(domainKeys(domains...)...)
}

// Close implements Cache.
func (c *memoryCache) Close() error {
	c.mu.Lock()
	c.items = map[string]cachedValue{}
	c.mu.Unlock()
	return nil
}
