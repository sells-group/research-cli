package apicache

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/opsmetrics"
)

// RedisConfig configures a Redis-backed API cache.
type RedisConfig struct {
	URL            string
	KeyPrefix      string
	ConnectTimeout time.Duration
}

type redisCache struct {
	client    *redis.Client
	keyPrefix string
}

// NewRedis creates a Redis-backed API cache.
func NewRedis(cfg RedisConfig) (Cache, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, eris.New("apicache: redis url is required")
	}

	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, eris.Wrap(err, "apicache: parse redis url")
	}
	if cfg.ConnectTimeout > 0 {
		opts.DialTimeout = cfg.ConnectTimeout
		opts.ReadTimeout = cfg.ConnectTimeout
		opts.WriteTimeout = cfg.ConnectTimeout
	}

	client := redis.NewClient(opts)
	if err := client.Ping(redisContext()).Err(); err != nil {
		_ = client.Close()
		return nil, eris.Wrap(err, "apicache: ping redis")
	}

	return &redisCache{
		client:    client,
		keyPrefix: strings.TrimSpace(cfg.KeyPrefix),
	}, nil
}

func (c *redisCache) namespaced(key string) string {
	if c.keyPrefix == "" {
		return key
	}
	return c.keyPrefix + ":" + key
}

// Get implements Cache.
func (c *redisCache) Get(key string) ([]byte, bool) {
	raw, err := c.client.Get(redisContext(), c.namespaced(key)).Bytes()
	if err != nil {
		opsmetrics.RecordCacheEvent("miss", key, "redis")
		return nil, false
	}

	var entry cachedValue
	if err := json.Unmarshal(raw, &entry); err != nil {
		opsmetrics.RecordCacheEvent("miss", key, "redis")
		return nil, false
	}
	if nowUTC().After(entry.ExpiresAt) {
		_ = c.Delete(key)
		opsmetrics.RecordCacheEvent("miss", key, "redis")
		return nil, false
	}
	opsmetrics.RecordCacheEvent("hit", key, "redis")
	return entry.Payload, true
}

// Set implements Cache.
func (c *redisCache) Set(key string, value any, ttl time.Duration) error {
	payload, err := marshalValue(value)
	if err != nil {
		return err
	}

	entry, err := json.Marshal(cachedValue{
		Payload:   payload,
		ExpiresAt: nowUTC().Add(ttl),
	})
	if err != nil {
		return eris.Wrap(err, "apicache: marshal redis cache entry")
	}

	if err := c.client.Set(redisContext(), c.namespaced(key), entry, ttl).Err(); err != nil {
		return eris.Wrap(err, "apicache: set redis key")
	}
	opsmetrics.RecordCacheEvent("set", key, "redis")
	return nil
}

// Delete implements Cache.
func (c *redisCache) Delete(keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	qualified := make([]string, 0, len(keys))
	for _, key := range keys {
		qualified = append(qualified, c.namespaced(key))
	}
	if err := c.client.Del(redisContext(), qualified...).Err(); err != nil {
		return eris.Wrap(err, "apicache: delete redis keys")
	}
	for _, key := range keys {
		opsmetrics.RecordCacheEvent("delete", key, "redis")
	}
	return nil
}

// DeleteDomains implements Cache.
func (c *redisCache) DeleteDomains(domains ...Domain) error {
	return c.Delete(domainKeys(domains...)...)
}

// Close implements Cache.
func (c *redisCache) Close() error {
	return c.client.Close()
}
