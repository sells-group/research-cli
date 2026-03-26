// Package apicache provides shared caching for API read surfaces.
package apicache

import (
	"context"
	"encoding/json"
	"time"
)

// Domain identifies a cache invalidation group.
type Domain string

const (
	// DomainRuns covers queue and run dashboard reads.
	DomainRuns Domain = "runs"
	// DomainData covers table metadata and explorer support reads.
	DomainData Domain = "data"
	// DomainFedsync covers fedsync status reads.
	DomainFedsync Domain = "fedsync"
)

const (
	// KeyQueueStatus caches /queue/status.
	KeyQueueStatus = "queue_status"
	// KeyDataTables caches /data/tables.
	KeyDataTables = "data_tables"
	// KeyFedsyncStatuses caches /fedsync/statuses.
	KeyFedsyncStatuses = "fedsync_statuses"
)

// Cache provides a backend-neutral API cache.
type Cache interface {
	Get(key string) ([]byte, bool)
	Set(key string, value any, ttl time.Duration) error
	Delete(keys ...string) error
	DeleteDomains(domains ...Domain) error
	Close() error
}

type cachedValue struct {
	Payload   []byte    `json:"payload"`
	ExpiresAt time.Time `json:"expires_at"`
}

func marshalValue(value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return typed, nil
	case json.RawMessage:
		return typed, nil
	default:
		return json.Marshal(value)
	}
}

func nowUTC() time.Time {
	return time.Now().UTC()
}

func domainKeys(domains ...Domain) []string {
	keys := make([]string, 0, len(domains)*2)
	for _, domain := range domains {
		switch domain {
		case DomainRuns:
			keys = append(keys, KeyQueueStatus)
		case DomainData:
			keys = append(keys, KeyDataTables)
		case DomainFedsync:
			keys = append(keys, KeyFedsyncStatuses)
		}
	}
	return keys
}

func redisContext() context.Context {
	return context.Background()
}
