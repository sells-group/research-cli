package main

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/store"
)

type poolProvider interface {
	Pool() db.Pool
}

func configuredReadModelDSN() string {
	if cfg.Fedsync.DatabaseURL != "" {
		return cfg.Fedsync.DatabaseURL
	}
	return cfg.Store.DatabaseURL
}

func sharedReadModelPool(ctx context.Context, st store.Store) (db.Pool, func(), error) {
	if provider, ok := st.(poolProvider); ok && shouldReuseStorePool() {
		return provider.Pool(), func() {}, nil
	}

	if cfg.Fedsync.DatabaseURL == "" {
		return nil, func() {}, nil
	}

	pool, err := openReadModelPool(ctx)
	if err != nil {
		return nil, nil, err
	}
	return pool, pool.Close, nil
}

func openReadModelPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := configuredReadModelDSN()
	if dsn == "" {
		return nil, eris.New("fedsync: no database_url configured (set fedsync.database_url or store.database_url)")
	}

	return openConfiguredPool(ctx, dsn, "read-model")
}

func openStorePostgresPool(ctx context.Context) (*pgxpool.Pool, error) {
	if cfg.Store.DatabaseURL == "" {
		return nil, eris.New("store: no database_url configured (set store.database_url)")
	}
	return openConfiguredPool(ctx, cfg.Store.DatabaseURL, "store")
}

func openConfiguredPool(ctx context.Context, dsn string, label string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, eris.Wrapf(err, "%s: parse connection string", label)
	}
	poolCfg.MaxConns = resolvedMaxConns()
	poolCfg.MinConns = resolvedMinConns()
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, eris.Wrapf(err, "%s: create connection pool", label)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrapf(err, "%s: ping database", label)
	}

	zap.L().Debug("connected database pool", zap.String("pool", label))
	return pool, nil
}

func resolvedMaxConns() int32 {
	if cfg.Store.MaxConns > 0 {
		return cfg.Store.MaxConns
	}
	return 15
}

func resolvedMinConns() int32 {
	if cfg.Store.MinConns > 0 {
		return cfg.Store.MinConns
	}
	return 2
}

func shouldReuseStorePool() bool {
	if cfg == nil || cfg.Store.DatabaseURL == "" {
		return false
	}
	if cfg.Fedsync.DatabaseURL == "" {
		return true
	}
	return cfg.Fedsync.DatabaseURL == cfg.Store.DatabaseURL
}

func openServeAPICache(ctx context.Context) (apicache.Cache, error) {
	cache, err := openRedisAPICache(ctx)
	if err != nil {
		return nil, err
	}
	if cache != nil {
		return cache, nil
	}
	return apicache.NewMemory(), nil
}

func openRedisAPICache(_ context.Context) (apicache.Cache, error) {
	if cfg == nil {
		return nil, nil
	}

	backend := strings.ToLower(strings.TrimSpace(cfg.Server.HTTPCache.Backend))
	redisURL := strings.TrimSpace(cfg.Server.HTTPCache.RedisURL)
	if redisURL == "" && backend != "redis" {
		return nil, nil
	}
	if redisURL == "" {
		return nil, eris.New("server.http_cache.redis_url is required when server.http_cache.backend=redis")
	}

	return apicache.NewRedis(apicache.RedisConfig{
		URL:            redisURL,
		KeyPrefix:      cfg.Server.HTTPCache.KeyPrefix,
		ConnectTimeout: time.Duration(cfg.Server.HTTPCache.ConnectTimeoutSecs) * time.Second,
	})
}

func attachSyncLogCache(ctx context.Context, syncLog *fedsync.SyncLog) (func(), error) {
	if syncLog == nil {
		return func() {}, nil
	}

	cache, err := openRedisAPICache(ctx)
	if err != nil {
		return nil, err
	}
	if cache == nil {
		return func() {}, nil
	}

	syncLog.SetCache(cache)
	return func() {
		_ = cache.Close()
	}, nil
}
