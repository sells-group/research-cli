package main

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/store"
	storemocks "github.com/sells-group/research-cli/internal/store/mocks"
)

type fakePoolStore struct {
	store.Store
	pool db.Pool
}

func (f *fakePoolStore) Pool() db.Pool {
	return f.pool
}

func TestConfiguredReadModelDSN_PrefersFedsync(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{DatabaseURL: "postgres://fedsync"},
		Store:   config.StoreConfig{DatabaseURL: "postgres://store"},
	}

	assert.Equal(t, "postgres://fedsync", configuredReadModelDSN())
}

func TestConfiguredReadModelDSN_FallsBackToStore(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{},
		Store:   config.StoreConfig{DatabaseURL: "postgres://store"},
	}

	assert.Equal(t, "postgres://store", configuredReadModelDSN())
}

func TestSharedReadModelPool_ReusesStorePoolWhenDSNsMatch(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{DatabaseURL: "postgres://shared"},
		Store:   config.StoreConfig{DatabaseURL: "postgres://shared"},
	}

	st := &fakePoolStore{pool: mockPool}
	pool, closeFn, err := sharedReadModelPool(context.Background(), st)
	require.NoError(t, err)
	require.NotNil(t, closeFn)
	assert.Same(t, mockPool, pool)
	closeFn()
}

func TestSharedReadModelPool_SkipsSQLiteOnlyServe(t *testing.T) {
	cfg = &config.Config{
		Fedsync: config.FedsyncConfig{},
		Store:   config.StoreConfig{DatabaseURL: "research.db"},
	}

	st := storemocks.NewMockStore(t)
	pool, closeFn, err := sharedReadModelPool(context.Background(), st)
	require.NoError(t, err)
	require.NotNil(t, closeFn)
	assert.Nil(t, pool)
	closeFn()
}
