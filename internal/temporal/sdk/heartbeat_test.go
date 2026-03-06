package sdk

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunWithHeartbeat_Success(t *testing.T) {
	called := false
	err := RunWithHeartbeat(context.Background(), "test", 50*time.Millisecond, func(_ context.Context) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

func TestRunWithHeartbeat_Error(t *testing.T) {
	err := RunWithHeartbeat(context.Background(), "test", 50*time.Millisecond, func(_ context.Context) error {
		return fmt.Errorf("sync failed")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sync failed")
}

func TestRunWithHeartbeat_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	err := RunWithHeartbeat(ctx, "test", 50*time.Millisecond, func(ctx context.Context) error {
		cancel()
		return ctx.Err()
	})
	require.Error(t, err)
}
