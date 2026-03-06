package sdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShortActivityOptions(t *testing.T) {
	opts := ShortActivityOptions()
	require.Equal(t, 2*time.Minute, opts.StartToCloseTimeout)
	require.NotNil(t, opts.RetryPolicy)
	require.Equal(t, int32(3), opts.RetryPolicy.MaximumAttempts)
}

func TestLongActivityOptions(t *testing.T) {
	opts := LongActivityOptions()
	require.Equal(t, 60*time.Minute, opts.StartToCloseTimeout)
	require.Equal(t, 2*time.Minute, opts.HeartbeatTimeout)
	require.NotNil(t, opts.RetryPolicy)
	require.Equal(t, int32(3), opts.RetryPolicy.MaximumAttempts)
	require.Equal(t, 5*time.Second, opts.RetryPolicy.InitialInterval)
	require.Equal(t, 2.0, opts.RetryPolicy.BackoffCoefficient)
	require.Equal(t, 5*time.Minute, opts.RetryPolicy.MaximumInterval)
}

func TestLogActivityOptions(t *testing.T) {
	opts := LogActivityOptions()
	require.Equal(t, 30*time.Second, opts.StartToCloseTimeout)
	require.NotNil(t, opts.RetryPolicy)
	require.Equal(t, int32(3), opts.RetryPolicy.MaximumAttempts)
}
