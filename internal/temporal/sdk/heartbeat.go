package sdk

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
)

// RunWithHeartbeat calls fn while sending Temporal heartbeats at the given interval.
// The heartbeat goroutine is stopped when fn returns or ctx is cancelled.
func RunWithHeartbeat(ctx context.Context, label string, interval time.Duration, fn func(context.Context) error) error {
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, label)
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	defer close(heartbeatDone)

	return fn(ctx)
}
