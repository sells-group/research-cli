package anthropic

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/rotisserie/eris"
)

const (
	defaultBatchPollInitial = 2 * time.Second
	defaultBatchPollCap     = 15 * time.Second
	defaultBatchPollTimeout = 30 * time.Minute
)

// PollOption configures batch polling behavior.
type PollOption func(*pollConfig)

type pollConfig struct {
	initial time.Duration
	cap     time.Duration
	timeout time.Duration
}

func defaultPollConfig() pollConfig {
	return pollConfig{
		initial: defaultBatchPollInitial,
		cap:     defaultBatchPollCap,
		timeout: defaultBatchPollTimeout,
	}
}

// WithPollInterval overrides the initial poll interval.
func WithPollInterval(d time.Duration) PollOption {
	return func(c *pollConfig) {
		c.initial = d
	}
}

// WithPollCap overrides the maximum poll interval.
func WithPollCap(d time.Duration) PollOption {
	return func(c *pollConfig) {
		c.cap = d
	}
}

// WithPollTimeout overrides the default poll timeout.
func WithPollTimeout(d time.Duration) PollOption {
	return func(c *pollConfig) {
		c.timeout = d
	}
}

// PollBatch polls GetBatch until the batch ends or the context expires.
// Uses exponential backoff: 5s -> 10s -> 20s -> 30s (capped).
func PollBatch(ctx context.Context, client Client, batchID string, opts ...PollOption) (*BatchResponse, error) {
	cfg := defaultPollConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	interval := cfg.initial
	for {
		batch, err := client.GetBatch(ctx, batchID)
		if err != nil {
			return nil, eris.Wrap(err, fmt.Sprintf("anthropic: poll batch %s", batchID))
		}

		if batch.ProcessingStatus == "ended" {
			return batch, nil
		}

		select {
		case <-ctx.Done():
			return nil, eris.Wrap(ctx.Err(), fmt.Sprintf("anthropic: poll batch %s timed out", batchID))
		case <-time.After(interval):
		}

		// Exponential backoff with jitter: double, cap, then add ±20% jitter.
		interval *= 2
		if interval > cfg.cap {
			interval = cfg.cap
		}
		jitter := time.Duration(rand.Int64N(int64(interval) / 5))
		if rand.IntN(2) == 0 {
			interval += jitter
		} else {
			interval -= jitter
		}
	}
}

// CollectBatchResults drains a BatchResultIterator and returns succeeded results
// keyed by custom_id. Non-succeeded items are skipped.
func CollectBatchResults(iter BatchResultIterator) (map[string]*MessageResponse, error) {
	defer iter.Close()

	results := make(map[string]*MessageResponse)
	for iter.Next() {
		item := iter.Item()
		if item.Type == "succeeded" && item.Message != nil {
			results[item.CustomID] = item.Message
		}
	}
	if err := iter.Err(); err != nil {
		return nil, eris.Wrap(err, "anthropic: collect batch results")
	}

	return results, nil
}
