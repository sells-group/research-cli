package firecrawl

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
)

const (
	defaultPollInitial = 2 * time.Second
	defaultPollCap     = 15 * time.Second
	defaultPollTimeout = 5 * time.Minute
)

// PollOption configures polling behavior.
type PollOption func(*pollConfig)

type pollConfig struct {
	initial time.Duration
	cap     time.Duration
	timeout time.Duration
}

func defaultPollConfig() pollConfig {
	return pollConfig{
		initial: defaultPollInitial,
		cap:     defaultPollCap,
		timeout: defaultPollTimeout,
	}
}

// WithPollInterval overrides the initial poll interval.
func WithPollInterval(d time.Duration) PollOption {
	return func(c *pollConfig) {
		c.initial = d
	}
}

// WithPollStep is a no-op retained for backward compatibility.
// Polling now uses exponential backoff (doubling) instead of linear increments.
func WithPollStep(d time.Duration) PollOption {
	return func(c *pollConfig) {}
}

// WithPollCap overrides the maximum poll interval.
func WithPollCap(d time.Duration) PollOption {
	return func(c *pollConfig) {
		c.cap = d
	}
}

// WithPollTimeout overrides the default timeout (applied only if the parent
// context has no deadline).
func WithPollTimeout(d time.Duration) PollOption {
	return func(c *pollConfig) {
		c.timeout = d
	}
}

// PollCrawl polls GetCrawlStatus until the crawl completes, fails, or the
// context expires. Uses exponential backoff: 2s -> 4s -> 8s -> 15s (capped).
func PollCrawl(ctx context.Context, client Client, id string, opts ...PollOption) (*CrawlStatusResponse, error) {
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
		status, err := client.GetCrawlStatus(ctx, id)
		if err != nil {
			return nil, eris.Wrap(err, fmt.Sprintf("firecrawl: poll crawl %s", id))
		}

		switch status.Status {
		case "completed":
			return status, nil
		case "failed":
			return nil, eris.Errorf("firecrawl: crawl %s failed", id)
		}

		select {
		case <-ctx.Done():
			return nil, eris.Wrap(ctx.Err(), fmt.Sprintf("firecrawl: poll crawl %s timed out", id))
		case <-time.After(interval):
		}

		interval *= 2
		if interval > cfg.cap {
			interval = cfg.cap
		}
	}
}

// PollBatchScrape polls GetBatchScrapeStatus until the batch completes, fails,
// or the context expires. Uses exponential backoff: 2s -> 4s -> 8s -> 15s (capped).
func PollBatchScrape(ctx context.Context, client Client, id string, opts ...PollOption) (*BatchScrapeStatusResponse, error) {
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
		status, err := client.GetBatchScrapeStatus(ctx, id)
		if err != nil {
			return nil, eris.Wrap(err, fmt.Sprintf("firecrawl: poll batch scrape %s", id))
		}

		switch status.Status {
		case "completed":
			return status, nil
		case "failed":
			return nil, eris.Errorf("firecrawl: batch scrape %s failed", id)
		}

		select {
		case <-ctx.Done():
			return nil, eris.Wrap(ctx.Err(), fmt.Sprintf("firecrawl: poll batch scrape %s timed out", id))
		case <-time.After(interval):
		}

		interval *= 2
		if interval > cfg.cap {
			interval = cfg.cap
		}
	}
}
