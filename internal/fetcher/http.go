package fetcher

import (
	"context"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// HTTPOptions configures the HTTP fetcher.
type HTTPOptions struct {
	UserAgent    string
	Timeout      time.Duration
	MaxRetries   int
	RateLimiters map[string]*rate.Limiter
}

// AdaptiveLimiter wraps a rate.Limiter with adaptive rate adjustment.
// On success it increases the rate by 20% (up to 2x initial).
// On 429 it halves the rate (down to initial/4 minimum).
type AdaptiveLimiter struct {
	mu          sync.Mutex
	limiter     *rate.Limiter
	initialRate rate.Limit
	maxRate     rate.Limit
	minRate     rate.Limit
	currentRate rate.Limit
}

// NewAdaptiveLimiter creates an adaptive rate limiter that auto-tunes.
func NewAdaptiveLimiter(initialRate rate.Limit, burst int) *AdaptiveLimiter {
	return &AdaptiveLimiter{
		limiter:     rate.NewLimiter(initialRate, burst),
		initialRate: initialRate,
		maxRate:     initialRate * 2,
		minRate:     initialRate / 4,
		currentRate: initialRate,
	}
}

// Wait blocks until the limiter allows an event.
func (a *AdaptiveLimiter) Wait(ctx context.Context) error {
	return a.limiter.Wait(ctx)
}

// OnSuccess increases the rate by 20%, up to 2x initial.
func (a *AdaptiveLimiter) OnSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	newRate := a.currentRate * 1.2
	if newRate > a.maxRate {
		newRate = a.maxRate
	}
	a.currentRate = newRate
	a.limiter.SetLimit(newRate)
}

// OnRateLimit halves the rate on 429 responses.
func (a *AdaptiveLimiter) OnRateLimit() {
	a.mu.Lock()
	defer a.mu.Unlock()
	newRate := a.currentRate * 0.5
	if newRate < a.minRate {
		newRate = a.minRate
	}
	a.currentRate = newRate
	a.limiter.SetLimit(newRate)
	zap.L().Warn("adaptive rate limit: reducing rate after 429",
		zap.Float64("new_rate", float64(newRate)),
	)
}

// Limit returns the current rate limit.
func (a *AdaptiveLimiter) Limit() rate.Limit {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.currentRate
}

// HTTPFetcher implements Fetcher using net/http with retry and rate limiting.
type HTTPFetcher struct {
	client           *http.Client
	opts             HTTPOptions
	limiters         map[string]*rate.Limiter
	adaptiveLimiters map[string]*AdaptiveLimiter
}

// DefaultRateLimiters returns the default per-host rate limiters.
func DefaultRateLimiters() map[string]*rate.Limiter {
	return map[string]*rate.Limiter{
		"efts.sec.gov": rate.NewLimiter(10, 10),
		"www.sec.gov":  rate.NewLimiter(10, 10),
		"data.sec.gov": rate.NewLimiter(10, 10),
		"api.sam.gov":  rate.NewLimiter(5, 5),
	}
}

// NewHTTPFetcher creates a new HTTPFetcher with the given options.
func NewHTTPFetcher(opts HTTPOptions) *HTTPFetcher {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "research-cli/1.0"
	}
	limiters := make(map[string]*rate.Limiter)
	for k, v := range opts.RateLimiters {
		limiters[k] = v
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     20,
		IdleConnTimeout:     90 * time.Second,
	}
	return &HTTPFetcher{
		client: &http.Client{
			Timeout:   opts.Timeout,
			Transport: transport,
		},
		opts:             opts,
		limiters:         limiters,
		adaptiveLimiters: DefaultAdaptiveLimiters(),
	}
}

// DefaultAdaptiveLimiters returns adaptive rate limiters for known hosts.
func DefaultAdaptiveLimiters() map[string]*AdaptiveLimiter {
	return map[string]*AdaptiveLimiter{
		"efts.sec.gov": NewAdaptiveLimiter(10, 10),
		"www.sec.gov":  NewAdaptiveLimiter(10, 10),
		"data.sec.gov": NewAdaptiveLimiter(10, 10),
		"api.sam.gov":  NewAdaptiveLimiter(5, 5),
	}
}

// adaptiveLimiterFor returns the adaptive limiter for the given host, if any.
func (f *HTTPFetcher) adaptiveLimiterFor(rawURL string) *AdaptiveLimiter {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	return f.adaptiveLimiters[u.Host]
}

func (f *HTTPFetcher) limiterFor(rawURL string) *rate.Limiter {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rate.NewLimiter(20, 20)
	}
	if lim, ok := f.limiters[u.Host]; ok {
		return lim
	}
	return rate.NewLimiter(20, 20)
}

func (f *HTTPFetcher) doWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	adaptive := f.adaptiveLimiterFor(req.URL.String())

	var lastErr error
	for attempt := range f.opts.MaxRetries {
		// Use adaptive limiter if available, otherwise fall back to fixed.
		if adaptive != nil {
			if err := adaptive.Wait(ctx); err != nil {
				return nil, eris.Wrap(err, "rate limiter wait")
			}
		} else {
			lim := f.limiterFor(req.URL.String())
			if err := lim.Wait(ctx); err != nil {
				return nil, eris.Wrap(err, "rate limiter wait")
			}
		}

		cloned := req.Clone(ctx)
		resp, err := f.client.Do(cloned)
		if err != nil {
			lastErr = err
			zap.L().Warn("http request failed, retrying",
				zap.String("url", req.URL.String()),
				zap.Int("attempt", attempt+1),
				zap.Error(err),
			)
			f.backoff(ctx, attempt)
			continue
		}

		// Handle 429 Too Many Requests with adaptive backoff.
		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			lastErr = eris.Errorf("http 429 from %s", req.URL.String())
			if adaptive != nil {
				adaptive.OnRateLimit()
			}
			zap.L().Warn("rate limited (429), backing off",
				zap.String("url", req.URL.String()),
				zap.Int("attempt", attempt+1),
			)
			f.backoff(ctx, attempt)
			continue
		}

		if resp.StatusCode >= 500 {
			_ = resp.Body.Close()
			lastErr = eris.Errorf("http %d from %s", resp.StatusCode, req.URL.String())
			zap.L().Warn("server error, retrying",
				zap.String("url", req.URL.String()),
				zap.Int("status", resp.StatusCode),
				zap.Int("attempt", attempt+1),
			)
			f.backoff(ctx, attempt)
			continue
		}

		// Success: increase adaptive rate.
		if adaptive != nil {
			adaptive.OnSuccess()
		}

		return resp, nil
	}

	return nil, eris.Wrap(lastErr, "all retries exhausted")
}

func (f *HTTPFetcher) backoff(ctx context.Context, attempt int) {
	base := time.Second
	maxBackoff := 30 * time.Second
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt)))
	if d > maxBackoff {
		d = maxBackoff
	}
	jitter := time.Duration(rand.Int64N(int64(d) / 2))
	d = d + jitter

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Download fetches the URL and returns the response body.
func (f *HTTPFetcher) Download(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "create request")
	}
	req.Header.Set("User-Agent", f.opts.UserAgent)

	resp, err := f.doWithRetry(ctx, req)
	if err != nil {
		return nil, eris.Wrap(err, "download")
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, eris.Errorf("download: unexpected status %d from %s", resp.StatusCode, rawURL)
	}

	return resp.Body, nil
}

// DownloadToFile fetches the URL and writes it to the given path.
func (f *HTTPFetcher) DownloadToFile(ctx context.Context, rawURL string, path string) (int64, error) {
	body, err := f.Download(ctx, rawURL)
	if err != nil {
		return 0, err
	}
	defer body.Close() //nolint:errcheck

	file, err := os.Create(path)
	if err != nil {
		return 0, eris.Wrap(err, "create file")
	}
	defer file.Close() //nolint:errcheck

	n, err := io.Copy(file, body)
	if err != nil {
		return n, eris.Wrap(err, "write file")
	}

	return n, nil
}

// HeadETag performs a HEAD request and returns the ETag header value.
func (f *HTTPFetcher) HeadETag(ctx context.Context, rawURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return "", eris.Wrap(err, "create head request")
	}
	req.Header.Set("User-Agent", f.opts.UserAgent)

	lim := f.limiterFor(rawURL)
	if err := lim.Wait(ctx); err != nil {
		return "", eris.Wrap(err, "rate limiter wait")
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return "", eris.Wrap(err, "head request")
	}
	defer resp.Body.Close() //nolint:errcheck

	return resp.Header.Get("ETag"), nil
}

// DownloadIfChanged fetches the URL only if the ETag has changed.
func (f *HTTPFetcher) DownloadIfChanged(ctx context.Context, rawURL string, etag string) (io.ReadCloser, string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", false, eris.Wrap(err, "create request")
	}
	req.Header.Set("User-Agent", f.opts.UserAgent)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	lim := f.limiterFor(rawURL)
	if err := lim.Wait(ctx); err != nil {
		return nil, "", false, eris.Wrap(err, "rate limiter wait")
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, "", false, eris.Wrap(err, "download if changed")
	}

	if resp.StatusCode == http.StatusNotModified {
		_ = resp.Body.Close()
		return nil, etag, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, "", false, eris.Errorf("download if changed: unexpected status %d from %s", resp.StatusCode, rawURL)
	}

	newETag := resp.Header.Get("ETag")
	return resp.Body, newETag, true, nil
}
