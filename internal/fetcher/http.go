package fetcher

import (
	"context"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
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

// HTTPFetcher implements Fetcher using net/http with retry and rate limiting.
type HTTPFetcher struct {
	client   *http.Client
	opts     HTTPOptions
	limiters map[string]*rate.Limiter
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
	return &HTTPFetcher{
		client: &http.Client{Timeout: opts.Timeout},
		opts:   opts,
		limiters: limiters,
	}
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
	lim := f.limiterFor(req.URL.String())

	var lastErr error
	for attempt := range f.opts.MaxRetries {
		if err := lim.Wait(ctx); err != nil {
			return nil, eris.Wrap(err, "rate limiter wait")
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

		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = eris.Errorf("http %d from %s", resp.StatusCode, req.URL.String())
			zap.L().Warn("server error, retrying",
				zap.String("url", req.URL.String()),
				zap.Int("status", resp.StatusCode),
				zap.Int("attempt", attempt+1),
			)
			f.backoff(ctx, attempt)
			continue
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
		resp.Body.Close()
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
	defer body.Close()

	file, err := os.Create(path)
	if err != nil {
		return 0, eris.Wrap(err, "create file")
	}
	defer file.Close()

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
	defer resp.Body.Close()

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
		resp.Body.Close()
		return nil, etag, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, "", false, eris.Errorf("download if changed: unexpected status %d from %s", resp.StatusCode, rawURL)
	}

	newETag := resp.Header.Get("ETag")
	return resp.Body, newETag, true, nil
}
