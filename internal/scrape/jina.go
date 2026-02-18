package scrape

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/jina"
)

// circuitBreaker tracks consecutive failures to skip a flaky upstream.
type circuitBreaker struct {
	mu           sync.Mutex
	failures     int
	lastFailure  time.Time
	openUntil    time.Time
	threshold    int           // consecutive failures to trip
	window       time.Duration // failures must occur within this window
	cooldown     time.Duration // how long the circuit stays open
}

func newCircuitBreaker(threshold int, window, cooldown time.Duration) *circuitBreaker {
	return &circuitBreaker{
		threshold: threshold,
		window:    window,
		cooldown:  cooldown,
	}
}

func (cb *circuitBreaker) isOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if time.Now().Before(cb.openUntil) {
		return true
	}
	return false
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	now := time.Now()
	if now.Sub(cb.lastFailure) > cb.window {
		cb.failures = 0
	}
	cb.failures++
	cb.lastFailure = now
	if cb.failures >= cb.threshold {
		cb.openUntil = now.Add(cb.cooldown)
		zap.L().Warn("scrape: jina circuit breaker opened",
			zap.Int("failures", cb.failures),
			zap.Duration("cooldown", cb.cooldown),
		)
	}
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
}

// JinaAdapter wraps a Jina Reader client as a Scraper with a circuit breaker.
type JinaAdapter struct {
	client  jina.Client
	breaker *circuitBreaker
}

// NewJinaAdapter creates a JinaAdapter from a Jina client.
// Includes a circuit breaker: 3 consecutive failures within 30s opens
// the circuit for 60s, causing immediate fallback to the next scraper.
func NewJinaAdapter(client jina.Client) *JinaAdapter {
	return &JinaAdapter{
		client:  client,
		breaker: newCircuitBreaker(3, 30*time.Second, 60*time.Second),
	}
}

func (j *JinaAdapter) Name() string { return "jina" }

// Supports returns true unless the circuit breaker is open.
func (j *JinaAdapter) Supports(_ string) bool {
	return !j.breaker.isOpen()
}

// Scrape fetches a URL via Jina Reader and validates the response.
func (j *JinaAdapter) Scrape(ctx context.Context, targetURL string) (*Result, error) {
	if j.breaker.isOpen() {
		return nil, eris.New("jina: circuit breaker open")
	}

	resp, err := j.client.Read(ctx, targetURL)
	if err != nil {
		j.breaker.recordFailure()
		return nil, err
	}

	if needsFallback(resp) {
		j.breaker.recordFailure()
		return nil, eris.New("jina: response needs fallback")
	}

	j.breaker.recordSuccess()
	return &Result{
		Page: model.CrawledPage{
			URL:        resp.Data.URL,
			Title:      resp.Data.Title,
			Markdown:   resp.Data.Content,
			StatusCode: resp.Code,
		},
		Source: "jina",
	}, nil
}

// needsFallback checks whether a Jina response contains usable content
// or indicates the page is blocked/empty. Returns true if the response
// should be retried with a different scraper.
func needsFallback(resp *jina.ReadResponse) bool {
	if resp == nil {
		return true
	}

	if resp.Code != 0 && resp.Code != 200 {
		return true
	}

	content := strings.TrimSpace(resp.Data.Content)

	if len(content) < 100 {
		return true
	}

	lower := strings.ToLower(content)

	challengeSignatures := []string{
		"checking your browser",
		"enable javascript",
		"please enable cookies",
		"access denied",
		"403 forbidden",
		"just a moment",
		"cloudflare",
		"attention required",
	}

	for _, sig := range challengeSignatures {
		if strings.Contains(lower, sig) && len(content) < 1000 {
			return true
		}
	}

	return false
}
