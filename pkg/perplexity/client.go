package perplexity

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
)

const (
	defaultBaseURL = "https://api.perplexity.ai"
	defaultModel   = "sonar-pro"
)

// Client performs chat completions against the Perplexity API.
type Client interface {
	ChatCompletion(ctx context.Context, req ChatCompletionRequest) (*ChatCompletionResponse, error)
}

// ChatCompletionRequest is the request body for POST /chat/completions.
type ChatCompletionRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature *float64  `json:"temperature,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
}

// Message represents a single message in the conversation.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionResponse is the response from POST /chat/completions.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// Choice is a single completion choice.
type Choice struct {
	Index   int     `json:"index"`
	Message Message `json:"message"`
}

// Usage reports token consumption.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// Option configures the client.
type Option func(*httpClient)

// WithBaseURL overrides the default API base URL.
func WithBaseURL(url string) Option {
	return func(c *httpClient) {
		c.baseURL = url
	}
}

// WithModel overrides the default model.
func WithModel(model string) Option {
	return func(c *httpClient) {
		c.model = model
	}
}

// WithHTTPClient overrides the default http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *httpClient) {
		c.http = hc
	}
}

type httpClient struct {
	apiKey  string
	baseURL string
	model   string
	http    *http.Client
}

// NewClient creates a Perplexity API client.
func NewClient(apiKey string, opts ...Option) Client {
	c := &httpClient{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		model:   defaultModel,
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

const maxRetryAttempts = 3

func (c *httpClient) ChatCompletion(ctx context.Context, req ChatCompletionRequest) (*ChatCompletionResponse, error) {
	if req.Model == "" {
		req.Model = c.model
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, eris.Wrap(err, "perplexity: marshal request")
	}

	backoff := 500 * time.Millisecond
	var lastErr error

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			backoff *= 2
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, eris.Wrap(err, "perplexity: create request")
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.http.Do(httpReq)
		if err != nil {
			// Don't retry on context cancellation/deadline.
			if ctx.Err() != nil {
				return nil, eris.Wrap(err, "perplexity: send request")
			}
			lastErr = eris.Wrap(err, "perplexity: send request")
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, eris.Wrap(err, "perplexity: read response")
		}

		if resp.StatusCode == http.StatusOK {
			var result ChatCompletionResponse
			if err := json.Unmarshal(respBody, &result); err != nil {
				return nil, eris.Wrap(err, "perplexity: unmarshal response")
			}
			return &result, nil
		}

		lastErr = eris.Errorf("perplexity: unexpected status %d: %s", resp.StatusCode, string(respBody))

		// Retry on 5xx and 429; don't retry other 4xx.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			continue
		}
		return nil, lastErr
	}

	return nil, lastErr
}
