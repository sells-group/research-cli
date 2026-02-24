package perplexity

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChatCompletion(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantErr    string
		wantID     string
		wantTokens int
	}{
		{
			name:   "success",
			status: http.StatusOK,
			body: `{
				"id": "cmpl-123",
				"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hello!"}}],
				"usage": {"prompt_tokens": 10, "completion_tokens": 5}
			}`,
			wantID:     "cmpl-123",
			wantTokens: 5,
		},
		{
			name:    "rate_limit",
			status:  http.StatusTooManyRequests,
			body:    `{"error": "rate limit exceeded"}`,
			wantErr: "unexpected status 429",
		},
		{
			name:    "server_error",
			status:  http.StatusInternalServerError,
			body:    `{"error": "internal server error"}`,
			wantErr: "unexpected status 500",
		},
		{
			name:    "malformed_response",
			status:  http.StatusOK,
			body:    `{invalid json`,
			wantErr: "unmarshal response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/chat/completions", r.URL.Path)
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
				assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			client := NewClient("test-key", WithBaseURL(srv.URL))

			resp, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
				Messages: []Message{{Role: "user", Content: "Hi"}},
			})

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, resp)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.wantID, resp.ID)
			require.Len(t, resp.Choices, 1)
			assert.Equal(t, "Hello!", resp.Choices[0].Message.Content)
			assert.Equal(t, tt.wantTokens, resp.Usage.CompletionTokens)
		})
	}
}

func TestDefaultModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "sonar-pro", req.Model)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.NoError(t, err)
}

func TestWithModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "sonar", req.Model)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL), WithModel("sonar"))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.NoError(t, err)
}

func TestRequestModelOverridesDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "sonar-reasoning", req.Model)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Model:    "sonar-reasoning",
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.NoError(t, err)
}

func TestContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[],"usage":{}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.ChatCompletion(ctx, ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send request")
}

func TestWithHTTPClient(t *testing.T) {
	t.Parallel()
	customClient := &http.Client{}
	c := NewClient("test-key", WithHTTPClient(customClient))
	hc := c.(*httpClient)
	assert.Equal(t, customClient, hc.http)
}

func TestNewClient_Defaults(t *testing.T) {
	t.Parallel()
	c := NewClient("my-key")
	hc := c.(*httpClient)
	assert.Equal(t, "my-key", hc.apiKey)
	assert.Equal(t, defaultBaseURL, hc.baseURL)
	assert.Equal(t, defaultModel, hc.model)
	assert.NotNil(t, hc.http)
	assert.NotNil(t, hc.http.Transport)
}

func TestErrorResponseIncludesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"invalid api key","message":"check your credentials"}`))
	}))
	defer srv.Close()

	client := NewClient("bad-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
	assert.Contains(t, err.Error(), "invalid api key")
}

func TestChatCompletion_Temperature(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		// Verify temperature is present and set to 0.2
		require.NotNil(t, req.Temperature)
		assert.InDelta(t, 0.2, *req.Temperature, 0.001)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	temp := 0.2
	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages:    []Message{{Role: "user", Content: "test"}},
		Temperature: &temp,
	})
	require.NoError(t, err)
}

func TestChatCompletion_NoTemperature(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read raw body to check temperature is absent
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var raw map[string]any
		err = json.Unmarshal(body, &raw)
		require.NoError(t, err)

		// temperature should be absent (omitempty)
		_, hasTemp := raw["temperature"]
		assert.False(t, hasTemp, "temperature should not be in request body when nil")

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
		// Temperature is nil (not set)
	})
	require.NoError(t, err)
}

func TestChatCompletion_MaxTokens(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ChatCompletionRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		require.NotNil(t, req.MaxTokens)
		assert.Equal(t, 500, *req.MaxTokens)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	maxTokens := 500
	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages:  []Message{{Role: "user", Content: "test"}},
		MaxTokens: &maxTokens,
	})
	require.NoError(t, err)
}

func TestChatCompletion_Retries5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal server error"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"retry-ok","choices":[{"index":0,"message":{"role":"assistant","content":"recovered"}}],"usage":{"prompt_tokens":5,"completion_tokens":3}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "retry-ok", resp.ID)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestChatCompletion_Retries429(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"rate-ok","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}],"usage":{}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "rate-ok", resp.ID)
	assert.Equal(t, int32(2), attempts.Load())
}

func TestChatCompletion_NoRetryOn4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
	assert.Equal(t, int32(1), attempts.Load())
}

func TestChatCompletion_ExhaustsRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
	assert.Equal(t, int32(maxRetryAttempts), attempts.Load())
}

func TestChatCompletion_RetryRespectsContextCancel(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context shortly after the first attempt so it triggers during backoff.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.ChatCompletion(ctx, ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	// Should have completed fewer attempts than the maximum because the context
	// was cancelled during the backoff between the first and second attempt.
	assert.Less(t, attempts.Load(), int32(maxRetryAttempts))
}

func TestChatCompletion_RetrySucceedsOnSecondAttempt(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"temporary failure"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"second-try","choices":[{"index":0,"message":{"role":"assistant","content":"success on retry"}}],"usage":{"prompt_tokens":8,"completion_tokens":4}}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	resp, err := client.ChatCompletion(context.Background(), ChatCompletionRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "second-try", resp.ID)
	require.Len(t, resp.Choices, 1)
	assert.Equal(t, "success on retry", resp.Choices[0].Message.Content)
	assert.Equal(t, 8, resp.Usage.PromptTokens)
	assert.Equal(t, 4, resp.Usage.CompletionTokens)
	assert.Equal(t, int32(2), attempts.Load())
}
