package anthropic

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestClient creates an sdkClient pointing at a local test server.
func newTestClient(baseURL string) *sdkClient {
	return &sdkClient{
		client: sdk.NewClient(
			option.WithAPIKey("test-key"),
			option.WithBaseURL(baseURL),
		),
	}
}

func TestSDKClient_CreateMessage(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.URL.Path, "/messages")

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"id":   "msg_test_001",
			"type": "message",
			"role": "assistant",
			"content": []map[string]any{
				{"type": "text", "text": "Hello from test"},
			},
			"model":       "claude-sonnet-4-5-20250929",
			"stop_reason": "end_turn",
			"usage": map[string]any{
				"input_tokens":                10,
				"output_tokens":               5,
				"cache_creation_input_tokens": 0,
				"cache_read_input_tokens":     0,
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	resp, err := client.CreateMessage(context.Background(), MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 1024,
		Messages:  []Message{{Role: "user", Content: "Hello"}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "msg_test_001", resp.ID)
	assert.Equal(t, "claude-sonnet-4-5-20250929", resp.Model)
	assert.Equal(t, "end_turn", resp.StopReason)
	require.Len(t, resp.Content, 1)
	assert.Equal(t, "Hello from test", resp.Content[0].Text)
	assert.Equal(t, int64(10), resp.Usage.InputTokens)
	assert.Equal(t, int64(5), resp.Usage.OutputTokens)
}

func TestSDKClient_CreateMessage_WithSystemAndTemp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"id":   "msg_sys",
			"type": "message",
			"role": "assistant",
			"content": []map[string]any{
				{"type": "text", "text": "Acknowledged"},
			},
			"model":       "claude-sonnet-4-5-20250929",
			"stop_reason": "end_turn",
			"usage": map[string]any{
				"input_tokens":                50,
				"output_tokens":               3,
				"cache_creation_input_tokens": 5000,
				"cache_read_input_tokens":     0,
			},
		})
	}))
	defer ts.Close()

	temp := 0.5
	client := newTestClient(ts.URL)
	resp, err := client.CreateMessage(context.Background(), MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 128,
		System: []SystemBlock{
			{Text: "You are a test assistant", CacheControl: &CacheControl{TTL: "1h"}},
		},
		Messages:    []Message{{Role: "user", Content: "Ack"}},
		Temperature: &temp,
	})
	require.NoError(t, err)
	assert.Equal(t, "msg_sys", resp.ID)
	assert.Equal(t, int64(5000), resp.Usage.CacheCreationInputTokens)
}

func TestSDKClient_CreateMessage_Error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"type": "error",
			"error": map[string]any{
				"type":    "api_error",
				"message": "Internal server error",
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	_, err := client.CreateMessage(context.Background(), MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 1024,
		Messages:  []Message{{Role: "user", Content: "Hello"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "anthropic: create message")
}

func TestSDKClient_CreateBatch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/batches")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":                "batch_test_001",
			"type":              "message_batch",
			"processing_status": "in_progress",
			"results_url":       "",
			"request_counts": map[string]any{
				"processing": 2,
				"succeeded":  0,
				"errored":    0,
				"canceled":   0,
				"expired":    0,
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	resp, err := client.CreateBatch(context.Background(), BatchRequest{
		Requests: []BatchRequestItem{
			{CustomID: "q1", Params: MessageRequest{
				Model: "claude-haiku-4-5-20251001", MaxTokens: 512,
				Messages: []Message{{Role: "user", Content: "Q1"}},
			}},
			{CustomID: "q2", Params: MessageRequest{
				Model: "claude-haiku-4-5-20251001", MaxTokens: 512,
				Messages: []Message{{Role: "user", Content: "Q2"}},
			}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "batch_test_001", resp.ID)
	assert.Equal(t, "in_progress", resp.ProcessingStatus)
	assert.Equal(t, int64(2), resp.RequestCounts.Processing)
}

func TestSDKClient_CreateBatch_WithSystemAndTemp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":                "batch_sys",
			"type":              "message_batch",
			"processing_status": "in_progress",
			"request_counts": map[string]any{
				"processing": 1,
				"succeeded":  0,
				"errored":    0,
				"canceled":   0,
				"expired":    0,
			},
		})
	}))
	defer ts.Close()

	temp := 0.3
	client := newTestClient(ts.URL)
	resp, err := client.CreateBatch(context.Background(), BatchRequest{
		Requests: []BatchRequestItem{
			{CustomID: "q1", Params: MessageRequest{
				Model: "claude-sonnet-4-5-20250929", MaxTokens: 1024,
				System:      []SystemBlock{{Text: "Context data"}},
				Messages:    []Message{{Role: "user", Content: "Q1"}},
				Temperature: &temp,
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "batch_sys", resp.ID)
}

func TestSDKClient_CreateBatch_Error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "rate_limit_error",
				"message": "Rate limit exceeded",
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	_, err := client.CreateBatch(context.Background(), BatchRequest{
		Requests: []BatchRequestItem{
			{CustomID: "q1", Params: MessageRequest{
				Model: "claude-haiku-4-5-20251001", MaxTokens: 512,
				Messages: []Message{{Role: "user", Content: "Q1"}},
			}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "anthropic: create batch")
}

func TestSDKClient_GetBatch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "batch_get_001")

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":                "batch_get_001",
			"type":              "message_batch",
			"processing_status": "ended",
			"results_url":       "https://api.anthropic.com/results/batch_get_001",
			"request_counts": map[string]any{
				"processing": 0,
				"succeeded":  5,
				"errored":    0,
				"canceled":   0,
				"expired":    0,
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	resp, err := client.GetBatch(context.Background(), "batch_get_001")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "batch_get_001", resp.ID)
	assert.Equal(t, "ended", resp.ProcessingStatus)
	assert.Equal(t, int64(5), resp.RequestCounts.Succeeded)
	assert.Contains(t, resp.ResultsURL, "batch_get_001")
}

func TestSDKClient_GetBatch_Error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "not_found_error",
				"message": "Batch not found",
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	_, err := client.GetBatch(context.Background(), "batch_nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "anthropic: get batch")
}

func TestSDKClient_GetBatchResults(t *testing.T) {
	// The SDK's ResultsStreaming expects JSONL from the results endpoint.
	jsonl := `{"custom_id":"q1","result":{"type":"succeeded","message":{"id":"msg_r1","type":"message","role":"assistant","content":[{"type":"text","text":"Answer 1"}],"model":"claude-haiku-4-5-20251001","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}}` + "\n" +
		`{"custom_id":"q2","result":{"type":"succeeded","message":{"id":"msg_r2","type":"message","role":"assistant","content":[{"type":"text","text":"Answer 2"}],"model":"claude-haiku-4-5-20251001","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}}` + "\n"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "batch_results_001")
		w.Header().Set("Content-Type", "application/x-jsonlines")
		_, _ = w.Write([]byte(jsonl)) //nolint:errcheck
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	iter, err := client.GetBatchResults(context.Background(), "batch_results_001")
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close() //nolint:errcheck

	var items []BatchResultItem
	for iter.Next() {
		items = append(items, iter.Item())
	}
	require.NoError(t, iter.Err())
	require.Len(t, items, 2)

	assert.Equal(t, "q1", items[0].CustomID)
	assert.Equal(t, "succeeded", items[0].Type)
	require.NotNil(t, items[0].Message)
	assert.Equal(t, "Answer 1", items[0].Message.Content[0].Text)

	assert.Equal(t, "q2", items[1].CustomID)
	assert.Equal(t, "Answer 2", items[1].Message.Content[0].Text)
}

func TestSDKClient_GetBatchResults_Error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "not_found_error",
				"message": "Batch not found",
			},
		})
	}))
	defer ts.Close()

	client := newTestClient(ts.URL)
	_, err := client.GetBatchResults(context.Background(), "batch_nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "anthropic: get batch results")
}
