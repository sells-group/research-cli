package anthropic

import (
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromSDKMessage(t *testing.T) {
	sdkMsg := &sdk.Message{
		ID:           "msg_test_123",
		Model:        "claude-sonnet-4-5-20250929",
		StopReason:   "end_turn",
		StopSequence: "STOP",
		Content: []sdk.ContentBlockUnion{
			{Type: "text", Text: "Hello world"},
			{Type: "text", Text: "Second block"},
		},
		Usage: sdk.Usage{
			InputTokens:              100,
			OutputTokens:             50,
			CacheCreationInputTokens: 2000,
			CacheReadInputTokens:     3000,
		},
	}

	resp := fromSDKMessage(sdkMsg)
	require.NotNil(t, resp)
	assert.Equal(t, "msg_test_123", resp.ID)
	assert.Equal(t, "claude-sonnet-4-5-20250929", resp.Model)
	assert.Equal(t, "end_turn", resp.StopReason)
	assert.Equal(t, "STOP", resp.StopSequence)
	require.Len(t, resp.Content, 2)
	assert.Equal(t, "text", resp.Content[0].Type)
	assert.Equal(t, "Hello world", resp.Content[0].Text)
	assert.Equal(t, "Second block", resp.Content[1].Text)
	assert.Equal(t, int64(100), resp.Usage.InputTokens)
	assert.Equal(t, int64(50), resp.Usage.OutputTokens)
	assert.Equal(t, int64(2000), resp.Usage.CacheCreationInputTokens)
	assert.Equal(t, int64(3000), resp.Usage.CacheReadInputTokens)
}

func TestFromSDKMessage_EmptyContent(t *testing.T) {
	sdkMsg := &sdk.Message{
		ID:         "msg_empty",
		Model:      "claude-haiku-4-5-20251001",
		StopReason: "max_tokens",
	}

	resp := fromSDKMessage(sdkMsg)
	require.NotNil(t, resp)
	assert.Empty(t, resp.Content)
	assert.Equal(t, "max_tokens", resp.StopReason)
	assert.Equal(t, int64(0), resp.Usage.InputTokens)
}

func TestFromSDKBatch(t *testing.T) {
	sdkBatch := &sdk.MessageBatch{
		ID:               "batch_test_456",
		ProcessingStatus: "ended",
		ResultsURL:       "https://api.anthropic.com/results/batch_test_456",
		RequestCounts: sdk.MessageBatchRequestCounts{
			Processing: 0,
			Succeeded:  8,
			Errored:    1,
			Canceled:   0,
			Expired:    1,
		},
	}

	resp := fromSDKBatch(sdkBatch)
	require.NotNil(t, resp)
	assert.Equal(t, "batch_test_456", resp.ID)
	assert.Equal(t, "ended", resp.ProcessingStatus)
	assert.Equal(t, "https://api.anthropic.com/results/batch_test_456", resp.ResultsURL)
	assert.Equal(t, int64(0), resp.RequestCounts.Processing)
	assert.Equal(t, int64(8), resp.RequestCounts.Succeeded)
	assert.Equal(t, int64(1), resp.RequestCounts.Errored)
	assert.Equal(t, int64(0), resp.RequestCounts.Canceled)
	assert.Equal(t, int64(1), resp.RequestCounts.Expired)
}

func TestFromSDKBatch_InProgress(t *testing.T) {
	sdkBatch := &sdk.MessageBatch{
		ID:               "batch_prog",
		ProcessingStatus: "in_progress",
		RequestCounts: sdk.MessageBatchRequestCounts{
			Processing: 10,
			Succeeded:  0,
		},
	}

	resp := fromSDKBatch(sdkBatch)
	assert.Equal(t, "in_progress", resp.ProcessingStatus)
	assert.Equal(t, int64(10), resp.RequestCounts.Processing)
	assert.Equal(t, "", resp.ResultsURL)
}

func TestFromSDKBatchResult_Succeeded(t *testing.T) {
	sdkResp := sdk.MessageBatchIndividualResponse{
		CustomID: "q1",
		Result: sdk.MessageBatchResultUnion{
			Type: "succeeded",
			Message: sdk.Message{
				ID:         "msg_result_1",
				Model:      "claude-haiku-4-5-20251001",
				StopReason: "end_turn",
				Content: []sdk.ContentBlockUnion{
					{Type: "text", Text: "Extracted answer"},
				},
				Usage: sdk.Usage{
					InputTokens:  200,
					OutputTokens: 30,
				},
			},
		},
	}

	item := fromSDKBatchResult(sdkResp)
	assert.Equal(t, "q1", item.CustomID)
	assert.Equal(t, "succeeded", item.Type)
	require.NotNil(t, item.Message)
	assert.Equal(t, "msg_result_1", item.Message.ID)
	assert.Equal(t, "Extracted answer", item.Message.Content[0].Text)
	assert.Equal(t, int64(200), item.Message.Usage.InputTokens)
}

func TestFromSDKBatchResult_Errored(t *testing.T) {
	sdkResp := sdk.MessageBatchIndividualResponse{
		CustomID: "q_err",
		Result: sdk.MessageBatchResultUnion{
			Type: "errored",
		},
	}

	item := fromSDKBatchResult(sdkResp)
	assert.Equal(t, "q_err", item.CustomID)
	assert.Equal(t, "errored", item.Type)
	assert.Nil(t, item.Message)
}

func TestFromSDKBatchResult_Canceled(t *testing.T) {
	sdkResp := sdk.MessageBatchIndividualResponse{
		CustomID: "q_cancel",
		Result: sdk.MessageBatchResultUnion{
			Type: "canceled",
		},
	}

	item := fromSDKBatchResult(sdkResp)
	assert.Equal(t, "q_cancel", item.CustomID)
	assert.Equal(t, "canceled", item.Type)
	assert.Nil(t, item.Message)
}

func TestFromSDKBatchResult_Expired(t *testing.T) {
	sdkResp := sdk.MessageBatchIndividualResponse{
		CustomID: "q_exp",
		Result: sdk.MessageBatchResultUnion{
			Type: "expired",
		},
	}

	item := fromSDKBatchResult(sdkResp)
	assert.Equal(t, "q_exp", item.CustomID)
	assert.Equal(t, "expired", item.Type)
	assert.Nil(t, item.Message)
}

func TestToSDKMessages_UserRole(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "Hello"},
	}
	sdkMsgs := toSDKMessages(msgs)
	require.Len(t, sdkMsgs, 1)
}

func TestToSDKMessages_AssistantRole(t *testing.T) {
	msgs := []Message{
		{Role: "assistant", Content: "Hi there"},
	}
	sdkMsgs := toSDKMessages(msgs)
	require.Len(t, sdkMsgs, 1)
}

func TestToSDKMessages_MixedRoles(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "Question"},
		{Role: "assistant", Content: "Answer"},
		{Role: "user", Content: "Follow-up"},
	}
	sdkMsgs := toSDKMessages(msgs)
	require.Len(t, sdkMsgs, 3)
}

func TestToSDKMessages_UnknownRoleDefaultsToUser(t *testing.T) {
	msgs := []Message{
		{Role: "unknown", Content: "text"},
	}
	sdkMsgs := toSDKMessages(msgs)
	require.Len(t, sdkMsgs, 1)
}

func TestToSDKMessages_Empty(t *testing.T) {
	sdkMsgs := toSDKMessages(nil)
	assert.Empty(t, sdkMsgs)
}

func TestToSDKSystemBlocks_NoCacheControl(t *testing.T) {
	blocks := []SystemBlock{
		{Text: "System prompt text"},
	}
	sdkBlocks := toSDKSystemBlocks(blocks)
	require.Len(t, sdkBlocks, 1)
	assert.Equal(t, "System prompt text", sdkBlocks[0].Text)
}

func TestToSDKSystemBlocks_WithCacheControl(t *testing.T) {
	blocks := []SystemBlock{
		{Text: "Cached context", CacheControl: &CacheControl{TTL: "1h"}},
	}
	sdkBlocks := toSDKSystemBlocks(blocks)
	require.Len(t, sdkBlocks, 1)
	assert.Equal(t, "Cached context", sdkBlocks[0].Text)
	assert.NotNil(t, sdkBlocks[0].CacheControl)
}

func TestToSDKSystemBlocks_WithEmptyTTL(t *testing.T) {
	blocks := []SystemBlock{
		{Text: "Block", CacheControl: &CacheControl{TTL: ""}},
	}
	sdkBlocks := toSDKSystemBlocks(blocks)
	require.Len(t, sdkBlocks, 1)
	assert.NotNil(t, sdkBlocks[0].CacheControl)
}

func TestToSDKSystemBlocks_Multiple(t *testing.T) {
	blocks := []SystemBlock{
		{Text: "First block"},
		{Text: "Second block", CacheControl: &CacheControl{TTL: "5m"}},
		{Text: "Third block"},
	}
	sdkBlocks := toSDKSystemBlocks(blocks)
	require.Len(t, sdkBlocks, 3)
	assert.Equal(t, "First block", sdkBlocks[0].Text)
	assert.Equal(t, "Second block", sdkBlocks[1].Text)
	assert.Equal(t, "Third block", sdkBlocks[2].Text)
}

func TestNewClient_ReturnsNonNil(t *testing.T) {
	client := NewClient("test-api-key")
	require.NotNil(t, client)

	// Verify it implements the Client interface.
	var _ Client = client //nolint:staticcheck // interface compliance check
}

func TestMessageRequest_Fields(t *testing.T) {
	temp := 0.7
	req := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 2048,
		System: []SystemBlock{
			{Text: "System"},
		},
		Messages: []Message{
			{Role: "user", Content: "Hello"},
		},
		Temperature: &temp,
	}

	assert.Equal(t, "claude-sonnet-4-5-20250929", req.Model)
	assert.Equal(t, int64(2048), req.MaxTokens)
	assert.Len(t, req.System, 1)
	assert.Len(t, req.Messages, 1)
	assert.Equal(t, 0.7, *req.Temperature)
}

func TestBatchRequest_Fields(t *testing.T) {
	req := BatchRequest{
		Requests: []BatchRequestItem{
			{
				CustomID: "item1",
				Params: MessageRequest{
					Model:     "claude-haiku-4-5-20251001",
					MaxTokens: 512,
				},
			},
		},
	}
	require.Len(t, req.Requests, 1)
	assert.Equal(t, "item1", req.Requests[0].CustomID)
}

func TestBatchResponse_Fields(t *testing.T) {
	resp := BatchResponse{
		ID: "batch_789",
		RequestCounts: RequestCounts{
			Processing: 0,
			Succeeded:  5,
			Errored:    1,
			Canceled:   0,
			Expired:    0,
		},
	}
	assert.Equal(t, "batch_789", resp.ID)
	assert.Equal(t, int64(5), resp.RequestCounts.Succeeded)
}

func TestTokenUsage_Fields(t *testing.T) {
	usage := TokenUsage{
		InputTokens:              1000,
		OutputTokens:             500,
		CacheCreationInputTokens: 5000,
		CacheReadInputTokens:     4000,
	}
	assert.Equal(t, int64(1000), usage.InputTokens)
	assert.Equal(t, int64(500), usage.OutputTokens)
	assert.Equal(t, int64(5000), usage.CacheCreationInputTokens)
	assert.Equal(t, int64(4000), usage.CacheReadInputTokens)
}

func TestMockBatchResultIterator_Empty(t *testing.T) {
	iter := NewMockBatchResultIterator(nil)
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
	assert.NoError(t, iter.Close())
}

func TestMockBatchResultIterator_Items(t *testing.T) {
	items := []BatchResultItem{
		{CustomID: "a", Type: "succeeded"},
		{CustomID: "b", Type: "errored"},
	}
	iter := NewMockBatchResultIterator(items)

	assert.True(t, iter.Next())
	assert.Equal(t, "a", iter.Item().CustomID)
	assert.True(t, iter.Next())
	assert.Equal(t, "b", iter.Item().CustomID)
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestMockBatchResultIterator_WithError(t *testing.T) {
	items := []BatchResultItem{
		{CustomID: "a", Type: "succeeded"},
	}
	iter := NewMockBatchResultIteratorWithError(items, assert.AnError)

	assert.True(t, iter.Next())
	assert.Equal(t, "a", iter.Item().CustomID)

	// After consuming last item, Err() reports the error
	assert.False(t, iter.Next())
	assert.Equal(t, assert.AnError, iter.Err())
}

func TestContentBlock_Fields(t *testing.T) {
	b := ContentBlock{Type: "text", Text: "Hello"}
	assert.Equal(t, "text", b.Type)
	assert.Equal(t, "Hello", b.Text)
}
