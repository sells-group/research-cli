package anthropic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBuildCachedSystemBlocks(t *testing.T) {
	text := "You are a research analyst. Here is the company data:\n\n# Company: Acme Corp\n..."

	blocks := BuildCachedSystemBlocks(text)

	require.Len(t, blocks, 1)
	assert.Equal(t, text, blocks[0].Text)
	require.NotNil(t, blocks[0].CacheControl)
	assert.Equal(t, "1h", blocks[0].CacheControl.TTL)
}

func TestBuildCachedSystemBlocks_EmptyText(t *testing.T) {
	blocks := BuildCachedSystemBlocks("")

	require.Len(t, blocks, 1)
	assert.Equal(t, "", blocks[0].Text)
	require.NotNil(t, blocks[0].CacheControl)
	assert.Equal(t, "1h", blocks[0].CacheControl.TTL)
}

func TestPrimerRequest_Success(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	systemBlocks := BuildCachedSystemBlocks("Research context data for company X...")

	req := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 128,
		System:    systemBlocks,
		Messages: []Message{
			{Role: "user", Content: "Acknowledge receipt of the context."},
		},
	}

	expected := &MessageResponse{
		ID:         "msg_primer",
		Model:      "claude-sonnet-4-5-20250929",
		Content:    []ContentBlock{{Type: "text", Text: "Acknowledged."}},
		StopReason: "end_turn",
		Usage: TokenUsage{
			InputTokens:              100,
			OutputTokens:             5,
			CacheCreationInputTokens: 8000,
			CacheReadInputTokens:     0,
		},
	}

	mc.On("CreateMessage", ctx, req).Return(expected, nil)

	resp, err := PrimerRequest(ctx, mc, req)
	require.NoError(t, err)
	assert.Equal(t, "msg_primer", resp.ID)
	assert.Equal(t, int64(8000), resp.Usage.CacheCreationInputTokens)

	mc.AssertExpectations(t)
}

func TestPrimerRequest_Error(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	req := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 128,
		System:    BuildCachedSystemBlocks("Context"),
		Messages:  []Message{{Role: "user", Content: "Ack."}},
	}

	mc.On("CreateMessage", ctx, req).Return(nil, fmt.Errorf("rate limited"))

	_, err := PrimerRequest(ctx, mc, req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primer request")
	assert.Contains(t, err.Error(), "rate limited")

	mc.AssertExpectations(t)
}

func TestPrimerRequest_CacheHitOnSecondCall(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	systemBlocks := BuildCachedSystemBlocks("Large context data (~25K tokens)...")

	// First call: cache creation
	req1 := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 128,
		System:    systemBlocks,
		Messages:  []Message{{Role: "user", Content: "Question 1?"}},
	}
	mc.On("CreateMessage", ctx, req1).Return(&MessageResponse{
		ID:         "msg_1",
		Content:    []ContentBlock{{Type: "text", Text: "Answer 1"}},
		StopReason: "end_turn",
		Usage: TokenUsage{
			InputTokens:              100,
			CacheCreationInputTokens: 25000,
			CacheReadInputTokens:     0,
		},
	}, nil)

	// Second call: cache hit
	req2 := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 128,
		System:    systemBlocks,
		Messages:  []Message{{Role: "user", Content: "Question 2?"}},
	}
	mc.On("CreateMessage", ctx, req2).Return(&MessageResponse{
		ID:         "msg_2",
		Content:    []ContentBlock{{Type: "text", Text: "Answer 2"}},
		StopReason: "end_turn",
		Usage: TokenUsage{
			InputTokens:              100,
			CacheCreationInputTokens: 0,
			CacheReadInputTokens:     25000,
		},
	}, nil)

	// First: primer warms cache
	resp1, err := PrimerRequest(ctx, mc, req1)
	require.NoError(t, err)
	assert.Equal(t, int64(25000), resp1.Usage.CacheCreationInputTokens)
	assert.Equal(t, int64(0), resp1.Usage.CacheReadInputTokens)

	// Second: subsequent request hits cache
	resp2, err := mc.CreateMessage(ctx, req2)
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp2.Usage.CacheCreationInputTokens)
	assert.Equal(t, int64(25000), resp2.Usage.CacheReadInputTokens)

	mc.AssertExpectations(t)
}

func TestPrimerRequest_WithBatchWorkflow(t *testing.T) {
	// Tests the full primer + batch workflow pattern:
	// 1. Build cached system blocks
	// 2. Send primer request to warm cache
	// 3. Create batch with same system blocks
	// 4. Poll batch to completion
	// 5. Collect results

	mc := new(MockClient)
	ctx := context.Background()

	systemBlocks := BuildCachedSystemBlocks("Company research data...")

	// Step 2: Primer request
	primerReq := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 128,
		System:    systemBlocks,
		Messages:  []Message{{Role: "user", Content: "Ack."}},
	}
	mc.On("CreateMessage", ctx, primerReq).Return(&MessageResponse{
		ID:         "msg_primer",
		StopReason: "end_turn",
		Usage:      TokenUsage{CacheCreationInputTokens: 10000},
	}, nil)

	// Step 3: Create batch
	batchReq := BatchRequest{
		Requests: []BatchRequestItem{
			{CustomID: "q1", Params: MessageRequest{
				Model: "claude-sonnet-4-5-20250929", MaxTokens: 1024,
				System:   systemBlocks,
				Messages: []Message{{Role: "user", Content: "Q1?"}},
			}},
			{CustomID: "q2", Params: MessageRequest{
				Model: "claude-sonnet-4-5-20250929", MaxTokens: 1024,
				System:   systemBlocks,
				Messages: []Message{{Role: "user", Content: "Q2?"}},
			}},
		},
	}
	mc.On("CreateBatch", ctx, batchReq).Return(&BatchResponse{
		ID:               "batch_001",
		ProcessingStatus: "in_progress",
	}, nil)

	// Step 4: Poll returns ended (mock.Anything for ctx because PollBatch wraps it)
	mc.On("GetBatch", mock.Anything, "batch_001").Return(&BatchResponse{
		ID:               "batch_001",
		ProcessingStatus: "ended",
		RequestCounts:    RequestCounts{Succeeded: 2},
	}, nil)

	// Step 5: Get results
	resultItems := []BatchResultItem{
		{CustomID: "q1", Type: "succeeded", Message: &MessageResponse{
			ID: "msg_r1", Content: []ContentBlock{{Type: "text", Text: "A1"}},
			Usage: TokenUsage{CacheReadInputTokens: 10000},
		}},
		{CustomID: "q2", Type: "succeeded", Message: &MessageResponse{
			ID: "msg_r2", Content: []ContentBlock{{Type: "text", Text: "A2"}},
			Usage: TokenUsage{CacheReadInputTokens: 10000},
		}},
	}
	mc.On("GetBatchResults", ctx, "batch_001").Return(
		NewMockBatchResultIterator(resultItems), nil,
	)

	// Execute workflow
	resp, err := PrimerRequest(ctx, mc, primerReq)
	require.NoError(t, err)
	assert.Equal(t, int64(10000), resp.Usage.CacheCreationInputTokens)

	batchResp, err := mc.CreateBatch(ctx, batchReq)
	require.NoError(t, err)

	polled, err := PollBatch(ctx, mc, batchResp.ID,
		WithPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "ended", polled.ProcessingStatus)

	iter, err := mc.GetBatchResults(ctx, "batch_001")
	require.NoError(t, err)

	results, err := CollectBatchResults(iter)
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "A1", results["q1"].Content[0].Text)
	assert.Equal(t, "A2", results["q2"].Content[0].Text)

	// Both batch results should show cache reads
	assert.Equal(t, int64(10000), results["q1"].Usage.CacheReadInputTokens)
	assert.Equal(t, int64(10000), results["q2"].Usage.CacheReadInputTokens)

	mc.AssertExpectations(t)
}

func TestPollBatch_ContextCancelled(t *testing.T) {
	mc := new(MockClient)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	mc.On("GetBatch", mock.Anything, "batch_cancel").Return(nil, context.Canceled)

	_, err := PollBatch(ctx, mc, "batch_cancel",
		WithPollInterval(10*time.Millisecond),
	)
	require.Error(t, err)
}
