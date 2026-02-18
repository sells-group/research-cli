package anthropic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockClient implements Client for testing.
type MockClient struct {
	mock.Mock
}

func (m *MockClient) CreateMessage(ctx context.Context, req MessageRequest) (*MessageResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*MessageResponse), args.Error(1)
}

func (m *MockClient) CreateBatch(ctx context.Context, req BatchRequest) (*BatchResponse, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*BatchResponse), args.Error(1)
}

func (m *MockClient) GetBatch(ctx context.Context, batchID string) (*BatchResponse, error) {
	args := m.Called(ctx, batchID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*BatchResponse), args.Error(1)
}

func (m *MockClient) GetBatchResults(ctx context.Context, batchID string) (BatchResultIterator, error) {
	args := m.Called(ctx, batchID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(BatchResultIterator), args.Error(1)
}

// MockBatchResultIterator implements BatchResultIterator for testing.
type MockBatchResultIterator struct {
	mock.Mock
	items []BatchResultItem
	idx   int
	err   error
}

// NewMockBatchResultIterator creates an iterator that yields the given items.
func NewMockBatchResultIterator(items []BatchResultItem) *MockBatchResultIterator {
	return &MockBatchResultIterator{
		items: items,
		idx:   -1,
	}
}

// NewMockBatchResultIteratorWithError creates an iterator that fails after
// yielding the given items.
func NewMockBatchResultIteratorWithError(items []BatchResultItem, err error) *MockBatchResultIterator {
	return &MockBatchResultIterator{
		items: items,
		idx:   -1,
		err:   err,
	}
}

func (m *MockBatchResultIterator) Next() bool {
	if m.idx+1 < len(m.items) {
		m.idx++
		return true
	}
	return false
}

func (m *MockBatchResultIterator) Item() BatchResultItem {
	return m.items[m.idx]
}

func (m *MockBatchResultIterator) Err() error {
	if m.idx+1 >= len(m.items) {
		return m.err
	}
	return nil
}

func (m *MockBatchResultIterator) Close() error {
	return nil
}

func TestCreateMessage_MockClient(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	req := MessageRequest{
		Model:     "claude-sonnet-4-5-20250929",
		MaxTokens: 1024,
		Messages: []Message{
			{Role: "user", Content: "Hello"},
		},
	}

	expected := &MessageResponse{
		ID:         "msg_123",
		Model:      "claude-sonnet-4-5-20250929",
		Content:    []ContentBlock{{Type: "text", Text: "Hi there!"}},
		StopReason: "end_turn",
		Usage: TokenUsage{
			InputTokens:  10,
			OutputTokens: 5,
		},
	}

	mc.On("CreateMessage", ctx, req).Return(expected, nil)

	resp, err := mc.CreateMessage(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "msg_123", resp.ID)
	assert.Equal(t, "Hi there!", resp.Content[0].Text)
	assert.Equal(t, int64(10), resp.Usage.InputTokens)
	assert.Equal(t, int64(5), resp.Usage.OutputTokens)

	mc.AssertExpectations(t)
}

func TestCreateBatch_MockClient(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	req := BatchRequest{
		Requests: []BatchRequestItem{
			{
				CustomID: "q1",
				Params: MessageRequest{
					Model:     "claude-haiku-4-5-20251001",
					MaxTokens: 512,
					Messages:  []Message{{Role: "user", Content: "Extract info"}},
				},
			},
			{
				CustomID: "q2",
				Params: MessageRequest{
					Model:     "claude-haiku-4-5-20251001",
					MaxTokens: 512,
					Messages:  []Message{{Role: "user", Content: "More info"}},
				},
			},
		},
	}

	expected := &BatchResponse{
		ID:               "batch_abc",
		ProcessingStatus: "in_progress",
		RequestCounts: RequestCounts{
			Processing: 2,
		},
	}

	mc.On("CreateBatch", ctx, req).Return(expected, nil)

	resp, err := mc.CreateBatch(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "batch_abc", resp.ID)
	assert.Equal(t, "in_progress", resp.ProcessingStatus)
	assert.Equal(t, int64(2), resp.RequestCounts.Processing)

	mc.AssertExpectations(t)
}

func TestGetBatch_MockClient(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	expected := &BatchResponse{
		ID:               "batch_abc",
		ProcessingStatus: "ended",
		RequestCounts: RequestCounts{
			Succeeded: 2,
		},
	}

	mc.On("GetBatch", ctx, "batch_abc").Return(expected, nil)

	resp, err := mc.GetBatch(ctx, "batch_abc")
	require.NoError(t, err)
	assert.Equal(t, "ended", resp.ProcessingStatus)
	assert.Equal(t, int64(2), resp.RequestCounts.Succeeded)

	mc.AssertExpectations(t)
}

func TestGetBatchResults_MockClient(t *testing.T) {
	mc := new(MockClient)
	ctx := context.Background()

	items := []BatchResultItem{
		{
			CustomID: "q1",
			Type:     "succeeded",
			Message: &MessageResponse{
				ID:      "msg_1",
				Content: []ContentBlock{{Type: "text", Text: "Answer 1"}},
			},
		},
		{
			CustomID: "q2",
			Type:     "succeeded",
			Message: &MessageResponse{
				ID:      "msg_2",
				Content: []ContentBlock{{Type: "text", Text: "Answer 2"}},
			},
		},
	}

	iter := NewMockBatchResultIterator(items)
	mc.On("GetBatchResults", ctx, "batch_abc").Return(iter, nil)

	result, err := mc.GetBatchResults(ctx, "batch_abc")
	require.NoError(t, err)

	var collected []BatchResultItem
	for result.Next() {
		collected = append(collected, result.Item())
	}
	require.NoError(t, result.Err())
	assert.Len(t, collected, 2)
	assert.Equal(t, "q1", collected[0].CustomID)
	assert.Equal(t, "q2", collected[1].CustomID)

	mc.AssertExpectations(t)
}

func TestSDKTypeConversion_toSDKMessages(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "Hello"},
		{Role: "assistant", Content: "Hi there"},
	}

	sdkMsgs := toSDKMessages(msgs)
	require.Len(t, sdkMsgs, 2)
}

func TestSDKTypeConversion_toSDKSystemBlocks(t *testing.T) {
	blocks := []SystemBlock{
		{Text: "You are a helpful assistant."},
		{Text: "Context data here.", CacheControl: &CacheControl{TTL: "1h"}},
	}

	sdkBlocks := toSDKSystemBlocks(blocks)
	require.Len(t, sdkBlocks, 2)
	assert.Equal(t, "You are a helpful assistant.", sdkBlocks[0].Text)
	assert.Equal(t, "Context data here.", sdkBlocks[1].Text)
}

func TestSDKTypeConversion_fromSDKBatchResult(t *testing.T) {
	// Test the type constants
	item := BatchResultItem{
		CustomID: "test",
		Type:     "succeeded",
		Message: &MessageResponse{
			ID:         "msg_abc",
			Content:    []ContentBlock{{Type: "text", Text: "result"}},
			StopReason: "end_turn",
		},
	}

	assert.Equal(t, "test", item.CustomID)
	assert.Equal(t, "succeeded", item.Type)
	assert.Equal(t, "result", item.Message.Content[0].Text)
}

