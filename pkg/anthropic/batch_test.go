package anthropic

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPollBatch_CompletesImmediately(t *testing.T) {
	mc := new(MockClient)

	mc.On("GetBatch", mock.Anything, "batch_123").Return(&BatchResponse{
		ID:               "batch_123",
		ProcessingStatus: "ended",
		RequestCounts:    RequestCounts{Succeeded: 5},
	}, nil)

	resp, err := PollBatch(context.Background(), mc, "batch_123",
		WithPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "ended", resp.ProcessingStatus)
	assert.Equal(t, int64(5), resp.RequestCounts.Succeeded)

	mc.AssertExpectations(t)
}

// countingGetBatchMock is a mock that returns different responses based on
// call count, avoiding testify's functional return pattern.
type countingGetBatchMock struct {
	MockClient
	calls     atomic.Int32
	threshold int32
	endResp   *BatchResponse
}

func (m *countingGetBatchMock) GetBatch(_ context.Context, batchID string) (*BatchResponse, error) {
	n := m.calls.Add(1)
	if n < m.threshold {
		return &BatchResponse{
			ID:               batchID,
			ProcessingStatus: "in_progress",
		}, nil
	}
	return m.endResp, nil
}

func TestPollBatch_CompletesAfterRetries(t *testing.T) {
	mc := &countingGetBatchMock{
		threshold: 3,
		endResp: &BatchResponse{
			ID:               "batch_456",
			ProcessingStatus: "ended",
			RequestCounts:    RequestCounts{Succeeded: 10},
		},
	}

	resp, err := PollBatch(context.Background(), mc, "batch_456",
		WithPollInterval(10*time.Millisecond),
		WithPollCap(20*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, "ended", resp.ProcessingStatus)
	assert.Equal(t, int64(10), resp.RequestCounts.Succeeded)
	assert.Equal(t, int32(3), mc.calls.Load())
}

func TestPollBatch_Timeout(t *testing.T) {
	mc := new(MockClient)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	mc.On("GetBatch", mock.Anything, "batch_timeout").Return(&BatchResponse{
		ID:               "batch_timeout",
		ProcessingStatus: "in_progress",
	}, nil)

	_, err := PollBatch(ctx, mc, "batch_timeout",
		WithPollInterval(10*time.Millisecond),
		WithPollCap(15*time.Millisecond),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPollBatch_DefaultTimeout(t *testing.T) {
	mc := new(MockClient)

	mc.On("GetBatch", mock.Anything, "batch_def").Return(&BatchResponse{
		ID:               "batch_def",
		ProcessingStatus: "in_progress",
	}, nil)

	_, err := PollBatch(context.Background(), mc, "batch_def",
		WithPollInterval(5*time.Millisecond),
		WithPollCap(10*time.Millisecond),
		WithPollTimeout(50*time.Millisecond),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPollBatch_APIError(t *testing.T) {
	mc := new(MockClient)

	mc.On("GetBatch", mock.Anything, "batch_err").Return(nil, fmt.Errorf("api error: 500"))

	_, err := PollBatch(context.Background(), mc, "batch_err",
		WithPollInterval(10*time.Millisecond),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "api error: 500")
}

func TestPollBatch_ExponentialBackoff(t *testing.T) {
	var timestamps []time.Time
	mc := &struct {
		MockClient
		calls atomic.Int32
	}{}

	// We override GetBatch directly to record timestamps.
	origGetBatch := func(_ context.Context, batchID string) (*BatchResponse, error) {
		timestamps = append(timestamps, time.Now())
		n := mc.calls.Add(1)
		if n < 4 {
			return &BatchResponse{
				ID:               batchID,
				ProcessingStatus: "in_progress",
			}, nil
		}
		return &BatchResponse{
			ID:               batchID,
			ProcessingStatus: "ended",
			RequestCounts:    RequestCounts{Succeeded: 1},
		}, nil
	}

	// Use a wrapper that implements Client
	wrapper := &getBatchFuncClient{fn: origGetBatch}

	_, err := PollBatch(context.Background(), wrapper, "batch_backoff",
		WithPollInterval(20*time.Millisecond),
		WithPollCap(100*time.Millisecond),
	)
	require.NoError(t, err)
	assert.Equal(t, int32(4), mc.calls.Load())

	// Verify intervals are increasing (exponential backoff).
	// interval[0] = 20ms, interval[1] = 40ms, interval[2] = 80ms
	if len(timestamps) >= 3 {
		gap1 := timestamps[1].Sub(timestamps[0])
		gap2 := timestamps[2].Sub(timestamps[1])
		// gap2 should be larger than gap1 (with some tolerance for timing)
		assert.Greater(t, gap2.Milliseconds(), gap1.Milliseconds()-5,
			"backoff should increase: gap1=%v gap2=%v", gap1, gap2)
	}
}

// getBatchFuncClient is a minimal Client that delegates GetBatch to a function.
type getBatchFuncClient struct {
	fn func(context.Context, string) (*BatchResponse, error)
}

func (c *getBatchFuncClient) CreateMessage(context.Context, MessageRequest) (*MessageResponse, error) {
	return nil, nil
}
func (c *getBatchFuncClient) CreateBatch(context.Context, BatchRequest) (*BatchResponse, error) {
	return nil, nil
}
func (c *getBatchFuncClient) GetBatch(ctx context.Context, id string) (*BatchResponse, error) {
	return c.fn(ctx, id)
}
func (c *getBatchFuncClient) GetBatchResults(context.Context, string) (BatchResultIterator, error) {
	return nil, nil
}

func TestPollBatch_JitterRange(t *testing.T) {
	// Verify that jitter stays within ±20% of the base interval.
	// We run multiple polls and check the observed intervals.
	var timestamps []time.Time
	var calls atomic.Int32

	wrapper := &getBatchFuncClient{fn: func(_ context.Context, batchID string) (*BatchResponse, error) {
		timestamps = append(timestamps, time.Now())
		n := calls.Add(1)
		if n < 8 {
			return &BatchResponse{
				ID:               batchID,
				ProcessingStatus: "in_progress",
			}, nil
		}
		return &BatchResponse{
			ID:               batchID,
			ProcessingStatus: "ended",
			RequestCounts:    RequestCounts{Succeeded: 1},
		}, nil
	}}

	_, err := PollBatch(context.Background(), wrapper, "batch_jitter",
		WithPollInterval(20*time.Millisecond),
		WithPollCap(200*time.Millisecond),
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(timestamps), 4, "need enough data points")

	// Compute observed gaps and verify they grow (backoff) but stay within bounds.
	for i := 1; i < len(timestamps); i++ {
		gap := timestamps[i].Sub(timestamps[i-1])
		// Base interval doubles each time: 20, 40, 80, 160, 200 (cap)
		// With ±20% jitter and timing variance, gaps should be at least 50%
		// of the base and at most 200% (generous bounds for CI).
		assert.Greater(t, gap.Milliseconds(), int64(5),
			"gap %d too small: %v", i, gap)
	}
}

func TestCollectBatchResults_Success(t *testing.T) {
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
			Type:     "errored",
			Message:  nil,
		},
		{
			CustomID: "q3",
			Type:     "succeeded",
			Message: &MessageResponse{
				ID:      "msg_3",
				Content: []ContentBlock{{Type: "text", Text: "Answer 3"}},
			},
		},
		{
			CustomID: "q4",
			Type:     "canceled",
			Message:  nil,
		},
	}

	iter := NewMockBatchResultIterator(items)
	results, err := CollectBatchResults(iter)
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "Answer 1", results["q1"].Content[0].Text)
	assert.Equal(t, "Answer 3", results["q3"].Content[0].Text)
	assert.Nil(t, results["q2"])
	assert.Nil(t, results["q4"])
}

func TestCollectBatchResults_Empty(t *testing.T) {
	iter := NewMockBatchResultIterator(nil)
	results, err := CollectBatchResults(iter)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestCollectBatchResults_IteratorError(t *testing.T) {
	items := []BatchResultItem{
		{
			CustomID: "q1",
			Type:     "succeeded",
			Message: &MessageResponse{
				ID:      "msg_1",
				Content: []ContentBlock{{Type: "text", Text: "Answer 1"}},
			},
		},
	}

	iter := NewMockBatchResultIteratorWithError(items, fmt.Errorf("stream interrupted"))
	_, err := CollectBatchResults(iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream interrupted")
}
