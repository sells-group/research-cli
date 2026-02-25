package salesforce

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// mockClient implements Client for testing.
type mockClient struct {
	queryFn            func(ctx context.Context, soql string, out any) error
	insertOneFn        func(ctx context.Context, sObjectName string, record map[string]any) (string, error)
	insertCollectionFn func(ctx context.Context, sObjectName string, records []map[string]any) ([]CollectionResult, error)
	updateOneFn        func(ctx context.Context, sObjectName string, id string, fields map[string]any) error
	updateCollectionFn func(ctx context.Context, sObjectName string, records []CollectionRecord) ([]CollectionResult, error)
	describeSObjectFn  func(ctx context.Context, name string) (*SObjectDescription, error)
}

func (m *mockClient) Query(ctx context.Context, soql string, out any) error {
	if m.queryFn != nil {
		return m.queryFn(ctx, soql, out)
	}
	return nil
}

func (m *mockClient) InsertOne(ctx context.Context, sObjectName string, record map[string]any) (string, error) {
	if m.insertOneFn != nil {
		return m.insertOneFn(ctx, sObjectName, record)
	}
	return "001000000000001", nil
}

func (m *mockClient) InsertCollection(ctx context.Context, sObjectName string, records []map[string]any) ([]CollectionResult, error) {
	if m.insertCollectionFn != nil {
		return m.insertCollectionFn(ctx, sObjectName, records)
	}
	results := make([]CollectionResult, len(records))
	for i := range records {
		results[i] = CollectionResult{ID: "001" + string(rune('A'+i)), Success: true}
	}
	return results, nil
}

func (m *mockClient) UpdateOne(ctx context.Context, sObjectName string, id string, fields map[string]any) error {
	if m.updateOneFn != nil {
		return m.updateOneFn(ctx, sObjectName, id, fields)
	}
	return nil
}

func (m *mockClient) UpdateCollection(ctx context.Context, sObjectName string, records []CollectionRecord) ([]CollectionResult, error) {
	if m.updateCollectionFn != nil {
		return m.updateCollectionFn(ctx, sObjectName, records)
	}
	results := make([]CollectionResult, len(records))
	for i, r := range records {
		results[i] = CollectionResult{ID: r.ID, Success: true}
	}
	return results, nil
}

func (m *mockClient) DescribeSObject(ctx context.Context, name string) (*SObjectDescription, error) {
	if m.describeSObjectFn != nil {
		return m.describeSObjectFn(ctx, name)
	}
	return &SObjectDescription{Name: name, Label: name}, nil
}

func TestMockClientImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ Client = (*mockClient)(nil)
}

func TestNewClientReturnsClient(t *testing.T) {
	// Verify the type satisfies the interface.
	var _ Client = (*sfClient)(nil)

	// NewClient wraps a salesforce.Salesforce instance.
	client := NewClient(nil)
	require.NotNil(t, client)
	var _ Client = client //nolint:staticcheck // interface compliance check
}

func TestWithRateLimit(t *testing.T) {
	t.Run("sets limiter", func(t *testing.T) {
		c := NewClient(nil, WithRateLimit(10)).(*sfClient)
		require.NotNil(t, c.limiter)
		assert.Equal(t, rate.Limit(10), c.limiter.Limit())
		assert.Equal(t, 10, c.limiter.Burst())
	})

	t.Run("zero rate skips limiter", func(t *testing.T) {
		c := NewClient(nil, WithRateLimit(0)).(*sfClient)
		assert.Nil(t, c.limiter)
	})

	t.Run("negative rate skips limiter", func(t *testing.T) {
		c := NewClient(nil, WithRateLimit(-5)).(*sfClient)
		assert.Nil(t, c.limiter)
	})

	t.Run("no option means no limiter", func(t *testing.T) {
		c := NewClient(nil).(*sfClient)
		assert.Nil(t, c.limiter)
	})

	t.Run("fractional rate gets burst of 1", func(t *testing.T) {
		c := NewClient(nil, WithRateLimit(0.5)).(*sfClient)
		require.NotNil(t, c.limiter)
		assert.Equal(t, 1, c.limiter.Burst())
	})
}

func TestRateLimiter_CancelledContext(t *testing.T) {
	// Create a limiter with zero burst so Wait always blocks.
	c := &sfClient{
		limiter: rate.NewLimiter(rate.Every(time.Hour), 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := c.wait(ctx)
	assert.Error(t, err)
}

func TestCollectionResultFields(t *testing.T) {
	r := CollectionResult{
		ID:      "001xx",
		Success: false,
		Errors:  []string{"field required"},
	}
	assert.Equal(t, "001xx", r.ID)
	assert.False(t, r.Success)
	require.Len(t, r.Errors, 1)
	assert.Equal(t, "field required", r.Errors[0])
}
