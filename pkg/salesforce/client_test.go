package salesforce

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient implements Client for testing.
type mockClient struct {
	queryFn            func(ctx context.Context, soql string, out any) error
	insertOneFn        func(ctx context.Context, sObjectName string, record map[string]any) (string, error)
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
