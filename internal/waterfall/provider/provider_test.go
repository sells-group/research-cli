package provider

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockProvider implements Provider for testing.
type mockProvider struct {
	name            string
	supportedFields []string
}

func (m *mockProvider) Name() string              { return m.name }
func (m *mockProvider) SupportedFields() []string  { return m.supportedFields }
func (m *mockProvider) CostPerQuery(_ []string) float64 { return 0.10 }
func (m *mockProvider) CanProvide(fieldKey string) bool {
	for _, f := range m.supportedFields {
		if f == fieldKey {
			return true
		}
	}
	return false
}
func (m *mockProvider) Query(_ context.Context, _ CompanyIdentifier, _ []string) (*QueryResult, error) {
	now := time.Now()
	return &QueryResult{
		Provider: m.name,
		Fields:   []FieldResult{{FieldKey: "test", Value: "val", Confidence: 0.9, DataAsOf: &now}},
		CostUSD:  0.10,
	}, nil
}

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	assert.NotNil(t, r)
	assert.Empty(t, r.List())
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	p := &mockProvider{name: "clearbit", supportedFields: []string{"employee_count"}}
	r.Register(p)

	got := r.Get("clearbit")
	assert.NotNil(t, got)
	assert.Equal(t, "clearbit", got.Name())
}

func TestRegistry_Get_NotFound(t *testing.T) {
	r := NewRegistry()
	got := r.Get("nonexistent")
	assert.Nil(t, got)
}

func TestRegistry_List(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockProvider{name: "clearbit"})
	r.Register(&mockProvider{name: "zoominfo"})

	names := r.List()
	assert.Len(t, names, 2)
	assert.Contains(t, names, "clearbit")
	assert.Contains(t, names, "zoominfo")
}

func TestRegistry_Register_Overwrites(t *testing.T) {
	r := NewRegistry()
	p1 := &mockProvider{name: "clearbit", supportedFields: []string{"a"}}
	p2 := &mockProvider{name: "clearbit", supportedFields: []string{"a", "b"}}

	r.Register(p1)
	r.Register(p2)

	got := r.Get("clearbit")
	assert.Equal(t, []string{"a", "b"}, got.SupportedFields())
	assert.Len(t, r.List(), 1)
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := NewRegistry()

	var wg sync.WaitGroup
	// Concurrent writes.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r.Register(&mockProvider{name: "provider"})
		}(i)
	}
	// Concurrent reads.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Get("provider")
			_ = r.List()
		}()
	}
	wg.Wait()

	// Should have exactly one provider (all registered with same name).
	assert.Len(t, r.List(), 1)
}
