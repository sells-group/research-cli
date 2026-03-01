package geoscraper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// mockScraper implements GeoScraper for testing.
type mockScraper struct {
	name     string
	table    string
	category Category
	cadence  Cadence
	run      bool // ShouldRun return value
}

func (m *mockScraper) Name() string       { return m.name }
func (m *mockScraper) Table() string      { return m.table }
func (m *mockScraper) Category() Category { return m.category }
func (m *mockScraper) Cadence() Cadence   { return m.cadence }
func (m *mockScraper) ShouldRun(_ time.Time, _ *time.Time) bool {
	return m.run
}
func (m *mockScraper) Sync(_ context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	return &SyncResult{RowsSynced: 42}, nil
}

// mockStateScraper implements StateScraper for testing.
type mockStateScraper struct {
	mockScraper
	states []string
}

func (m *mockStateScraper) States() []string { return m.states }

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	s := &mockScraper{name: "hifld", table: "geo.infrastructure", category: National, cadence: Monthly}
	reg.Register(s)

	got, err := reg.Get("hifld")
	require.NoError(t, err)
	assert.Equal(t, "hifld", got.Name())
}

func TestRegistry_Get_NotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Get("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown scraper")
}

func TestRegistry_All_PreservesOrder(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockScraper{name: "alpha"})
	reg.Register(&mockScraper{name: "beta"})
	reg.Register(&mockScraper{name: "gamma"})

	names := reg.AllNames()
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, names)
}

func TestRegistry_ByCategory(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockScraper{name: "nat1", category: National})
	reg.Register(&mockScraper{name: "state1", category: State})
	reg.Register(&mockScraper{name: "nat2", category: National})

	nationals := reg.ByCategory(National)
	require.Len(t, nationals, 2)
	assert.Equal(t, "nat1", nationals[0].Name())
	assert.Equal(t, "nat2", nationals[1].Name())
}

func TestRegistry_Select_ByNames(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockScraper{name: "a", category: National})
	reg.Register(&mockScraper{name: "b", category: State})
	reg.Register(&mockScraper{name: "c", category: National})

	result, err := reg.Select(nil, []string{"a", "c"}, nil)
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, "a", result[0].Name())
	assert.Equal(t, "c", result[1].Name())
}

func TestRegistry_Select_ByCategory(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockScraper{name: "a", category: National})
	reg.Register(&mockScraper{name: "b", category: State})

	cat := State
	result, err := reg.Select(&cat, nil, nil)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "b", result[0].Name())
}

func TestRegistry_Select_ByNameAndCategory(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockScraper{name: "a", category: National})
	reg.Register(&mockScraper{name: "b", category: State})

	cat := National
	result, err := reg.Select(&cat, []string{"a", "b"}, nil)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "a", result[0].Name())
}

func TestRegistry_Select_ByStates(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockStateScraper{
		mockScraper: mockScraper{name: "tx_sos", category: State},
		states:      []string{"48"},
	})
	reg.Register(&mockStateScraper{
		mockScraper: mockScraper{name: "fl_sos", category: State},
		states:      []string{"12"},
	})
	reg.Register(&mockScraper{name: "hifld", category: National})

	result, err := reg.Select(nil, nil, []string{"48"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "tx_sos", result[0].Name())
}

func TestRegistry_Select_UnknownName(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Select(nil, []string{"nonexistent"}, nil)
	require.Error(t, err)
}

func TestRegistry_Select_All(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&mockScraper{name: "a"})
	reg.Register(&mockScraper{name: "b"})

	result, err := reg.Select(nil, nil, nil)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}
