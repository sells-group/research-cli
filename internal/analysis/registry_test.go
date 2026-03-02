package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	a := &mockAnalyzer{name: "a1", category: Spatial}
	r.Register(a)

	got, err := r.Get("a1")
	require.NoError(t, err)
	assert.Equal(t, "a1", got.Name())
}

func TestRegistry_Get_NotFound(t *testing.T) {
	r := NewRegistry()
	_, err := r.Get("missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown analyzer")
}

func TestRegistry_All_PreservesOrder(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "c", category: Spatial})
	r.Register(&mockAnalyzer{name: "a", category: Scoring})
	r.Register(&mockAnalyzer{name: "b", category: Correlation})

	all := r.All()
	require.Len(t, all, 3)
	assert.Equal(t, "c", all[0].Name())
	assert.Equal(t, "a", all[1].Name())
	assert.Equal(t, "b", all[2].Name())
}

func TestRegistry_AllNames(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "x"})
	r.Register(&mockAnalyzer{name: "y"})

	names := r.AllNames()
	assert.Equal(t, []string{"x", "y"}, names)

	// Verify returned slice is a copy.
	names[0] = "z"
	assert.Equal(t, []string{"x", "y"}, r.AllNames())
}

func TestRegistry_ByCategory(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "s1", category: Spatial})
	r.Register(&mockAnalyzer{name: "sc1", category: Scoring})
	r.Register(&mockAnalyzer{name: "s2", category: Spatial})

	spatial := r.ByCategory(Spatial)
	require.Len(t, spatial, 2)
	assert.Equal(t, "s1", spatial[0].Name())
	assert.Equal(t, "s2", spatial[1].Name())

	ranking := r.ByCategory(Ranking)
	assert.Empty(t, ranking)
}

// --- Validate ---

func TestRegistry_Validate_Success(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", category: Spatial})
	r.Register(&mockAnalyzer{name: "b", category: Scoring, deps: []string{"a"}})
	r.Register(&mockAnalyzer{name: "c", category: Ranking, deps: []string{"b"}})

	assert.NoError(t, r.Validate())
}

func TestRegistry_Validate_MissingDep(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", category: Spatial, deps: []string{"missing"}})

	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "depends on unknown analyzer")
}

func TestRegistry_Validate_Cycle(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", deps: []string{"b"}})
	r.Register(&mockAnalyzer{name: "b", deps: []string{"a"}})

	err := r.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dependency cycle")
}

// --- TopoSort ---

func TestRegistry_TopoSort_NoDeps(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a"})
	r.Register(&mockAnalyzer{name: "b"})
	r.Register(&mockAnalyzer{name: "c"})

	sorted, err := r.TopoSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)
	// With no deps, registration order is preserved.
	assert.Equal(t, "a", sorted[0].Name())
	assert.Equal(t, "b", sorted[1].Name())
	assert.Equal(t, "c", sorted[2].Name())
}

func TestRegistry_TopoSort_LinearChain(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "c", deps: []string{"b"}})
	r.Register(&mockAnalyzer{name: "b", deps: []string{"a"}})
	r.Register(&mockAnalyzer{name: "a"})

	sorted, err := r.TopoSort()
	require.NoError(t, err)
	require.Len(t, sorted, 3)
	assert.Equal(t, "a", sorted[0].Name())
	assert.Equal(t, "b", sorted[1].Name())
	assert.Equal(t, "c", sorted[2].Name())
}

func TestRegistry_TopoSort_Diamond(t *testing.T) {
	// A -> B, A -> C, B -> D, C -> D
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a"})
	r.Register(&mockAnalyzer{name: "b", deps: []string{"a"}})
	r.Register(&mockAnalyzer{name: "c", deps: []string{"a"}})
	r.Register(&mockAnalyzer{name: "d", deps: []string{"b", "c"}})

	sorted, err := r.TopoSort()
	require.NoError(t, err)
	require.Len(t, sorted, 4)

	// A must be first, D must be last.
	assert.Equal(t, "a", sorted[0].Name())
	assert.Equal(t, "d", sorted[3].Name())

	// B and C can be in either order.
	mid := []string{sorted[1].Name(), sorted[2].Name()}
	assert.Contains(t, mid, "b")
	assert.Contains(t, mid, "c")
}

func TestRegistry_TopoSort_Cycle(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "x", deps: []string{"y"}})
	r.Register(&mockAnalyzer{name: "y", deps: []string{"x"}})

	_, err := r.TopoSort()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dependency cycle")
}

// --- Select ---

func TestRegistry_Select_All(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", category: Spatial})
	r.Register(&mockAnalyzer{name: "b", category: Scoring, deps: []string{"a"}})

	selected, err := r.Select(nil, nil)
	require.NoError(t, err)
	require.Len(t, selected, 2)
	assert.Equal(t, "a", selected[0].Name())
	assert.Equal(t, "b", selected[1].Name())
}

func TestRegistry_Select_ByCategory(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", category: Spatial})
	r.Register(&mockAnalyzer{name: "b", category: Scoring})
	r.Register(&mockAnalyzer{name: "c", category: Spatial})

	cat := Spatial
	selected, err := r.Select(&cat, nil)
	require.NoError(t, err)
	require.Len(t, selected, 2)
	assert.Equal(t, "a", selected[0].Name())
	assert.Equal(t, "c", selected[1].Name())
}

func TestRegistry_Select_ByNames(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", category: Spatial})
	r.Register(&mockAnalyzer{name: "b", category: Scoring})
	r.Register(&mockAnalyzer{name: "c", category: Ranking})

	selected, err := r.Select(nil, []string{"b", "c"})
	require.NoError(t, err)
	require.Len(t, selected, 2)
	assert.Equal(t, "b", selected[0].Name())
	assert.Equal(t, "c", selected[1].Name())
}

func TestRegistry_Select_ByNameIncludesDeps(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a", category: Spatial})
	r.Register(&mockAnalyzer{name: "b", category: Scoring, deps: []string{"a"}})
	r.Register(&mockAnalyzer{name: "c", category: Ranking, deps: []string{"b"}})

	// Requesting "c" should include "a" and "b" transitively.
	selected, err := r.Select(nil, []string{"c"})
	require.NoError(t, err)
	require.Len(t, selected, 3)
	assert.Equal(t, "a", selected[0].Name())
	assert.Equal(t, "b", selected[1].Name())
	assert.Equal(t, "c", selected[2].Name())
}

func TestRegistry_Select_UnknownName(t *testing.T) {
	r := NewRegistry()
	r.Register(&mockAnalyzer{name: "a"})

	_, err := r.Select(nil, []string{"missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown analyzer")
}

func TestRegistry_Select_EmptyRegistry(t *testing.T) {
	r := NewRegistry()
	selected, err := r.Select(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, selected)
}
