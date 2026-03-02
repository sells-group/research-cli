package analysis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterSpatial(t *testing.T) {
	reg := NewRegistry()
	RegisterSpatial(reg)

	names := reg.AllNames()
	assert.Contains(t, names, "proximity_matrix")

	a, err := reg.Get("proximity_matrix")
	require.NoError(t, err)
	assert.Equal(t, Spatial, a.Category())
}

func TestRegisterAll(t *testing.T) {
	reg := NewRegistry()
	RegisterAll(reg)

	names := reg.AllNames()
	assert.Contains(t, names, "proximity_matrix")

	// All spatial analyzers should be in the Spatial category.
	for _, a := range reg.ByCategory(Spatial) {
		assert.Equal(t, Spatial, a.Category())
	}
}

func TestRegisterAll_NoDuplicates(t *testing.T) {
	reg := NewRegistry()
	RegisterAll(reg)

	names := reg.AllNames()
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		assert.False(t, seen[name], "duplicate analyzer name: %s", name)
		seen[name] = true
	}
}

func TestRegisterAll_ValidatePasses(t *testing.T) {
	reg := NewRegistry()
	RegisterAll(reg)

	assert.NoError(t, reg.Validate())
}
