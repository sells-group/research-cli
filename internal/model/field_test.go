package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFieldRegistry(t *testing.T) {
	t.Parallel()

	fields := []FieldMapping{
		{Key: "revenue", SFField: "AnnualRevenue", Required: true, DataType: "currency"},
		{Key: "employees", SFField: "NumberOfEmployees", Required: true, DataType: "integer"},
		{Key: "description", SFField: "Description", Required: false, DataType: "text"},
		{Key: "internal_only", SFField: "", Required: false, DataType: "text"},
	}

	reg := NewFieldRegistry(fields)

	t.Run("ByKey returns correct mapping", func(t *testing.T) {
		t.Parallel()
		f := reg.ByKey("revenue")
		require.NotNil(t, f)
		assert.Equal(t, "AnnualRevenue", f.SFField)
	})

	t.Run("ByKey returns nil for unknown key", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, reg.ByKey("nonexistent"))
	})

	t.Run("BySFName returns correct mapping", func(t *testing.T) {
		t.Parallel()
		f := reg.BySFName("NumberOfEmployees")
		require.NotNil(t, f)
		assert.Equal(t, "employees", f.Key)
	})

	t.Run("BySFName returns nil for unknown name", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, reg.BySFName("NonExistent__c"))
	})

	t.Run("BySFName skips empty SF field", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, reg.BySFName(""))
	})

	t.Run("Required returns only required fields", func(t *testing.T) {
		t.Parallel()
		req := reg.Required()
		assert.Len(t, req, 2)
		keys := make([]string, len(req))
		for i, f := range req {
			keys[i] = f.Key
		}
		assert.Contains(t, keys, "revenue")
		assert.Contains(t, keys, "employees")
	})

	t.Run("Fields slice preserved", func(t *testing.T) {
		t.Parallel()
		assert.Len(t, reg.Fields, 4)
	})
}

func TestNewFieldRegistryEmpty(t *testing.T) {
	t.Parallel()
	reg := NewFieldRegistry(nil)
	assert.NotNil(t, reg)
	assert.Empty(t, reg.Fields)
	assert.Nil(t, reg.ByKey("anything"))
	assert.Empty(t, reg.Required())
}
