package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterByMaxPriority(t *testing.T) {
	t.Parallel()

	questions := []Question{
		{ID: "q0", Priority: "P0", FieldKey: "critical_field"},
		{ID: "q1a", Priority: "P1", FieldKey: "important_field"},
		{ID: "q1b", Priority: "P1", FieldKey: "another_important"},
		{ID: "q2", Priority: "P2", FieldKey: "standard_field"},
		{ID: "q3", Priority: "P3", FieldKey: "low_field"},
	}

	t.Run("P0 returns only P0", func(t *testing.T) {
		t.Parallel()
		result := FilterByMaxPriority(questions, "P0")
		assert.Len(t, result, 1)
		assert.Equal(t, "q0", result[0].ID)
	})

	t.Run("P1 returns P0 and P1", func(t *testing.T) {
		t.Parallel()
		result := FilterByMaxPriority(questions, "P1")
		assert.Len(t, result, 3)
		ids := make([]string, len(result))
		for i, q := range result {
			ids[i] = q.ID
		}
		assert.Contains(t, ids, "q0")
		assert.Contains(t, ids, "q1a")
		assert.Contains(t, ids, "q1b")
	})

	t.Run("P2 returns P0, P1, P2", func(t *testing.T) {
		t.Parallel()
		result := FilterByMaxPriority(questions, "P2")
		assert.Len(t, result, 4)
	})

	t.Run("P3 returns all", func(t *testing.T) {
		t.Parallel()
		result := FilterByMaxPriority(questions, "P3")
		assert.Len(t, result, 5)
	})

	t.Run("invalid priority returns nil", func(t *testing.T) {
		t.Parallel()
		result := FilterByMaxPriority(questions, "P99")
		assert.Nil(t, result)
	})

	t.Run("empty questions returns nil", func(t *testing.T) {
		t.Parallel()
		result := FilterByMaxPriority(nil, "P1")
		assert.Nil(t, result)
	})

	t.Run("questions with unrecognized priority excluded", func(t *testing.T) {
		t.Parallel()
		qs := []Question{
			{ID: "q1", Priority: "P1"},
			{ID: "q_bad", Priority: "high"},
		}
		result := FilterByMaxPriority(qs, "P3")
		assert.Len(t, result, 1)
		assert.Equal(t, "q1", result[0].ID)
	})
}
