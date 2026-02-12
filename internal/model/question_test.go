package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenUsageAdd(t *testing.T) {
	t.Parallel()

	t.Run("adds all fields", func(t *testing.T) {
		t.Parallel()
		a := TokenUsage{InputTokens: 100, OutputTokens: 50, CacheCreationTokens: 10, CacheReadTokens: 20, Cost: 0.01}
		b := TokenUsage{InputTokens: 200, OutputTokens: 100, CacheCreationTokens: 5, CacheReadTokens: 30, Cost: 0.02}
		a.Add(b)
		assert.Equal(t, 300, a.InputTokens)
		assert.Equal(t, 150, a.OutputTokens)
		assert.Equal(t, 15, a.CacheCreationTokens)
		assert.Equal(t, 50, a.CacheReadTokens)
		assert.InDelta(t, 0.03, a.Cost, 0.0001)
	})

	t.Run("add zero is no-op", func(t *testing.T) {
		t.Parallel()
		a := TokenUsage{InputTokens: 100, Cost: 0.01}
		a.Add(TokenUsage{})
		assert.Equal(t, 100, a.InputTokens)
		assert.InDelta(t, 0.01, a.Cost, 0.0001)
	})
}
