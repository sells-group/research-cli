package analysis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/db"
)

// Compile-time interface check.
var _ Analyzer = (*mockAnalyzer)(nil)

// mockAnalyzer is a test double for the Analyzer interface.
type mockAnalyzer struct {
	name     string
	category Category
	deps     []string
	valErr   error
	runErr   error
	result   *RunResult
	runCalls int
}

func (m *mockAnalyzer) Name() string           { return m.name }
func (m *mockAnalyzer) Category() Category     { return m.category }
func (m *mockAnalyzer) Dependencies() []string { return m.deps }

func (m *mockAnalyzer) Validate(_ context.Context, _ db.Pool) error {
	return m.valErr
}

func (m *mockAnalyzer) Run(_ context.Context, _ db.Pool, _ RunOpts) (*RunResult, error) {
	m.runCalls++
	if m.runErr != nil {
		return nil, m.runErr
	}
	if m.result != nil {
		return m.result, nil
	}
	return &RunResult{RowsAffected: 42}, nil
}

// --- Category ---

func TestCategory_String(t *testing.T) {
	tests := []struct {
		cat  Category
		want string
	}{
		{Spatial, "spatial"},
		{Scoring, "scoring"},
		{Correlation, "correlation"},
		{Ranking, "ranking"},
		{Export, "export"},
		{Category(99), "unknown"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.cat.String())
	}
}

func TestParseCategory_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  Category
	}{
		{"spatial", Spatial},
		{"scoring", Scoring},
		{"correlation", Correlation},
		{"ranking", Ranking},
		{"export", Export},
	}
	for _, tt := range tests {
		cat, err := ParseCategory(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.want, cat)
	}
}

func TestParseCategory_Invalid(t *testing.T) {
	_, err := ParseCategory("bogus")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown analysis category")
}

// --- RunResult ---

func TestRunResult_Fields(t *testing.T) {
	r := &RunResult{
		RowsAffected: 100,
		Metadata:     map[string]any{"key": "val"},
	}
	assert.Equal(t, int64(100), r.RowsAffected)
	assert.Equal(t, "val", r.Metadata["key"])
}

// --- RunOpts ---

func TestRunOpts_Defaults(t *testing.T) {
	opts := RunOpts{}
	assert.Nil(t, opts.Category)
	assert.Nil(t, opts.Analyzers)
	assert.False(t, opts.Force)
}
