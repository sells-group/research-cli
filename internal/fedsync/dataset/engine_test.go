package dataset

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/fetcher"
)

// mockDataset implements Dataset for testing.
type mockDataset struct {
	name      string
	table     string
	phase     Phase
	cadence   Cadence
	shouldRun bool
	syncErr   error
	syncRows  int64
	synced    bool
}

func (m *mockDataset) Name() string    { return m.name }
func (m *mockDataset) Table() string   { return m.table }
func (m *mockDataset) Phase() Phase    { return m.phase }
func (m *mockDataset) Cadence() Cadence { return m.cadence }
func (m *mockDataset) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return m.shouldRun
}
func (m *mockDataset) Sync(ctx context.Context, pool *pgxpool.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	m.synced = true
	if m.syncErr != nil {
		return nil, m.syncErr
	}
	return &SyncResult{RowsSynced: m.syncRows}, nil
}

func TestParsePhase(t *testing.T) {
	tests := []struct {
		input string
		phase Phase
		err   bool
	}{
		{"1", Phase1, false},
		{"1b", Phase1B, false},
		{"1B", Phase1B, false},
		{"2", Phase2, false},
		{"3", Phase3, false},
		{"4", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		p, err := ParsePhase(tt.input)
		if tt.err {
			assert.Error(t, err, "input: %q", tt.input)
		} else {
			assert.NoError(t, err, "input: %q", tt.input)
			assert.Equal(t, tt.phase, p)
		}
	}
}

func TestPhaseString(t *testing.T) {
	assert.Equal(t, "1", Phase1.String())
	assert.Equal(t, "1b", Phase1B.String())
	assert.Equal(t, "2", Phase2.String())
	assert.Equal(t, "3", Phase3.String())
}

func TestRegistry_SelectByPhase(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	r.Register(&mockDataset{name: "a", phase: Phase1})
	r.Register(&mockDataset{name: "b", phase: Phase2})
	r.Register(&mockDataset{name: "c", phase: Phase1})

	p := Phase1
	result, err := r.Select(&p, nil)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "a", result[0].Name())
	assert.Equal(t, "c", result[1].Name())
}

func TestRegistry_SelectByName(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	r.Register(&mockDataset{name: "a", phase: Phase1})
	r.Register(&mockDataset{name: "b", phase: Phase2})

	result, err := r.Select(nil, []string{"b"})
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "b", result[0].Name())
}

func TestRegistry_SelectUnknown(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	_, err := r.Select(nil, []string{"nonexistent"})
	assert.Error(t, err)
}

func TestRegistry_AllNames(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	r.Register(&mockDataset{name: "a"})
	r.Register(&mockDataset{name: "b"})

	names := r.AllNames()
	assert.Equal(t, []string{"a", "b"}, names)
}
