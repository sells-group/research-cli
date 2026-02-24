package monitoring

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/resilience"
	"github.com/sells-group/research-cli/internal/store"
)

// mockStore implements store.Store for testing.
type mockStore struct {
	runs     []model.Run
	dlqCount int
	listErr  error
	dlqErr   error
}

func (m *mockStore) ListRuns(_ context.Context, filter store.RunFilter) ([]model.Run, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var filtered []model.Run
	for _, r := range m.runs {
		if !filter.CreatedAfter.IsZero() && r.CreatedAt.Before(filter.CreatedAfter) {
			continue
		}
		if filter.Status != "" && r.Status != filter.Status {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered, nil
}

func (m *mockStore) CountDLQ(_ context.Context) (int, error) {
	return m.dlqCount, m.dlqErr
}

// Unused store methods — satisfy the interface.
func (m *mockStore) CreateRun(context.Context, model.Company) (*model.Run, error)    { return nil, nil }
func (m *mockStore) UpdateRunStatus(context.Context, string, model.RunStatus) error  { return nil }
func (m *mockStore) UpdateRunResult(context.Context, string, *model.RunResult) error { return nil }
func (m *mockStore) FailRun(context.Context, string, *model.RunError) error          { return nil }
func (m *mockStore) GetRun(context.Context, string) (*model.Run, error)              { return nil, nil }
func (m *mockStore) CreatePhase(context.Context, string, string) (*model.RunPhase, error) {
	return nil, nil
}
func (m *mockStore) CompletePhase(context.Context, string, *model.PhaseResult) error { return nil }
func (m *mockStore) GetCachedCrawl(context.Context, string) (*model.CrawlCache, error) {
	return nil, nil
}
func (m *mockStore) SetCachedCrawl(context.Context, string, []model.CrawledPage, time.Duration) error {
	return nil
}
func (m *mockStore) DeleteExpiredCrawls(context.Context) (int, error)          { return 0, nil }
func (m *mockStore) GetCachedLinkedIn(context.Context, string) ([]byte, error) { return nil, nil }
func (m *mockStore) SetCachedLinkedIn(context.Context, string, []byte, time.Duration) error {
	return nil
}
func (m *mockStore) GetCachedScrape(context.Context, string) ([]byte, error) { return nil, nil }
func (m *mockStore) SetCachedScrape(context.Context, string, []byte, time.Duration) error {
	return nil
}
func (m *mockStore) GetHighConfidenceAnswers(context.Context, string, float64, time.Duration) ([]model.ExtractionAnswer, error) {
	return nil, nil
}
func (m *mockStore) SaveCheckpoint(context.Context, string, string, []byte) error { return nil }
func (m *mockStore) LoadCheckpoint(context.Context, string) (*model.Checkpoint, error) {
	return nil, nil
}
func (m *mockStore) DeleteCheckpoint(context.Context, string) error        { return nil }
func (m *mockStore) DeleteExpiredLinkedIn(context.Context) (int, error)    { return 0, nil }
func (m *mockStore) DeleteExpiredScrapes(context.Context) (int, error)     { return 0, nil }
func (m *mockStore) EnqueueDLQ(context.Context, resilience.DLQEntry) error { return nil }
func (m *mockStore) DequeueDLQ(context.Context, resilience.DLQFilter) ([]resilience.DLQEntry, error) {
	return nil, nil
}
func (m *mockStore) IncrementDLQRetry(context.Context, string, time.Time, string) error { return nil }
func (m *mockStore) RemoveDLQ(context.Context, string) error                            { return nil }
func (m *mockStore) SaveProvenance(context.Context, []model.FieldProvenance) error      { return nil }
func (m *mockStore) GetProvenance(context.Context, string) ([]model.FieldProvenance, error) {
	return nil, nil
}
func (m *mockStore) GetLatestProvenance(context.Context, string) ([]model.FieldProvenance, error) {
	return nil, nil
}
func (m *mockStore) ListStaleCompanies(context.Context, store.StaleCompanyFilter) ([]store.StaleCompany, error) {
	return nil, nil
}
func (m *mockStore) Ping(context.Context) error    { return nil }
func (m *mockStore) Migrate(context.Context) error { return nil }
func (m *mockStore) Close() error                  { return nil }

// mockSyncLog implements SyncLogQuerier for testing.
type mockSyncLog struct {
	entries []fedsync.SyncEntry
	err     error
}

func (m *mockSyncLog) ListAll(_ context.Context) ([]fedsync.SyncEntry, error) {
	return m.entries, m.err
}

func TestCollector_EmptyStore(t *testing.T) {
	st := &mockStore{}
	c := NewCollector(st, nil)

	snap, err := c.Collect(context.Background(), 24)
	require.NoError(t, err)

	assert.Equal(t, 0, snap.PipelineTotal)
	assert.Equal(t, 0, snap.PipelineFailed)
	assert.Equal(t, 0.0, snap.PipelineFailRate)
	assert.Equal(t, 0.0, snap.PipelineCostUSD)
	assert.Equal(t, 24, snap.LookbackHours)
	assert.False(t, snap.CollectedAt.IsZero())
}

func TestCollector_PipelineMetrics(t *testing.T) {
	now := time.Now().UTC()
	st := &mockStore{
		runs: []model.Run{
			{ID: "1", Status: model.RunStatusComplete, CreatedAt: now.Add(-1 * time.Hour), Result: &model.RunResult{TotalCost: 1.50, TotalTokens: 5000, Score: 0.85}},
			{ID: "2", Status: model.RunStatusComplete, CreatedAt: now.Add(-2 * time.Hour), Result: &model.RunResult{TotalCost: 2.00, TotalTokens: 7000, Score: 0.90}},
			{ID: "3", Status: model.RunStatusFailed, CreatedAt: now.Add(-3 * time.Hour), Result: &model.RunResult{}},
			{ID: "4", Status: model.RunStatusQueued, CreatedAt: now.Add(-30 * time.Minute)},
			// Outside lookback window — should be filtered out.
			{ID: "5", Status: model.RunStatusFailed, CreatedAt: now.Add(-48 * time.Hour), Result: &model.RunResult{}},
		},
		dlqCount: 3,
	}

	c := NewCollector(st, nil)
	snap, err := c.Collect(context.Background(), 24)
	require.NoError(t, err)

	assert.Equal(t, 4, snap.PipelineTotal)
	assert.Equal(t, 2, snap.PipelineComplete)
	assert.Equal(t, 1, snap.PipelineFailed)
	assert.Equal(t, 1, snap.PipelineQueued)
	assert.InDelta(t, 1.0/3.0, snap.PipelineFailRate, 0.001) // 1 failed / 3 finished
	assert.InDelta(t, 3.50, snap.PipelineCostUSD, 0.001)
	assert.InDelta(t, 0.875, snap.PipelineAvgScore, 0.001)
	assert.Equal(t, 3000, snap.PipelineAvgTokens) // (5000+7000)/4
	assert.Equal(t, 3, snap.DLQDepth)
}

func TestCollector_FedsyncMetrics(t *testing.T) {
	now := time.Now().UTC()
	st := &mockStore{}
	sl := &mockSyncLog{
		entries: []fedsync.SyncEntry{
			{Dataset: "cbp", Status: "complete", StartedAt: now.Add(-2 * time.Hour)},
			{Dataset: "fpds", Status: "failed", StartedAt: now.Add(-5 * time.Hour)},
			{Dataset: "qcew", Status: "running", StartedAt: now.Add(-1 * time.Hour)},
			// Outside window.
			{Dataset: "oews", Status: "failed", StartedAt: now.Add(-72 * time.Hour)},
		},
	}

	c := NewCollector(st, sl)
	snap, err := c.Collect(context.Background(), 24)
	require.NoError(t, err)

	assert.Equal(t, 3, snap.FedsyncTotal)
	assert.Equal(t, 1, snap.FedsyncComplete)
	assert.Equal(t, 1, snap.FedsyncFailed)
	assert.Equal(t, 1, snap.FedsyncRunning)
}

func TestCollector_NilSyncLog(t *testing.T) {
	st := &mockStore{}
	c := NewCollector(st, nil)

	snap, err := c.Collect(context.Background(), 24)
	require.NoError(t, err)
	assert.Equal(t, 0, snap.FedsyncTotal)
}

func TestCollector_FailureRateZeroFinished(t *testing.T) {
	now := time.Now().UTC()
	st := &mockStore{
		runs: []model.Run{
			{ID: "1", Status: model.RunStatusQueued, CreatedAt: now.Add(-1 * time.Hour)},
			{ID: "2", Status: model.RunStatusQueued, CreatedAt: now.Add(-2 * time.Hour)},
		},
	}

	c := NewCollector(st, nil)
	snap, err := c.Collect(context.Background(), 24)
	require.NoError(t, err)

	// No finished runs, so failure rate should be 0.
	assert.Equal(t, 0.0, snap.PipelineFailRate)
}
