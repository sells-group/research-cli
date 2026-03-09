package dataset

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/resolve"
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

func (m *mockDataset) Name() string     { return m.name }
func (m *mockDataset) Table() string    { return m.table }
func (m *mockDataset) Phase() Phase     { return m.phase }
func (m *mockDataset) Cadence() Cadence { return m.cadence }
func (m *mockDataset) ShouldRun(_ time.Time, _ *time.Time) bool {
	return m.shouldRun
}
func (m *mockDataset) Sync(_ context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
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

func TestPhaseString_Unknown(t *testing.T) {
	p := Phase(99)
	assert.Equal(t, "unknown", p.String())
}

func TestRegistry_Get(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	r.Register(&mockDataset{name: "a", phase: Phase1})

	d, err := r.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "a", d.Name())

	_, err = r.Get("nonexistent")
	assert.Error(t, err)
}

func TestRegistry_SelectAll(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	r.Register(&mockDataset{name: "a", phase: Phase1})
	r.Register(&mockDataset{name: "b", phase: Phase2})

	result, err := r.Select(nil, nil)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestRegistry_SelectByNameAndPhase(t *testing.T) {
	r := &Registry{datasets: make(map[string]Dataset)}
	r.Register(&mockDataset{name: "a", phase: Phase1})
	r.Register(&mockDataset{name: "b", phase: Phase2})

	p := Phase1
	result, err := r.Select(&p, []string{"a", "b"})
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "a", result[0].Name())
}

// newMockSyncLog creates a pgxmock and SyncLog for engine tests.
func newMockSyncLog(t *testing.T) (pgxmock.PgxPoolIface, *fedsync.SyncLog) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(func() { mock.Close() })
	return mock, fedsync.NewSyncLog(mock)
}

func TestEngine_Run_Success(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	ds := &mockDataset{name: "test_ds", phase: Phase1, shouldRun: true, syncRows: 100}
	reg := &Registry{datasets: map[string]Dataset{"test_ds": ds}, order: []string{"test_ds"}}

	// LastSuccess query - no rows (never synced)
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("test_ds").
		WillReturnError(errors.New("no rows in result set"))

	// Start query - returns sync ID
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("test_ds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Complete query
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(100), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.True(t, ds.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_Skip(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	ds := &mockDataset{name: "test_ds", phase: Phase1, shouldRun: false, syncRows: 0}
	reg := &Registry{datasets: map[string]Dataset{"test_ds": ds}, order: []string{"test_ds"}}

	// LastSuccess returns a recent time
	lastSync := time.Now().Add(-1 * time.Hour)
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("test_ds").
		WillReturnRows(pgxmock.NewRows([]string{"started_at"}).AddRow(lastSync))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.False(t, ds.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_Force(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	ds := &mockDataset{name: "test_ds", phase: Phase1, shouldRun: false, syncRows: 50}
	reg := &Registry{datasets: map[string]Dataset{"test_ds": ds}, order: []string{"test_ds"}}

	// Force=true skips LastSuccess check entirely, goes straight to Start
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("test_ds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(50), pgxmock.AnyArg(), int64(2)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{Force: true})
	assert.NoError(t, err)
	assert.True(t, ds.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_SyncFailure(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	syncErr := errors.New("download failed")
	ds := &mockDataset{name: "test_ds", phase: Phase1, shouldRun: true, syncErr: syncErr}
	reg := &Registry{datasets: map[string]Dataset{"test_ds": ds}, order: []string{"test_ds"}}

	// LastSuccess - never synced
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("test_ds").
		WillReturnError(errors.New("no rows in result set"))

	// Start
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("test_ds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(3)))

	// Fail (the sync failed, so engine records failure)
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs("download failed", int64(3)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err) // engine continues past failures
	assert.True(t, ds.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_ContextCancellation(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	ds := &mockDataset{name: "test_ds", phase: Phase1, shouldRun: true}
	reg := &Registry{datasets: map[string]Dataset{"test_ds": ds}, order: []string{"test_ds"}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(ctx, RunOpts{Force: true})
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, ds.synced)
}

func TestEngine_Run_NoDatasetsSelected(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)

	reg := &Registry{datasets: make(map[string]Dataset), order: nil}

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
}

func TestEngine_Run_InvalidDatasetSelection(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)

	reg := &Registry{datasets: make(map[string]Dataset), order: nil}

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{Datasets: []string{"nonexistent"}})
	assert.Error(t, err)
}

// blockingMockDataset implements Dataset with a Sync that blocks until ctx is cancelled.
type blockingMockDataset struct {
	mockDataset
}

func (m *blockingMockDataset) Sync(ctx context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	m.synced = true
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestEngine_Run_DatasetTimeout(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	ds := &blockingMockDataset{mockDataset: mockDataset{name: "slow_ds", phase: Phase1, shouldRun: true}}
	reg := &Registry{datasets: map[string]Dataset{"slow_ds": ds}, order: []string{"slow_ds"}}

	// Start query - returns sync ID
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("slow_ds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Fail query - the dataset times out and is recorded as failed
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(ctx, RunOpts{Force: true})
	// The engine may return a context error if the errgroup goroutine
	// returns gctx.Err() via the initial select check, OR it may return nil
	// if the dataset goroutine handled the error internally (failed.Add(1); return nil).
	// Either way, the sync should have been attempted and recorded as failed.
	if err != nil {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
	assert.True(t, ds.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_TimeoutDoesNotAffectOthers(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	slowDS := &blockingMockDataset{mockDataset: mockDataset{name: "slow_ds", phase: Phase1, shouldRun: true}}
	fastDS := &mockDataset{name: "fast_ds", phase: Phase1, shouldRun: true, syncRows: 42}
	reg := &Registry{
		datasets: map[string]Dataset{"slow_ds": slowDS, "fast_ds": fastDS},
		order:    []string{"fast_ds", "slow_ds"},
	}

	// fast_ds: Start -> Complete
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("fast_ds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// slow_ds: Start -> Fail (timeout)
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("slow_ds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(pgxmock.AnyArg(), int64(2)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(ctx, RunOpts{Force: true})
	// The engine may return a context error or nil depending on goroutine scheduling.
	if err != nil {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
	// The fast dataset should have completed successfully regardless of the slow one timing out.
	assert.True(t, fastDS.synced)
	assert.True(t, slowDS.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_MultipleDatasets(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	ds1 := &mockDataset{name: "ds1", phase: Phase1, shouldRun: true, syncRows: 10}
	ds2 := &mockDataset{name: "ds2", phase: Phase1, shouldRun: false}
	ds3 := &mockDataset{name: "ds3", phase: Phase1, shouldRun: true, syncRows: 20}
	reg := &Registry{
		datasets: map[string]Dataset{"ds1": ds1, "ds2": ds2, "ds3": ds3},
		order:    []string{"ds1", "ds2", "ds3"},
	}

	// ds1: LastSuccess (never synced) -> Start -> Complete
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("ds1").
		WillReturnError(errors.New("no rows in result set"))
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("ds1").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(10), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// ds2: LastSuccess returns recent sync, shouldRun=false -> skip
	lastSync := time.Now().Add(-1 * time.Hour)
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("ds2").
		WillReturnRows(pgxmock.NewRows([]string{"started_at"}).AddRow(lastSync))

	// ds3: LastSuccess (never synced) -> Start -> Complete
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("ds3").
		WillReturnError(errors.New("no rows in result set"))
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("ds3").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(20), pgxmock.AnyArg(), int64(2)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.True(t, ds1.synced)
	assert.False(t, ds2.synced)
	assert.True(t, ds3.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestXrefInSelection(t *testing.T) {
	engine := &Engine{}

	t.Run("xref present", func(t *testing.T) {
		datasets := []Dataset{
			&mockDataset{name: "fpds"},
			&EntityXref{},
		}
		assert.True(t, engine.xrefInSelection(datasets))
	})

	t.Run("xref absent", func(t *testing.T) {
		datasets := []Dataset{
			&mockDataset{name: "fpds"},
			&mockDataset{name: "ppp"},
		}
		assert.False(t, engine.xrefInSelection(datasets))
	})

	t.Run("empty", func(t *testing.T) {
		assert.False(t, engine.xrefInSelection(nil))
	})
}

func TestEngine_Run_AutoTriggerXref(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	// An entity-bearing dataset that syncs successfully.
	ds := &mockDataset{name: "fpds", phase: Phase1, shouldRun: true, syncRows: 10}
	reg := &Registry{datasets: map[string]Dataset{"fpds": ds}, order: []string{"fpds"}}

	// LastSuccess → never synced
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("fpds").
		WillReturnError(errors.New("no rows in result set"))

	// Start fpds sync
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("fpds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))
	// Complete fpds sync
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(10), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// Auto-trigger: Start entity_xref sync
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("entity_xref").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))

	// Stage 1: xref builder — truncate + 2 CRD-CIK passes
	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	for range 2 {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref").
			WillReturnResult(pgxmock.NewResult("INSERT", 0))
	}

	// Stage 2: multi xref builder — truncate + 85 passes
	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	for range 85 {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
			WillReturnResult(pgxmock.NewResult("INSERT", 0))
	}

	// Complete entity_xref sync
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(0), pgxmock.AnyArg(), int64(2)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.True(t, ds.synced)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_NoAutoTriggerWhenXrefSelected(t *testing.T) {
	mock, syncLog := newMockSyncLog(t)
	mock.MatchExpectationsInOrder(false)

	// Entity-bearing dataset + entity_xref both selected → no auto-trigger.
	ds := &mockDataset{name: "fpds", phase: Phase1, shouldRun: true, syncRows: 10}
	xref := &EntityXref{}
	reg := &Registry{
		datasets: map[string]Dataset{"fpds": ds, "entity_xref": xref},
		order:    []string{"fpds", "entity_xref"},
	}

	// fpds: LastSuccess → never
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("fpds").
		WillReturnError(errors.New("no rows in result set"))
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("fpds").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(10), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// entity_xref: LastSuccess → never
	mock.ExpectQuery("SELECT started_at FROM fed_data.sync_log").
		WithArgs("entity_xref").
		WillReturnError(errors.New("no rows in result set"))
	mock.ExpectQuery("INSERT INTO fed_data.sync_log").
		WithArgs("entity_xref").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(2)))

	// entity_xref.Sync: Stage 1 — truncate + 3 passes
	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	for range 2 {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref").
			WillReturnResult(pgxmock.NewResult("INSERT", 0))
	}
	// Stage 2 — truncate + 85 passes
	mock.ExpectExec("TRUNCATE TABLE fed_data.entity_xref_multi").
		WillReturnResult(pgxmock.NewResult("TRUNCATE", 0))
	for range 85 {
		mock.ExpectExec("INSERT INTO fed_data.entity_xref_multi").
			WillReturnResult(pgxmock.NewResult("INSERT", 0))
	}

	mock.ExpectExec("UPDATE fed_data.sync_log").
		WithArgs(int64(0), pgxmock.AnyArg(), int64(2)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	engine := NewEngine(mock, nil, syncLog, reg, t.TempDir())
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.True(t, ds.synced)
	// If auto-trigger fired, we'd have unmet expectations for a second entity_xref log entry.
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEntityBearingDatasets(t *testing.T) {
	// All expected entity-bearing datasets should be registered.
	expected := []string{
		"adv_part1", "ia_compilation", "brokercheck", "form_bd",
		"edgar_submissions", "form_d", "ncen", "form_5500",
		"eo_bmf", "fdic_bankfind", "usaspending", "fpds",
		"ppp", "osha_ita", "epa_echo", "sba_7a_504",
		"ncua_call_reports",
	}
	for _, name := range expected {
		assert.True(t, entityBearingDatasets[name], "missing entity-bearing dataset: %s", name)
	}

	// Non-entity datasets should not be present.
	nonEntity := []string{"entity_xref", "cbp", "qcew", "fred", "census_geo"}
	for _, name := range nonEntity {
		assert.False(t, entityBearingDatasets[name], "unexpected entity-bearing dataset: %s", name)
	}
}

// TestEntityBearingDatasets_HaveXrefPasses is a CI safety net that fails when
// a new entity-bearing dataset is added to entityBearingDatasets without
// corresponding cross-reference passes in resolve.AllPassSQL().
// It maps each dataset name to the fed_data table(s) it populates and asserts
// that at least one xref pass references that table.
func TestEntityBearingDatasets_HaveXrefPasses(t *testing.T) {
	// Map dataset name → fed_data table name(s) that should appear in xref SQL.
	tableMap := map[string][]string{
		"adv_part1":         {"adv_firms"},
		"ia_compilation":    {"adv_firms"},
		"brokercheck":       {"brokercheck"},
		"form_bd":           {"form_bd"},
		"edgar_submissions": {"edgar_entities"},
		"form_d":            {"form_d"},
		"ncen":              {"ncen_registrants", "ncen_advisers"},
		"form_5500":         {"form_5500"},
		"eo_bmf":            {"eo_bmf"},
		"fdic_bankfind":     {"fdic_institutions"},
		"usaspending":       {"usaspending_awards"},
		"fpds":              {"fpds_contracts"},
		"ppp":               {"ppp_loans"},
		"osha_ita":          {"osha_inspections"},
		"epa_echo":          {"epa_facilities"},
		"sba_7a_504":        {"sba_loans"},
		"ncua_call_reports": {"ncua_call_reports"},
	}

	allSQL := resolve.AllPassSQL()

	// Every entity-bearing dataset must have at least one table covered.
	for dsName := range entityBearingDatasets {
		tables, ok := tableMap[dsName]
		if !ok {
			t.Errorf("entity-bearing dataset %q has no tableMap entry — add it to this test", dsName)
			continue
		}

		covered := false
		for _, tbl := range tables {
			if strings.Contains(allSQL, "fed_data."+tbl) {
				covered = true
				break
			}
		}
		assert.True(t, covered,
			"entity-bearing dataset %q (tables: %v) has no xref passes — "+
				"add cross-reference passes to resolve/multi_xref.go", dsName, tables)
	}
}
