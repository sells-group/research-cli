package analysis

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupEngine creates an Engine with pgxmock and registers the given analyzers.
func setupEngine(t *testing.T, analyzers ...Analyzer) (*Engine, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(func() { mock.Close() })

	mock.MatchExpectationsInOrder(false)

	reg := NewRegistry()
	for _, a := range analyzers {
		reg.Register(a)
	}

	alog := NewLog(mock)
	engine := NewEngine(mock, alog, reg)
	return engine, mock
}

// expectAnalysisRun adds pgxmock expectations for a successful analyzer log cycle
// (Start → Complete with 3 args: rows_affected, metadata, id).
func expectAnalysisRun(mock pgxmock.PgxPoolIface, name string, runID int64) {
	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs(name).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(runID))
	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), runID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
}

// expectAnalysisFail adds pgxmock expectations for a failed analyzer log cycle
// (Start → Fail with 2 args: error, id).
func expectAnalysisFail(mock pgxmock.PgxPoolIface, name string, runID int64) {
	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs(name).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(runID))
	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(pgxmock.AnyArg(), runID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
}

func TestEngine_Run_NoAnalyzers(t *testing.T) {
	engine, mock := setupEngine(t)

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_SingleAnalyzer(t *testing.T) {
	a := &mockAnalyzer{name: "prox", category: Spatial, result: &RunResult{RowsAffected: 100}}
	engine, mock := setupEngine(t, a)

	expectAnalysisRun(mock, "prox", 1)

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_DependencyOrder(t *testing.T) {
	a := &mockAnalyzer{name: "a", category: Spatial, result: &RunResult{RowsAffected: 10}}
	b := &mockAnalyzer{name: "b", category: Scoring, deps: []string{"a"}, result: &RunResult{RowsAffected: 20}}
	c := &mockAnalyzer{name: "c", category: Ranking, deps: []string{"b"}, result: &RunResult{RowsAffected: 30}}

	engine, mock := setupEngine(t, a, b, c)

	expectAnalysisRun(mock, "a", 1)
	expectAnalysisRun(mock, "b", 2)
	expectAnalysisRun(mock, "c", 3)

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)

	// All three should have run in dependency order.
	assert.Equal(t, 1, a.runCalls)
	assert.Equal(t, 1, b.runCalls)
	assert.Equal(t, 1, c.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_SkipsOnMissingDep(t *testing.T) {
	// "a" fails, "b" depends on "a" and should be skipped.
	a := &mockAnalyzer{name: "a", category: Spatial, runErr: fmt.Errorf("boom")}
	b := &mockAnalyzer{name: "b", category: Scoring, deps: []string{"a"}, result: &RunResult{RowsAffected: 20}}

	engine, mock := setupEngine(t, a, b)

	// "a" starts but fails.
	expectAnalysisFail(mock, "a", 1)

	// "b" never starts because "a" failed (dependency not satisfied).

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.Equal(t, 0, b.runCalls) // skipped due to missing dep
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_Force(t *testing.T) {
	a := &mockAnalyzer{
		name:     "a",
		category: Spatial,
		valErr:   fmt.Errorf("no source data"),
		result:   &RunResult{RowsAffected: 5},
	}
	engine, mock := setupEngine(t, a)

	expectAnalysisRun(mock, "a", 1)

	// With Force=true, validation is skipped.
	err := engine.Run(context.Background(), RunOpts{Force: true})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_ValidationFailed(t *testing.T) {
	a := &mockAnalyzer{
		name:     "a",
		category: Spatial,
		valErr:   fmt.Errorf("no source data"),
	}
	engine, mock := setupEngine(t, a)

	// Without Force, validation failure causes skip (no log entries).
	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.Equal(t, 0, a.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_AnalyzerFailure_DoesNotAbort(t *testing.T) {
	// "a" and "b" are independent; "a" fails, "b" should still run.
	a := &mockAnalyzer{name: "a", category: Spatial, runErr: fmt.Errorf("boom")}
	b := &mockAnalyzer{name: "b", category: Scoring, result: &RunResult{RowsAffected: 10}}

	engine, mock := setupEngine(t, a, b)

	// "a" starts and fails.
	expectAnalysisFail(mock, "a", 1)

	// "b" starts and completes.
	expectAnalysisRun(mock, "b", 2)

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.Equal(t, 1, b.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	a := &mockAnalyzer{name: "a", category: Spatial}
	engine, mock := setupEngine(t, a)

	err := engine.Run(ctx, RunOpts{})
	// Context cancelled propagates.
	assert.Error(t, err)
	assert.Equal(t, 0, a.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_StartLogError(t *testing.T) {
	a := &mockAnalyzer{name: "a", category: Spatial}
	engine, mock := setupEngine(t, a)

	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs("a").
		WillReturnError(fmt.Errorf("db down"))

	err := engine.Run(context.Background(), RunOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start analysis log")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_CompleteLogError(t *testing.T) {
	// Complete log error is non-fatal; the engine continues.
	a := &mockAnalyzer{name: "a", category: Spatial, result: &RunResult{RowsAffected: 10}}
	engine, mock := setupEngine(t, a)

	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs("a").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), int64(1)).
		WillReturnError(fmt.Errorf("oops"))

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_FailLogError(t *testing.T) {
	// Fail log error is non-fatal; the engine continues.
	a := &mockAnalyzer{name: "a", category: Spatial, runErr: fmt.Errorf("compute failed")}
	engine, mock := setupEngine(t, a)

	mock.ExpectQuery("INSERT INTO geo.analysis_log").
		WithArgs("a").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec("UPDATE geo.analysis_log").
		WithArgs(pgxmock.AnyArg(), int64(1)).
		WillReturnError(fmt.Errorf("oops"))

	err := engine.Run(context.Background(), RunOpts{})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_WithCategory(t *testing.T) {
	a := &mockAnalyzer{name: "a", category: Spatial, result: &RunResult{RowsAffected: 10}}
	b := &mockAnalyzer{name: "b", category: Scoring, result: &RunResult{RowsAffected: 20}}

	engine, mock := setupEngine(t, a, b)

	cat := Spatial
	expectAnalysisRun(mock, "a", 1)

	err := engine.Run(context.Background(), RunOpts{Category: &cat})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.Equal(t, 0, b.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_WithAnalyzerNames(t *testing.T) {
	a := &mockAnalyzer{name: "a", category: Spatial, result: &RunResult{RowsAffected: 10}}
	b := &mockAnalyzer{name: "b", category: Scoring, deps: []string{"a"}, result: &RunResult{RowsAffected: 20}}
	c := &mockAnalyzer{name: "c", category: Ranking, result: &RunResult{RowsAffected: 30}}

	engine, mock := setupEngine(t, a, b, c)

	// Requesting "b" should include its dependency "a" but not "c".
	expectAnalysisRun(mock, "a", 1)
	expectAnalysisRun(mock, "b", 2)

	err := engine.Run(context.Background(), RunOpts{Analyzers: []string{"b"}})
	assert.NoError(t, err)
	assert.Equal(t, 1, a.runCalls)
	assert.Equal(t, 1, b.runCalls)
	assert.Equal(t, 0, c.runCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// --- buildLevels ---

func TestBuildLevels_Empty(t *testing.T) {
	levels := buildLevels(nil)
	assert.Nil(t, levels)
}

func TestBuildLevels_NoDeps(t *testing.T) {
	analyzers := []Analyzer{
		&mockAnalyzer{name: "a"},
		&mockAnalyzer{name: "b"},
		&mockAnalyzer{name: "c"},
	}
	levels := buildLevels(analyzers)
	require.Len(t, levels, 1)
	assert.Len(t, levels[0], 3)
}

func TestBuildLevels_LinearChain(t *testing.T) {
	analyzers := []Analyzer{
		&mockAnalyzer{name: "a"},
		&mockAnalyzer{name: "b", deps: []string{"a"}},
		&mockAnalyzer{name: "c", deps: []string{"b"}},
	}
	levels := buildLevels(analyzers)
	require.Len(t, levels, 3)
	assert.Len(t, levels[0], 1)
	assert.Equal(t, "a", levels[0][0].Name())
	assert.Len(t, levels[1], 1)
	assert.Equal(t, "b", levels[1][0].Name())
	assert.Len(t, levels[2], 1)
	assert.Equal(t, "c", levels[2][0].Name())
}

func TestBuildLevels_Diamond(t *testing.T) {
	// A (L0) -> B,C (L1) -> D (L2)
	analyzers := []Analyzer{
		&mockAnalyzer{name: "a"},
		&mockAnalyzer{name: "b", deps: []string{"a"}},
		&mockAnalyzer{name: "c", deps: []string{"a"}},
		&mockAnalyzer{name: "d", deps: []string{"b", "c"}},
	}
	levels := buildLevels(analyzers)
	require.Len(t, levels, 3)
	assert.Len(t, levels[0], 1) // a
	assert.Len(t, levels[1], 2) // b, c
	assert.Len(t, levels[2], 1) // d
	assert.Equal(t, "a", levels[0][0].Name())
	assert.Equal(t, "d", levels[2][0].Name())
}

func TestBuildLevels_MultiRoot(t *testing.T) {
	// Two independent roots.
	analyzers := []Analyzer{
		&mockAnalyzer{name: "a"},
		&mockAnalyzer{name: "b"},
		&mockAnalyzer{name: "c", deps: []string{"a", "b"}},
	}
	levels := buildLevels(analyzers)
	require.Len(t, levels, 2)
	assert.Len(t, levels[0], 2) // a, b
	assert.Len(t, levels[1], 1) // c
}
