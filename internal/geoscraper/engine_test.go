package geoscraper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geospatial"
)

// failScraper always returns an error from Sync.
type failScraper struct {
	mockScraper
}

func (f *failScraper) Sync(_ context.Context, _ db.Pool, _ fetcher.Fetcher, _ string) (*SyncResult, error) {
	return nil, errors.New("sync failed")
}

// addressScraper implements AddressProducer.
type addressScraper struct {
	mockScraper
}

func (a *addressScraper) HasAddresses() bool { return true }

func setupEngine(t *testing.T, scrapers ...GeoScraper) (*Engine, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)

	reg := NewRegistry()
	for _, s := range scrapers {
		reg.Register(s)
	}

	syncLog := fedsync.NewSyncLog(mock)
	engine := NewEngine(mock, nil, syncLog, reg, nil, t.TempDir())
	return engine, mock
}

func setupEngineWithQueue(t *testing.T, scrapers ...GeoScraper) (*Engine, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)

	reg := NewRegistry()
	for _, s := range scrapers {
		reg.Register(s)
	}

	syncLog := fedsync.NewSyncLog(mock)
	queue := geospatial.NewGeocodeQueue(mock, nil, 100)
	engine := NewEngine(mock, nil, syncLog, reg, queue, t.TempDir())
	return engine, mock
}

func TestEngine_Run_NoScrapers(t *testing.T) {
	engine, mock := setupEngine(t)
	defer mock.Close()

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
}

func TestEngine_Run_SkipsNotDue(t *testing.T) {
	s := &mockScraper{name: "test_scraper", category: National, run: false}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	// LastSuccess query — return a recent time so ShouldRun returns false.
	recentSync := time.Now().Add(-1 * time.Hour)
	mock.ExpectQuery(`SELECT started_at FROM fed_data\.sync_log`).
		WithArgs("test_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"started_at"}).AddRow(recentSync))

	err := engine.Run(context.Background(), RunOpts{})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_Force(t *testing.T) {
	s := &mockScraper{name: "test_scraper", category: National, run: false}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	// Force=true skips ShouldRun check, goes straight to Start.
	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("test_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Complete.
	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_SyncFailure(t *testing.T) {
	s := &failScraper{mockScraper: mockScraper{name: "bad_scraper", category: National, run: true}}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	// Force to skip scheduling check.
	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("bad_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Fail.
	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs("sync failed", int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err) // individual failure doesn't abort engine
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_ContextCancelled(t *testing.T) {
	s := &mockScraper{name: "test_scraper", category: National, run: true}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := engine.Run(ctx, RunOpts{Force: true})
	assert.Error(t, err)
}

func TestEngine_Run_SelectError(t *testing.T) {
	engine, mock := setupEngine(t)
	defer mock.Close()

	// Requesting a nonexistent source triggers a Select error.
	err := engine.Run(context.Background(), RunOpts{Sources: []string{"nonexistent"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown scraper")
}

func TestEngine_Run_LastSuccessError(t *testing.T) {
	s := &mockScraper{name: "test_scraper", category: National, run: true}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	mock.ExpectQuery(`SELECT started_at FROM fed_data\.sync_log`).
		WithArgs("test_scraper").
		WillReturnError(errors.New("db connection lost"))

	err := engine.Run(context.Background(), RunOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check last sync")
}

func TestEngine_Run_StartSyncLogError(t *testing.T) {
	s := &mockScraper{name: "test_scraper", category: National, run: true}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("test_scraper").
		WillReturnError(errors.New("insert failed"))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start sync log")
}

func TestEngine_Run_CompleteLogError(t *testing.T) {
	s := &mockScraper{name: "test_scraper", category: National, run: true}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("test_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Complete fails — engine should still succeed (logs error, doesn't abort).
	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnError(errors.New("update failed"))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
}

func TestEngine_Run_FailLogError(t *testing.T) {
	s := &failScraper{mockScraper: mockScraper{name: "bad_scraper", category: National, run: true}}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("bad_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Fail log itself fails — engine should still proceed.
	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs("sync failed", int64(1)).
		WillReturnError(errors.New("fail log broken"))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
}

func TestEngine_Run_PostSyncWithAddressProducer(t *testing.T) {
	s := &addressScraper{mockScraper: mockScraper{
		name: "poi_scraper", table: "geo.poi", category: National, run: true,
	}}
	engine, mock := setupEngineWithQueue(t, s)
	defer mock.Close()

	// Start sync log.
	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("poi_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	// Complete sync log.
	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// PostSyncGeocode: query for ungeocoded rows — return empty (no work).
	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnRows(pgxmock.NewRows([]string{"source_id", "address"}))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_PostSyncSkipsNonAddressProducer(t *testing.T) {
	// Regular mockScraper doesn't implement AddressProducer — PostSync skipped.
	s := &mockScraper{name: "boundary_scraper", table: "geo.boundaries", category: National, run: true}
	engine, mock := setupEngineWithQueue(t, s)
	defer mock.Close()

	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("boundary_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// No PostSync query expected — scraper doesn't implement AddressProducer.

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_PostSyncError(t *testing.T) {
	s := &addressScraper{mockScraper: mockScraper{
		name: "poi_scraper", table: "geo.poi", category: National, run: true,
	}}
	engine, mock := setupEngineWithQueue(t, s)
	defer mock.Close()

	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("poi_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	// PostSyncGeocode: query fails — engine logs warning but doesn't abort.
	mock.ExpectQuery(`SELECT source_id, address FROM`).
		WillReturnError(errors.New("postsync query failed"))

	err := engine.Run(context.Background(), RunOpts{Force: true})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEngine_Run_ShouldRunTrue(t *testing.T) {
	s := &mockScraper{name: "due_scraper", category: National, run: true}
	engine, mock := setupEngine(t, s)
	defer mock.Close()

	// LastSuccess returns nil (never synced) → ShouldRun returns true.
	mock.ExpectQuery(`SELECT started_at FROM fed_data\.sync_log`).
		WithArgs("due_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"started_at"}))

	mock.ExpectQuery(`INSERT INTO fed_data\.sync_log`).
		WithArgs("due_scraper").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(1)))

	mock.ExpectExec(`UPDATE fed_data\.sync_log`).
		WithArgs(int64(42), pgxmock.AnyArg(), int64(1)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err := engine.Run(context.Background(), RunOpts{})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
