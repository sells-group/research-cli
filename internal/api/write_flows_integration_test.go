//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/enrichmentstart"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/migrate"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/monitoring"
	"github.com/sells-group/research-cli/internal/readmodel"
	"github.com/sells-group/research-cli/internal/store"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
)

type workflowAcceptedResponse struct {
	Status        string `json:"status"`
	Company       string `json:"company"`
	WorkflowID    string `json:"workflow_id"`
	WorkflowRunID string `json:"workflow_run_id"`
	OriginalRunID string `json:"original_run_id,omitempty"`
	Reused        bool   `json:"reused"`
}

type listRunsResponse struct {
	Runs   []model.Run `json:"runs"`
	Total  int         `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
}

type runnerBehavior struct {
	Block chan struct{}
	Fail  bool
}

type integrationRunner struct {
	store     store.Store
	mu        sync.Mutex
	behaviors []runnerBehavior
	calls     int
	started   chan string
}

func (r *integrationRunner) Run(ctx context.Context, company model.Company) (*model.EnrichmentResult, error) {
	run, err := r.store.CreateRun(ctx, company)
	if err != nil {
		return nil, err
	}
	if err := r.store.UpdateRunStatus(ctx, run.ID, model.RunStatusCrawling); err != nil {
		return nil, err
	}

	behavior := r.nextBehavior()
	select {
	case r.started <- run.ID:
	default:
	}

	if behavior.Block != nil {
		select {
		case <-behavior.Block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if behavior.Fail {
		runErr := &model.RunError{
			Message:     "integration runner failure",
			Category:    model.ErrorCategoryTransient,
			FailedPhase: "pipeline",
		}
		if err := r.store.FailRun(ctx, run.ID, runErr); err != nil {
			return nil, err
		}
		return nil, errors.New(runErr.Message)
	}

	result := &model.RunResult{
		Score:       0.82,
		FieldsFound: 1,
		FieldsTotal: 1,
		Phases: []model.PhaseResult{{
			Name:     "pipeline",
			Status:   model.PhaseStatusComplete,
			Duration: 25,
		}},
	}
	if err := r.store.UpdateRunResult(ctx, run.ID, result); err != nil {
		return nil, err
	}

	return &model.EnrichmentResult{
		Company: company,
		RunID:   run.ID,
		Score:   result.Score,
	}, nil
}

func (r *integrationRunner) nextBehavior() runnerBehavior {
	r.mu.Lock()
	defer r.mu.Unlock()

	var behavior runnerBehavior
	if r.calls < len(r.behaviors) {
		behavior = r.behaviors[r.calls]
	}
	r.calls++
	return behavior
}

type integrationHarness struct {
	store     store.Store
	baseStore *store.PostgresStore
	cache     apicache.Cache
	syncLog   *fedsync.SyncLog
	server    *httptest.Server
}

func TestWebhookEnrichIntegration_IdempotentWhileRunningAndRestartsAfterCompletion(t *testing.T) {
	blockFirst := make(chan struct{})
	h := newIntegrationHarness(t, []runnerBehavior{{Block: blockFirst}, {}})

	requireQueueStatus(t, h)
	require.NoError(t, h.cache.Set(apicache.KeyQueueStatus, queueStatusResponse{Queued: 99}, time.Minute))

	first := postWebhook(t, h, enrichRequest{
		URL:          "https://acme.com",
		SalesforceID: "001-test",
		Name:         "Acme",
	})
	assert.False(t, first.Reused)
	assert.NotEmpty(t, first.WorkflowID)
	assert.NotEmpty(t, first.WorkflowRunID)
	_, ok := h.cache.Get(apicache.KeyQueueStatus)
	assert.False(t, ok)

	waitForRuns(t, h, "https://acme.com", 1)
	running := requireQueueStatus(t, h)
	assert.Equal(t, 1, running.Running)

	duplicate := postWebhook(t, h, enrichRequest{
		URL:          "https://acme.com",
		SalesforceID: "001-test",
		Name:         "Acme",
	})
	assert.True(t, duplicate.Reused)
	assert.Equal(t, first.WorkflowID, duplicate.WorkflowID)
	assert.Equal(t, first.WorkflowRunID, duplicate.WorkflowRunID)

	close(blockFirst)
	waitForQueueStatus(t, h, func(status queueStatusResponse) bool {
		return status.Complete == 1 && status.Total == 1
	})

	runs := requireRuns(t, h, "https://acme.com")
	require.Len(t, runs.Runs, 1)
	assert.Equal(t, model.RunStatusComplete, runs.Runs[0].Status)

	restart := postWebhook(t, h, enrichRequest{
		URL:          "https://acme.com",
		SalesforceID: "001-test",
		Name:         "Acme",
	})
	assert.False(t, restart.Reused)
	assert.Equal(t, first.WorkflowID, restart.WorkflowID)
	assert.NotEqual(t, first.WorkflowRunID, restart.WorkflowRunID)

	waitForQueueStatus(t, h, func(status queueStatusResponse) bool {
		return status.Complete == 2 && status.Total == 2
	})
}

func TestRetryRunIntegration_IdempotentWhileRunning(t *testing.T) {
	blockFirst := make(chan struct{})
	h := newIntegrationHarness(t, []runnerBehavior{{Block: blockFirst}})

	company := model.Company{URL: "https://retry-acme.com", Name: "Retry Acme"}
	originalRun, err := h.store.CreateRun(context.Background(), company)
	require.NoError(t, err)
	require.NoError(t, h.store.FailRun(context.Background(), originalRun.ID, &model.RunError{
		Message:     "boom",
		Category:    model.ErrorCategoryTransient,
		FailedPhase: "pipeline",
	}))

	requireQueueStatus(t, h)
	require.NoError(t, h.cache.Set(apicache.KeyQueueStatus, queueStatusResponse{Failed: 1, Total: 1}, time.Minute))

	first := postRetry(t, h, originalRun.ID)
	assert.False(t, first.Reused)
	assert.Equal(t, originalRun.ID, first.OriginalRunID)
	_, ok := h.cache.Get(apicache.KeyQueueStatus)
	assert.False(t, ok)

	waitForRuns(t, h, company.URL, 2)

	duplicate := postRetry(t, h, originalRun.ID)
	assert.True(t, duplicate.Reused)
	assert.Equal(t, first.WorkflowID, duplicate.WorkflowID)
	assert.Equal(t, first.WorkflowRunID, duplicate.WorkflowRunID)
	assert.Equal(t, originalRun.ID, duplicate.OriginalRunID)

	close(blockFirst)
	waitForQueueStatus(t, h, func(status queueStatusResponse) bool {
		return status.Failed == 1 && status.Complete == 1 && status.Total == 2
	})

	runs := requireRuns(t, h, company.URL)
	require.Len(t, runs.Runs, 2)
}

func TestFedsyncStatusesIntegration_RefreshesAfterSyncLogWrite(t *testing.T) {
	h := newIntegrationHarness(t, nil)

	initial := getFedsyncStatuses(t, h)
	require.NotEmpty(t, initial)
	require.NoError(t, h.cache.Set(apicache.KeyFedsyncStatuses, initial, time.Minute))

	syncID, err := h.syncLog.Start(context.Background(), "fpds")
	require.NoError(t, err)
	require.NoError(t, h.syncLog.Complete(context.Background(), syncID, &fedsync.SyncResult{
		RowsSynced: 42,
		Metadata: map[string]any{
			"source": "integration",
		},
	}))

	_, ok := h.cache.Get(apicache.KeyFedsyncStatuses)
	assert.False(t, ok)

	statuses := getFedsyncStatuses(t, h)
	fpds := findDatasetStatus(t, statuses, "fpds")
	assert.Equal(t, "complete", fpds.LastStatus)
	assert.EqualValues(t, 42, fpds.RowsSynced)
	require.NotNil(t, fpds.LastSync)
	assert.Equal(t, "integration", fpds.Metadata["source"])
}

func TestReadModelHotQueriesIntegration_UseMaterializedViews(t *testing.T) {
	h := newIntegrationHarness(t, nil)

	for _, datasetName := range []string{"fpds", "qcew"} {
		syncID, err := h.syncLog.Start(context.Background(), datasetName)
		require.NoError(t, err)
		require.NoError(t, h.syncLog.Complete(context.Background(), syncID, &fedsync.SyncResult{
			RowsSynced: 10,
		}))
	}

	statusPlan := explainPlan(t, h.baseStore.Pool(), `
		SELECT dataset, status, rows_synced, started_at, metadata
		FROM fed_data.mv_dataset_status_latest`)
	assert.Contains(t, statusPlan, "mv_dataset_status_latest")

	trendPlan := explainPlan(t, h.baseStore.Pool(), `
		SELECT sync_date, dataset, rows_synced
		FROM fed_data.mv_sync_daily_trends
		WHERE sync_date >= current_date - $1::int
		ORDER BY sync_date DESC, dataset`, 30)
	assert.Contains(t, trendPlan, "mv_sync_daily_trends")
}

func newIntegrationHarness(t *testing.T, behaviors []runnerBehavior) *integrationHarness {
	t.Helper()

	ctx := context.Background()
	dbURL := integrationDatabaseURL(t)
	resetIntegrationDatabase(t, ctx, dbURL)

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	dev := startTemporalDevServer(t, ctx)

	cache, err := apicache.NewRedis(apicache.RedisConfig{
		URL:            "redis://" + server.Addr(),
		KeyPrefix:      "itest:" + t.Name(),
		ConnectTimeout: time.Second,
	})
	require.NoError(t, err)

	baseStore, err := store.NewPostgres(ctx, dbURL, &store.PoolConfig{MaxConns: 4, MinConns: 1})
	require.NoError(t, err)

	wrappedStore := store.WithAPICache(baseStore, cache)
	syncLog := fedsync.NewSyncLog(baseStore.Pool())
	syncLog.SetCache(cache)

	runner := &integrationRunner{
		store:     wrappedStore,
		behaviors: behaviors,
		started:   make(chan string, 8),
	}

	w := worker.New(dev.Client(), temporalpkg.EnrichmentTaskQueue, worker.Options{})
	w.RegisterWorkflow(temporalenrich.EnrichCompanyWorkflow)
	w.RegisterActivity(temporalenrich.NewActivities(runner))
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	cfg := &config.Config{
		Temporal: config.TemporalConfig{
			HostPort:  dev.FrontendHostPort(),
			Namespace: "default",
		},
		Server: config.ServerConfig{
			HTTPCache: config.HTTPCacheConfig{
				Backend:            "redis",
				RedisURL:           "redis://" + server.Addr(),
				KeyPrefix:          "itest:" + t.Name(),
				ConnectTimeoutSecs: 1,
			},
		},
	}

	collector := monitoring.NewCollector(wrappedStore, syncLog)
	h := NewHandlers(cfg, wrappedStore, nil, collector, nil)
	h.SetCache(cache)
	h.SetReadModel(readmodel.NewPostgresService(baseStore.Pool(), cfg))
	h.SetTemporalClient(dev.Client())
	h.SetEnrichmentStarter(enrichmentstart.NewService(dev.Client()))

	httpServer := httptest.NewServer(Router(h))
	t.Cleanup(httpServer.Close)
	t.Cleanup(func() {
		require.NoError(t, wrappedStore.Close())
	})

	return &integrationHarness{
		store:     wrappedStore,
		baseStore: baseStore,
		cache:     cache,
		syncLog:   syncLog,
		server:    httpServer,
	}
}

func startTemporalDevServer(t *testing.T, ctx context.Context) *testsuite.DevServer {
	t.Helper()

	path, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI not found in PATH")
	}

	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:     path,
		LogLevel:         "error",
		LogFormat:        "json",
		SearchAttributes: temporalpkg.EnrichmentSearchAttributeDefinitions(),
		ClientOptions:    nil,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dev.Stop())
	})
	return dev
}

func integrationDatabaseURL(t *testing.T) string {
	t.Helper()

	dbURL := os.Getenv("RESEARCH_INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		t.Skip("RESEARCH_INTEGRATION_DATABASE_URL is not set")
	}
	return dbURL
}

func resetIntegrationDatabase(t *testing.T, ctx context.Context, dbURL string) {
	t.Helper()

	db, err := sql.Open("pgx", dbURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	_, err = db.ExecContext(ctx, `
		DROP SCHEMA IF EXISTS fed_data CASCADE;
		DROP SCHEMA IF EXISTS public CASCADE;
		CREATE SCHEMA public;
	`)
	require.NoError(t, err)
	require.NoError(t, migrate.Apply(ctx, dbURL))
}

func postWebhook(t *testing.T, h *integrationHarness, reqBody enrichRequest) workflowAcceptedResponse {
	t.Helper()

	var body bytes.Buffer
	require.NoError(t, json.NewEncoder(&body).Encode(reqBody))

	req, err := http.NewRequest(http.MethodPost, h.server.URL+"/api/v1/webhook/enrich", &body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	var payload workflowAcceptedResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.NotEmpty(t, resp.Header.Get("X-Request-Id"))
	return payload
}

func postRetry(t *testing.T, h *integrationHarness, runID string) workflowAcceptedResponse {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, h.server.URL+"/api/v1/runs/"+runID+"/retry", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	var payload workflowAcceptedResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.NotEmpty(t, resp.Header.Get("X-Request-Id"))
	return payload
}

func requireQueueStatus(t *testing.T, h *integrationHarness) queueStatusResponse {
	t.Helper()

	resp, err := http.Get(h.server.URL + "/api/v1/queue/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload queueStatusResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload
}

func waitForQueueStatus(t *testing.T, h *integrationHarness, cond func(queueStatusResponse) bool) queueStatusResponse {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		status := requireQueueStatus(t, h)
		if cond(status) {
			return status
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("queue status condition not met before timeout")
	return queueStatusResponse{}
}

func waitForRuns(t *testing.T, h *integrationHarness, companyURL string, expected int) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		runs := requireRuns(t, h, companyURL)
		if runs.Total == expected {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("expected %d runs for %s before timeout", expected, companyURL)
}

func requireRuns(t *testing.T, h *integrationHarness, companyURL string) listRunsResponse {
	t.Helper()

	resp, err := http.Get(h.server.URL + "/api/v1/runs?company_url=" + url.QueryEscape(companyURL))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload listRunsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload
}

func getFedsyncStatuses(t *testing.T, h *integrationHarness) []readmodel.DatasetStatus {
	t.Helper()

	resp, err := http.Get(h.server.URL + "/api/v1/fedsync/statuses")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload []readmodel.DatasetStatus
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload
}

func findDatasetStatus(t *testing.T, statuses []readmodel.DatasetStatus, name string) readmodel.DatasetStatus {
	t.Helper()

	for _, status := range statuses {
		if status.Name == name {
			return status
		}
	}
	t.Fatalf("dataset status %q not found", name)
	return readmodel.DatasetStatus{}
}

func explainPlan(t *testing.T, pool db.Pool, query string, args ...any) string {
	t.Helper()

	rows, err := pool.Query(context.Background(), "EXPLAIN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var buf bytes.Buffer
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	return buf.String()
}
