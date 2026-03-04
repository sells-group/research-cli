// Package fedsync provides Temporal workflows and activities for the fedsync subsystem.
package fedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
)

// Activities holds dependencies for fedsync Temporal activities.
type Activities struct {
	pool    db.Pool
	fetcher fetcher.Fetcher
	syncLog *fedsync.SyncLog
	reg     *dataset.Registry
	tempDir string
	cfg     *config.Config
}

// NewActivities creates a new fedsync Activities instance.
func NewActivities(pool db.Pool, f fetcher.Fetcher, syncLog *fedsync.SyncLog, reg *dataset.Registry, tempDir string, cfg *config.Config) *Activities {
	return &Activities{
		pool:    pool,
		fetcher: f,
		syncLog: syncLog,
		reg:     reg,
		tempDir: tempDir,
		cfg:     cfg,
	}
}

// SelectDatasetsParams is the input for SelectDatasets.
type SelectDatasetsParams struct {
	Phase    *string  `json:"phase,omitempty"`
	Datasets []string `json:"datasets,omitempty"`
	Force    bool     `json:"force"`
}

// SelectDatasetsResult is the output of SelectDatasets.
type SelectDatasetsResult struct {
	DatasetNames []string `json:"dataset_names"`
}

// SelectDatasets determines which datasets need syncing based on phase, names, and scheduling.
func (a *Activities) SelectDatasets(ctx context.Context, params SelectDatasetsParams) (*SelectDatasetsResult, error) {
	now := time.Now().UTC()

	var phase *dataset.Phase
	if params.Phase != nil {
		p, err := dataset.ParsePhase(*params.Phase)
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidPhase", err)
		}
		phase = &p
	}

	datasets, err := a.reg.Select(phase, params.Datasets)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "SelectionError", err)
	}

	var names []string
	for _, ds := range datasets {
		if params.Force {
			names = append(names, ds.Name())
			continue
		}
		lastSync, err := a.syncLog.LastSuccess(ctx, ds.Name())
		if err != nil {
			return nil, eris.Wrapf(err, "check last sync for %s", ds.Name())
		}
		if ds.ShouldRun(now, lastSync) {
			names = append(names, ds.Name())
		}
	}

	return &SelectDatasetsResult{DatasetNames: names}, nil
}

// StartSyncLogParams is the input for StartSyncLog.
type StartSyncLogParams struct {
	Dataset string `json:"dataset"`
}

// StartSyncLogResult is the output of StartSyncLog.
type StartSyncLogResult struct {
	SyncID int64 `json:"sync_id"`
}

// StartSyncLog records the beginning of a dataset sync run.
func (a *Activities) StartSyncLog(ctx context.Context, params StartSyncLogParams) (*StartSyncLogResult, error) {
	syncID, err := a.syncLog.Start(ctx, params.Dataset)
	if err != nil {
		return nil, eris.Wrapf(err, "start sync log for %s", params.Dataset)
	}
	return &StartSyncLogResult{SyncID: syncID}, nil
}

// SyncDatasetParams is the input for SyncDataset.
type SyncDatasetParams struct {
	Dataset string `json:"dataset"`
	Full    bool   `json:"full"`
}

// SyncDatasetResult is the output of SyncDataset.
type SyncDatasetResult struct {
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// SyncDataset runs the actual data download, parse, and load for a single dataset.
// It sends heartbeats every 30 seconds for liveness detection.
func (a *Activities) SyncDataset(ctx context.Context, params SyncDatasetParams) (*SyncDatasetResult, error) {
	log := zap.L().With(zap.String("dataset", params.Dataset))

	ds, lookupErr := a.reg.Get(params.Dataset)
	if lookupErr != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unknown dataset: %s", params.Dataset),
			"UnknownDataset", lookupErr)
	}

	// Start heartbeat goroutine.
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, fmt.Sprintf("syncing %s", params.Dataset))
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	defer close(heartbeatDone)

	var result *dataset.SyncResult
	var err error

	if params.Full {
		if fs, ok := ds.(dataset.FullSyncer); ok {
			log.Info("running full sync via Temporal")
			result, err = fs.SyncFull(ctx, a.pool, a.fetcher, a.tempDir)
		} else {
			result, err = ds.Sync(ctx, a.pool, a.fetcher, a.tempDir)
		}
	} else {
		result, err = ds.Sync(ctx, a.pool, a.fetcher, a.tempDir)
	}

	if err != nil {
		return nil, eris.Wrapf(err, "sync dataset %s", params.Dataset)
	}

	return &SyncDatasetResult{
		RowsSynced: result.RowsSynced,
		Metadata:   result.Metadata,
	}, nil
}

// CompleteSyncLogParams is the input for CompleteSyncLog.
type CompleteSyncLogParams struct {
	SyncID     int64          `json:"sync_id"`
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// CompleteSyncLog marks a sync run as successfully completed.
func (a *Activities) CompleteSyncLog(ctx context.Context, params CompleteSyncLogParams) error {
	return a.syncLog.Complete(ctx, params.SyncID, &fedsync.SyncResult{
		RowsSynced: params.RowsSynced,
		Metadata:   params.Metadata,
	})
}

// FailSyncLogParams is the input for FailSyncLog.
type FailSyncLogParams struct {
	SyncID int64  `json:"sync_id"`
	Error  string `json:"error"`
}

// FailSyncLog marks a sync run as failed.
func (a *Activities) FailSyncLog(ctx context.Context, params FailSyncLogParams) error {
	return a.syncLog.Fail(ctx, params.SyncID, params.Error)
}
