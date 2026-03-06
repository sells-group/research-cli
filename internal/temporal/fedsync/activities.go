// Package fedsync provides Temporal workflows and activities for the fedsync subsystem.
package fedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/temporal/sdk"
)

// Activities holds dependencies for fedsync Temporal activities.
type Activities struct {
	sdk.SyncLogActivities

	pool    db.Pool
	fetcher fetcher.Fetcher
	reg     *dataset.Registry
	tempDir string
	cfg     *config.Config
}

// NewActivities creates a new fedsync Activities instance.
func NewActivities(pool db.Pool, f fetcher.Fetcher, syncLog *fedsync.SyncLog, reg *dataset.Registry, tempDir string, cfg *config.Config) *Activities {
	return &Activities{
		SyncLogActivities: sdk.SyncLogActivities{SyncLog: syncLog},
		pool:              pool,
		fetcher:           f,
		reg:               reg,
		tempDir:           tempDir,
		cfg:               cfg,
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
		lastSync, err := a.SyncLog.LastSuccess(ctx, ds.Name())
		if err != nil {
			return nil, eris.Wrapf(err, "check last sync for %s", ds.Name())
		}
		if ds.ShouldRun(now, lastSync) {
			names = append(names, ds.Name())
		}
	}

	return &SelectDatasetsResult{DatasetNames: names}, nil
}

// SyncDatasetParams is the input for SyncDataset.
type SyncDatasetParams struct {
	Dataset string `json:"dataset"`
	Full    bool   `json:"full"`
}

// SyncDatasetResult is the output of SyncDataset.
type SyncDatasetResult = sdk.SyncItemResult

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

	var result *dataset.SyncResult
	syncErr := sdk.RunWithHeartbeat(ctx, fmt.Sprintf("syncing %s", params.Dataset), 30*time.Second, func(ctx context.Context) error {
		var err error
		if params.Full {
			if fs, ok := ds.(dataset.FullSyncer); ok {
				log.Info("running full sync via Temporal")
				result, err = fs.SyncFull(ctx, a.pool, a.fetcher, a.tempDir)
				return err
			}
		}
		result, err = ds.Sync(ctx, a.pool, a.fetcher, a.tempDir)
		return err
	})

	if syncErr != nil {
		return nil, eris.Wrapf(syncErr, "sync dataset %s", params.Dataset)
	}

	return &SyncDatasetResult{
		RowsSynced: result.RowsSynced,
		Metadata:   result.Metadata,
	}, nil
}
