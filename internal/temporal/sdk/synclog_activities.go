package sdk

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/fedsync"
)

// SyncLogActivities provides sync log lifecycle activities shared across domains.
type SyncLogActivities struct {
	SyncLog *fedsync.SyncLog
}

// StartSyncLogParams is the input for StartSyncLog.
type StartSyncLogParams struct {
	Name string `json:"name"`
}

// StartSyncLogResult is the output of StartSyncLog.
type StartSyncLogResult struct {
	SyncID int64 `json:"sync_id"`
}

// CompleteSyncLogParams is the input for CompleteSyncLog.
type CompleteSyncLogParams struct {
	SyncID     int64          `json:"sync_id"`
	RowsSynced int64          `json:"rows_synced"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// FailSyncLogParams is the input for FailSyncLog.
type FailSyncLogParams struct {
	SyncID int64  `json:"sync_id"`
	Error  string `json:"error"`
}

// StartSyncLog records the beginning of a sync run.
func (a *SyncLogActivities) StartSyncLog(ctx context.Context, params StartSyncLogParams) (*StartSyncLogResult, error) {
	syncID, err := a.SyncLog.Start(ctx, params.Name)
	if err != nil {
		return nil, eris.Wrapf(err, "start sync log for %s", params.Name)
	}
	return &StartSyncLogResult{SyncID: syncID}, nil
}

// CompleteSyncLog marks a sync run as successfully completed.
func (a *SyncLogActivities) CompleteSyncLog(ctx context.Context, params CompleteSyncLogParams) error {
	return a.SyncLog.Complete(ctx, params.SyncID, &fedsync.SyncResult{
		RowsSynced: params.RowsSynced,
		Metadata:   params.Metadata,
	})
}

// FailSyncLog marks a sync run as failed.
func (a *SyncLogActivities) FailSyncLog(ctx context.Context, params FailSyncLogParams) error {
	return a.SyncLog.Fail(ctx, params.SyncID, params.Error)
}
