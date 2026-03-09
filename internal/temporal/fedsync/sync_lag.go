package fedsync

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fedsync/dataset"
)

// SyncLagResult is the output of CheckSyncLag.
type SyncLagResult struct {
	Overdue []OverdueDataset `json:"overdue"`
}

// OverdueDataset describes a dataset that's behind schedule.
type OverdueDataset struct {
	Name     string     `json:"name"`
	Cadence  string     `json:"cadence"`
	LastSync *time.Time `json:"last_sync,omitempty"`
	Message  string     `json:"message"`
}

// CheckSyncLag inspects all registered datasets and returns those significantly overdue.
func (a *Activities) CheckSyncLag(ctx context.Context) (*SyncLagResult, error) {
	now := time.Now().UTC()
	log := zap.L().With(zap.String("activity", "check_sync_lag"))

	var overdue []OverdueDataset
	for _, ds := range a.reg.All() {
		lastSync, err := a.SyncLog.LastSuccess(ctx, ds.Name())
		if err != nil {
			log.Warn("failed to check last sync", zap.String("dataset", ds.Name()), zap.Error(err))
			continue
		}

		if !ds.ShouldRun(now, lastSync) {
			continue
		}

		// If ShouldRun is true but we have a recent sync, it's just due.
		// Only flag as overdue if the lag exceeds 2x the cadence.
		if lastSync != nil && !isSignificantlyOverdue(ds, now, *lastSync) {
			continue
		}

		od := OverdueDataset{
			Name:     ds.Name(),
			Cadence:  string(ds.Cadence()),
			LastSync: lastSync,
		}
		if lastSync == nil {
			od.Message = fmt.Sprintf("%s has never been synced", ds.Name())
		} else {
			od.Message = fmt.Sprintf("%s last synced %s ago (cadence: %s)", ds.Name(), now.Sub(*lastSync).Round(time.Hour), ds.Cadence())
		}
		overdue = append(overdue, od)
	}

	log.Info("sync lag check complete", zap.Int("overdue", len(overdue)))
	return &SyncLagResult{Overdue: overdue}, nil
}

// isSignificantlyOverdue returns true if a dataset is more than 2x overdue.
func isSignificantlyOverdue(ds dataset.Dataset, now time.Time, lastSync time.Time) bool {
	elapsed := now.Sub(lastSync)
	switch ds.Cadence() {
	case dataset.Daily:
		return elapsed > 48*time.Hour
	case dataset.Weekly:
		return elapsed > 14*24*time.Hour
	case dataset.Monthly:
		return elapsed > 60*24*time.Hour
	case dataset.Quarterly:
		return elapsed > 180*24*time.Hour
	case dataset.Annual:
		return elapsed > 730*24*time.Hour
	default:
		return false
	}
}
