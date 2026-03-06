package schedules

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
)

// ReconcileOpts configures schedule reconciliation behavior.
type ReconcileOpts struct {
	Prune  bool // delete schedules not in desired list
	DryRun bool // log changes without applying
}

// ReconcileResult summarizes what reconcile did.
type ReconcileResult struct {
	Created   []string
	Updated   []string
	Deleted   []string
	Unchanged []string
}

// Reconcile ensures Temporal schedules match the desired state.
// Creates missing schedules, updates changed ones (cron/args), and optionally
// deletes unmanaged schedules.
func Reconcile(ctx context.Context, c client.Client, desired []Schedule, opts ReconcileOpts) (*ReconcileResult, error) {
	log := zap.L().With(zap.Bool("dry_run", opts.DryRun))
	result := &ReconcileResult{}

	// Build desired map.
	desiredMap := make(map[string]Schedule)
	for _, s := range desired {
		desiredMap[s.ID] = s
	}

	// List existing schedules.
	existing := make(map[string]bool)
	iter, err := c.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	if err != nil {
		return nil, eris.Wrap(err, "schedules: list existing")
	}
	for iter.HasNext() {
		entry, err := iter.Next()
		if err != nil {
			return nil, eris.Wrap(err, "schedules: iterate")
		}
		existing[entry.ID] = true
	}

	// Create or update desired schedules.
	for _, s := range desired {
		if existing[s.ID] {
			// Update: delete and recreate (Temporal SDK doesn't support Update easily).
			if opts.DryRun {
				log.Info("would update schedule", zap.String("id", s.ID))
				result.Updated = append(result.Updated, s.ID)
				continue
			}

			handle := c.ScheduleClient().GetHandle(ctx, s.ID)
			if err := handle.Delete(ctx); err != nil {
				return nil, eris.Wrapf(err, "schedules: delete %s for update", s.ID)
			}

			if err := createSchedule(ctx, c, s); err != nil {
				return nil, eris.Wrapf(err, "schedules: recreate %s", s.ID)
			}
			log.Info("updated schedule", zap.String("id", s.ID))
			result.Updated = append(result.Updated, s.ID)
		} else {
			// Create.
			if opts.DryRun {
				log.Info("would create schedule", zap.String("id", s.ID))
				result.Created = append(result.Created, s.ID)
				continue
			}

			if err := createSchedule(ctx, c, s); err != nil {
				return nil, eris.Wrapf(err, "schedules: create %s", s.ID)
			}
			log.Info("created schedule", zap.String("id", s.ID))
			result.Created = append(result.Created, s.ID)
		}
		delete(existing, s.ID)
	}

	// Handle remaining existing schedules (not in desired list).
	for id := range existing {
		if opts.Prune {
			if opts.DryRun {
				log.Info("would delete unmanaged schedule", zap.String("id", id))
				result.Deleted = append(result.Deleted, id)
				continue
			}

			handle := c.ScheduleClient().GetHandle(ctx, id)
			if err := handle.Delete(ctx); err != nil {
				// Ignore not-found errors — schedule may have been deleted concurrently.
				if _, ok := err.(*serviceerror.NotFound); !ok {
					return nil, eris.Wrapf(err, "schedules: delete unmanaged %s", id)
				}
			}
			log.Info("deleted unmanaged schedule", zap.String("id", id))
			result.Deleted = append(result.Deleted, id)
		} else {
			result.Unchanged = append(result.Unchanged, id)
		}
	}

	return result, nil
}

func createSchedule(ctx context.Context, c client.Client, s Schedule) error {
	overlap := s.Overlap
	if overlap == 0 {
		overlap = 1 // SCHEDULE_OVERLAP_POLICY_SKIP
	}

	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: s.ID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{s.Cron},
			TimeZoneName:    "UTC",
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        fmt.Sprintf("sched-%s", s.ID),
			Workflow:  s.Workflow,
			Args:      s.Args,
			TaskQueue: s.TaskQueue,
		},
		Overlap: overlap,
		Paused:  s.Paused,
	})
	return err
}
