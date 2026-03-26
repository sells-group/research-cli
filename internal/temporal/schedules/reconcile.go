package schedules

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"

	"github.com/rotisserie/eris"
	enumspb "go.temporal.io/api/enums/v1"
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

const scheduleFingerprintMemoKey = "schedule_fingerprint"

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
			handle := c.ScheduleClient().GetHandle(ctx, s.ID)
			desc, err := handle.Describe(ctx)
			if err != nil {
				return nil, eris.Wrapf(err, "schedules: describe %s", s.ID)
			}

			if scheduleMatches(desc, s) {
				result.Unchanged = append(result.Unchanged, s.ID)
				delete(existing, s.ID)
				continue
			}

			if opts.DryRun {
				log.Info("would update schedule", zap.String("id", s.ID))
				result.Updated = append(result.Updated, s.ID)
				delete(existing, s.ID)
				continue
			}

			if err := handle.Update(ctx, client.ScheduleUpdateOptions{
				DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
					return &client.ScheduleUpdate{
						Schedule: desiredSchedule(s, currentScheduleState(input.Description, s)),
					}, nil
				},
			}); err != nil {
				return nil, eris.Wrapf(err, "schedules: update %s", s.ID)
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
	_, err := c.ScheduleClient().Create(ctx, scheduleOptions(s))
	return err
}

func scheduleOptions(s Schedule) client.ScheduleOptions {
	return client.ScheduleOptions{
		ID:      s.ID,
		Spec:    desiredSpec(s),
		Action:  desiredWorkflowAction(s),
		Overlap: resolvedOverlap(s.Overlap),
		Paused:  s.Paused,
	}
}

func desiredSchedule(s Schedule, state client.ScheduleState) *client.Schedule {
	return &client.Schedule{
		Action: desiredWorkflowAction(s),
		Spec:   desiredScheduleSpec(s),
		Policy: &client.SchedulePolicies{
			Overlap: resolvedOverlap(s.Overlap),
		},
		State: &state,
	}
}

func desiredWorkflowAction(s Schedule) *client.ScheduleWorkflowAction {
	fingerprint := scheduleFingerprint(s)
	return &client.ScheduleWorkflowAction{
		ID:       desiredActionID(s),
		Workflow: s.Workflow,
		Args:     s.Args,
		Memo: map[string]interface{}{
			scheduleFingerprintMemoKey: fingerprint,
		},
		TaskQueue: s.TaskQueue,
	}
}

func desiredSpec(s Schedule) client.ScheduleSpec {
	return client.ScheduleSpec{
		CronExpressions: []string{s.Cron},
		TimeZoneName:    "UTC",
	}
}

func desiredScheduleSpec(s Schedule) *client.ScheduleSpec {
	spec := desiredSpec(s)
	return &spec
}

func resolvedOverlap(overlap enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if overlap == 0 {
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return overlap
}

func currentScheduleState(desc client.ScheduleDescription, fallback Schedule) client.ScheduleState {
	state := client.ScheduleState{Paused: fallback.Paused}
	if desc.Schedule.State != nil {
		state.Paused = desc.Schedule.State.Paused
		state.Note = desc.Schedule.State.Note
		state.LimitedActions = desc.Schedule.State.LimitedActions
		state.RemainingActions = desc.Schedule.State.RemainingActions
	}
	return state
}

func scheduleMatches(desc *client.ScheduleDescription, s Schedule) bool {
	if desc == nil {
		return false
	}

	action, ok := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || action == nil {
		return false
	}

	expected := scheduleFingerprint(s)
	if fingerprint, ok := action.Memo[scheduleFingerprintMemoKey].(string); ok {
		return fingerprint == expected
	}

	return action.ID == desiredActionID(s)
}

func desiredActionID(s Schedule) string {
	fingerprint := scheduleFingerprint(s)
	return fmt.Sprintf("sched-%s-%s", s.ID, fingerprint[:12])
}

func scheduleFingerprint(s Schedule) string {
	type fingerprintPayload struct {
		Cron      string                        `json:"cron"`
		TaskQueue string                        `json:"task_queue"`
		Workflow  string                        `json:"workflow"`
		Args      []interface{}                 `json:"args"`
		Overlap   enumspb.ScheduleOverlapPolicy `json:"overlap"`
	}

	payload, err := json.Marshal(fingerprintPayload{
		Cron:      s.Cron,
		TaskQueue: s.TaskQueue,
		Workflow:  workflowIdentity(s.Workflow),
		Args:      s.Args,
		Overlap:   resolvedOverlap(s.Overlap),
	})
	if err != nil {
		payload = []byte(fmt.Sprintf("%s|%s|%s|%#v|%d", s.Cron, s.TaskQueue, workflowIdentity(s.Workflow), s.Args, resolvedOverlap(s.Overlap)))
	}

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func workflowIdentity(workflow interface{}) string {
	if workflow == nil {
		return ""
	}
	if workflowName, ok := workflow.(string); ok {
		return workflowName
	}

	value := reflect.ValueOf(workflow)
	if value.Kind() == reflect.Func {
		if fn := runtime.FuncForPC(value.Pointer()); fn != nil {
			return fn.Name()
		}
	}

	return fmt.Sprintf("%T", workflow)
}
