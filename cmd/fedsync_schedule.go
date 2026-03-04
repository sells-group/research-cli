package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalfedsync "github.com/sells-group/research-cli/internal/temporal/fedsync"
)

type scheduleSpec struct {
	ID        string
	Cron      string
	Phase     string
	TaskQueue string
}

var fedsyncScheduleCmd = &cobra.Command{
	Use:   "schedule",
	Short: "Manage Temporal schedules for fedsync phases",
	Long: `Creates or updates 4 Temporal Schedules (one per fedsync phase).
Each schedule triggers daily; ShouldRun() inside the workflow still
determines which datasets actually sync.

  fedsync-phase1  — 2:00 AM UTC (Market Intelligence)
  fedsync-phase1b — 3:00 AM UTC (SEC/EDGAR)
  fedsync-phase2  — 4:00 AM UTC (Extended)
  fedsync-phase3  — 5:00 AM UTC (On-Demand)`,
	RunE: func(_ *cobra.Command, _ []string) error {
		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		schedules := []scheduleSpec{
			{ID: "fedsync-phase1", Cron: "0 2 * * *", Phase: "1", TaskQueue: temporalpkg.FedsyncTaskQueue},
			{ID: "fedsync-phase1b", Cron: "0 3 * * *", Phase: "1b", TaskQueue: temporalpkg.FedsyncTaskQueue},
			{ID: "fedsync-phase2", Cron: "0 4 * * *", Phase: "2", TaskQueue: temporalpkg.FedsyncTaskQueue},
			{ID: "fedsync-phase3", Cron: "0 5 * * *", Phase: "3", TaskQueue: temporalpkg.FedsyncTaskQueue},
		}

		ctx := context.Background()

		for _, spec := range schedules {
			phase := spec.Phase
			_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
				ID: spec.ID,
				Spec: client.ScheduleSpec{
					CronExpressions: []string{spec.Cron},
					TimeZoneName:    "UTC",
				},
				Action: &client.ScheduleWorkflowAction{
					ID:        fmt.Sprintf("fedsync-run-%s", spec.ID),
					Workflow:  temporalfedsync.RunWorkflow,
					Args:      []interface{}{temporalfedsync.RunParams{Phase: &phase}},
					TaskQueue: spec.TaskQueue,
				},
				Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				Paused:  false,
			})
			if err != nil {
				// If schedule already exists, log and continue.
				zap.L().Warn("schedule create failed (may already exist)",
					zap.String("schedule_id", spec.ID),
					zap.Error(err),
				)
				continue
			}

			zap.L().Info("schedule created",
				zap.String("schedule_id", spec.ID),
				zap.String("cron", spec.Cron),
				zap.String("phase", spec.Phase),
			)
		}

		fmt.Printf("Fedsync schedules configured (%s)\n", time.Now().Format(time.RFC3339))
		return nil
	},
}

func init() {
	fedsyncCmd.AddCommand(fedsyncScheduleCmd)
}
