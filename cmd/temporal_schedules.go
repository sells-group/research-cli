package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	"github.com/sells-group/research-cli/internal/temporal/schedules"
)

var temporalSchedulesCmd = &cobra.Command{
	Use:   "temporal-schedules",
	Short: "Manage Temporal schedules for all workflow domains",
}

var schedulesReconcileCmd = &cobra.Command{
	Use:   "reconcile",
	Short: "Create/update all Temporal schedules to match desired state",
	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		prune, _ := cmd.Flags().GetBool("prune")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

		desired := schedules.AllSchedules()

		result, err := schedules.Reconcile(context.Background(), c, desired, schedules.ReconcileOpts{
			Prune:  prune,
			DryRun: dryRun,
		})
		if err != nil {
			return err
		}

		prefix := ""
		if dryRun {
			prefix = "(dry-run) "
		}

		fmt.Printf("%sSchedule reconciliation complete:\n", prefix)
		fmt.Printf("  Created:   %d %v\n", len(result.Created), result.Created)
		fmt.Printf("  Updated:   %d %v\n", len(result.Updated), result.Updated)
		fmt.Printf("  Deleted:   %d %v\n", len(result.Deleted), result.Deleted)
		fmt.Printf("  Unchanged: %d %v\n", len(result.Unchanged), result.Unchanged)

		return nil
	},
}

var schedulesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List current Temporal schedules with status",
	RunE: func(_ *cobra.Command, _ []string) error {
		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		iter, err := c.ScheduleClient().List(context.Background(), client.ScheduleListOptions{})
		if err != nil {
			return err
		}

		count := 0
		for iter.HasNext() {
			entry, err := iter.Next()
			if err != nil {
				return err
			}
			fmt.Printf("%-30s\n", entry.ID)
			count++
		}

		if count == 0 {
			fmt.Println("No schedules found.")
		} else {
			fmt.Printf("\n%d schedule(s) total\n", count)
		}

		return nil
	},
}

var schedulesPauseCmd = &cobra.Command{
	Use:   "pause <schedule-id>",
	Short: "Pause a schedule",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		handle := c.ScheduleClient().GetHandle(context.Background(), args[0])
		if err := handle.Pause(context.Background(), client.SchedulePauseOptions{Note: "paused via CLI"}); err != nil {
			return err
		}

		zap.L().Info("schedule paused", zap.String("id", args[0]))
		fmt.Printf("Paused schedule: %s\n", args[0])
		return nil
	},
}

var schedulesUnpauseCmd = &cobra.Command{
	Use:   "unpause <schedule-id>",
	Short: "Unpause a schedule",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		handle := c.ScheduleClient().GetHandle(context.Background(), args[0])
		if err := handle.Unpause(context.Background(), client.ScheduleUnpauseOptions{Note: "unpaused via CLI"}); err != nil {
			return err
		}

		zap.L().Info("schedule unpaused", zap.String("id", args[0]))
		fmt.Printf("Unpaused schedule: %s\n", args[0])
		return nil
	},
}

var schedulesTriggerCmd = &cobra.Command{
	Use:   "trigger <schedule-id>",
	Short: "Manually trigger a schedule now",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		c, err := temporalpkg.NewClient(cfg.Temporal)
		if err != nil {
			return err
		}
		defer c.Close()

		handle := c.ScheduleClient().GetHandle(context.Background(), args[0])
		if err := handle.Trigger(context.Background(), client.ScheduleTriggerOptions{}); err != nil {
			return err
		}

		zap.L().Info("schedule triggered", zap.String("id", args[0]))
		fmt.Printf("Triggered schedule: %s\n", args[0])
		return nil
	},
}

func init() {
	schedulesReconcileCmd.Flags().Bool("prune", false, "delete schedules not in desired list")
	schedulesReconcileCmd.Flags().Bool("dry-run", false, "preview changes without applying")

	temporalSchedulesCmd.AddCommand(schedulesReconcileCmd)
	temporalSchedulesCmd.AddCommand(schedulesListCmd)
	temporalSchedulesCmd.AddCommand(schedulesPauseCmd)
	temporalSchedulesCmd.AddCommand(schedulesUnpauseCmd)
	temporalSchedulesCmd.AddCommand(schedulesTriggerCmd)
	rootCmd.AddCommand(temporalSchedulesCmd)
}
