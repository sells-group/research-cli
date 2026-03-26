//go:build integration

package main

import (
	"bytes"
	"context"
	"os/exec"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/sells-group/research-cli/internal/config"
)

func TestTemporalSchedulesCommandsIntegration(t *testing.T) {
	ctx := context.Background()
	dev := startTemporalSchedulesDevServer(t, ctx)
	client := dev.Client()

	prevCfg := cfg
	cfg = &config.Config{
		Temporal: config.TemporalConfig{
			HostPort:  dev.FrontendHostPort(),
			Namespace: "default",
		},
	}
	t.Cleanup(func() {
		cfg = prevCfg
	})

	reconcileOutput := runScheduleCommand(t, schedulesReconcileCmd, nil, func(cmd *cobra.Command) {
		require.NoError(t, cmd.Flags().Set("prune", "true"))
		require.NoError(t, cmd.Flags().Set("dry-run", "false"))
	})
	assert.Contains(t, reconcileOutput, "Schedule reconciliation complete")
	assert.Contains(t, reconcileOutput, "fedsync-daily")

	listOutput := runScheduleCommand(t, schedulesListCmd, nil, nil)
	assert.Contains(t, listOutput, "fedsync-daily")
	assert.Contains(t, listOutput, "geo-national")

	runScheduleCommand(t, schedulesPauseCmd, []string{"fedsync-daily"}, nil)
	requireSchedulePausedState(t, client, "fedsync-daily", true)

	runScheduleCommand(t, schedulesUnpauseCmd, []string{"fedsync-daily"}, nil)
	requireSchedulePausedState(t, client, "fedsync-daily", false)

	triggerOutput := runScheduleCommand(t, schedulesTriggerCmd, []string{"fedsync-daily"}, nil)
	assert.Contains(t, triggerOutput, "Triggered schedule: fedsync-daily")
}

func startTemporalSchedulesDevServer(t *testing.T, ctx context.Context) *testsuite.DevServer {
	t.Helper()

	path, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("temporal CLI not found in PATH")
	}

	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: path,
		LogLevel:     "error",
		LogFormat:    "json",
		ClientOptions: &client.Options{
			Namespace: "default",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dev.Stop())
	})
	return dev
}

func runScheduleCommand(
	t *testing.T,
	cmd *cobra.Command,
	args []string,
	configure func(*cobra.Command),
) string {
	t.Helper()

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if configure != nil {
		configure(cmd)
	}

	require.NoError(t, cmd.RunE(cmd, args))
	return out.String()
}

func requireSchedulePausedState(t *testing.T, c client.Client, id string, expected bool) {
	t.Helper()

	desc, err := c.ScheduleClient().GetHandle(context.Background(), id).Describe(context.Background())
	require.NoError(t, err)
	require.NotNil(t, desc.Schedule)
	require.NotNil(t, desc.Schedule.State)
	assert.Equal(t, expected, desc.Schedule.State.Paused)
}
