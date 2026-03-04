package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalgeo "github.com/sells-group/research-cli/internal/temporal/geo"
)

// runGeoBackfillViaTemporal starts a GeoBackfillWorkflow for the given source.
func runGeoBackfillViaTemporal(ctx context.Context, cmd *cobra.Command, source string) error {
	c, err := temporalpkg.NewClient(cfg.Temporal)
	if err != nil {
		return err
	}
	defer c.Close()

	limit, _ := cmd.Flags().GetInt("limit")
	batchSize, _ := cmd.Flags().GetInt("batch-size")
	skipMSA, _ := cmd.Flags().GetBool("skip-msa")

	params := temporalgeo.BackfillParams{
		Source:    source,
		Limit:     limit,
		BatchSize: batchSize,
		SkipMSA:   skipMSA,
	}

	workflowID := fmt.Sprintf("geo-backfill-%s-%d", source, time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporalpkg.GeoTaskQueue,
	}, temporalgeo.BackfillWorkflow, params)
	if err != nil {
		return eris.Wrapf(err, "start geo backfill %s workflow", source)
	}

	zap.L().Info("geo backfill workflow started",
		zap.String("source", source),
		zap.String("workflow_id", run.GetID()),
		zap.String("run_id", run.GetRunID()),
	)

	var result temporalgeo.BackfillResult
	if err := run.Get(ctx, &result); err != nil {
		return eris.Wrapf(err, "geo backfill %s workflow failed", source)
	}

	fmt.Printf("%s backfill complete: %d created, %d geocoded, %d linked, %d branches, %d MSA associations, %d failed\n",
		source, result.Created, result.Geocoded, result.Linked, result.Branches, result.MSAs, result.Failed)
	return nil
}
