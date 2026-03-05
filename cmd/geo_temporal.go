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
	temporalgeoscraper "github.com/sells-group/research-cli/internal/temporal/geoscraper"
	temporaltigerload "github.com/sells-group/research-cli/internal/temporal/tigerload"
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

// runGeoScrapeViaTemporal starts a ScrapeWorkflow via Temporal.
func runGeoScrapeViaTemporal(ctx context.Context, cmd *cobra.Command) error {
	c, err := temporalpkg.NewClient(cfg.Temporal)
	if err != nil {
		return err
	}
	defer c.Close()

	categoryStr, _ := cmd.Flags().GetString("category")
	sourcesStr, _ := cmd.Flags().GetString("sources")
	statesStr, _ := cmd.Flags().GetString("states")
	force, _ := cmd.Flags().GetBool("force")

	params := temporalgeoscraper.ScrapeParams{
		Force: force,
	}
	if categoryStr != "" {
		params.Category = &categoryStr
	}
	if sourcesStr != "" {
		params.Sources = splitAndTrim(sourcesStr)
	}
	if statesStr != "" {
		params.States = splitAndTrim(statesStr)
	}

	workflowID := fmt.Sprintf("geo-scrape-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporalpkg.GeoTaskQueue,
	}, temporalgeoscraper.ScrapeWorkflow, params)
	if err != nil {
		return eris.Wrap(err, "start geo scrape workflow")
	}

	zap.L().Info("geo scrape workflow started",
		zap.String("workflow_id", run.GetID()),
		zap.String("run_id", run.GetRunID()),
	)

	var result temporalgeoscraper.ScrapeResult
	if err := run.Get(ctx, &result); err != nil {
		return eris.Wrap(err, "geo scrape workflow failed")
	}

	fmt.Printf("Geo scrape complete: %d synced, %d failed\n", result.Synced, result.Failed)
	for _, o := range result.Outcomes {
		if o.Status == "complete" {
			fmt.Printf("  %s: %d rows\n", o.Scraper, o.RowsSynced)
		} else {
			fmt.Printf("  %s: FAILED (%s)\n", o.Scraper, o.Error)
		}
	}
	return nil
}

// runTigerLoadViaTemporal starts a tigerload.Workflow via Temporal.
func runTigerLoadViaTemporal(ctx context.Context, cmd *cobra.Command) error {
	c, err := temporalpkg.NewClient(cfg.Temporal)
	if err != nil {
		return err
	}
	defer c.Close()

	statesStr, _ := cmd.Flags().GetString("states")
	tablesStr, _ := cmd.Flags().GetString("tables")
	year, _ := cmd.Flags().GetInt("year")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	incremental, _ := cmd.Flags().GetBool("incremental")

	params := temporaltigerload.Params{
		Year:        year,
		Concurrency: concurrency,
		Incremental: incremental,
	}

	if statesStr != "" {
		params.States = toUpper(splitAndTrim(statesStr))
	} else if len(cfg.Tiger.States) > 0 {
		params.States = cfg.Tiger.States
	}

	if tablesStr != "" {
		params.Tables = toUpper(splitAndTrim(tablesStr))
	} else if len(cfg.Tiger.Tables) > 0 {
		params.Tables = cfg.Tiger.Tables
	}

	// Apply config defaults.
	if params.Year == 0 {
		params.Year = cfg.Tiger.Year
	}
	if params.Concurrency == 0 {
		params.Concurrency = cfg.Tiger.Concurrency
	}

	workflowID := fmt.Sprintf("tiger-load-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporalpkg.GeoTaskQueue,
	}, temporaltigerload.Workflow, params)
	if err != nil {
		return eris.Wrap(err, "start tiger load workflow")
	}

	zap.L().Info("tiger load workflow started",
		zap.String("workflow_id", run.GetID()),
		zap.String("run_id", run.GetRunID()),
	)

	var result temporaltigerload.Result
	if err := run.Get(ctx, &result); err != nil {
		return eris.Wrap(err, "tiger load workflow failed")
	}

	fmt.Printf("TIGER load complete: %d national, %d states loaded, %d failed\n",
		result.National, result.Loaded, result.Failed)
	for _, o := range result.Outcomes {
		if o.Status == "complete" {
			fmt.Printf("  %s: %d rows\n", o.State, o.RowsLoaded)
		} else {
			fmt.Printf("  %s: FAILED (%s)\n", o.State, o.Error)
		}
	}
	return nil
}
