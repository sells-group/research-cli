package main

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
	sfpkg "github.com/sells-group/research-cli/pkg/salesforce"
)

var (
	sfreportReportID    string
	sfreportLimit       int
	sfreportConcurrency int
	sfreportForce       bool
	sfreportDryRun      bool
	sfreportSandbox     bool
)

var sfreportCmd = &cobra.Command{
	Use:   "sfreport",
	Short: "Enrich accounts from a Salesforce report",
	Long: `Fetches accounts from a Salesforce report via the Analytics API,
runs the enrichment pipeline on each account, and pushes results back to Salesforce.

Examples:
  # Dry run — fetch report and print accounts
  research-cli sfreport --report-id 00OPo00000g8kDcMAI --dry-run

  # Enrich first 5 accounts
  research-cli sfreport --report-id 00OPo00000g8kDcMAI --limit 5

  # Full batch with higher concurrency
  research-cli sfreport --report-id 00OPo00000g8kDcMAI --concurrency 5`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		if shouldUseTemporal(cmd) && !sfreportDryRun {
			return runSFReportViaTemporal(ctx, cmd)
		}

		// Swap to sandbox credentials if requested.
		if sfreportSandbox {
			cfg.Salesforce.UseSandbox()
			zap.L().Info("using salesforce sandbox",
				zap.String("login_url", cfg.Salesforce.LoginURL),
				zap.String("username", cfg.Salesforce.Username),
			)
		}

		// For dry-run, only initialize the SF client (skip store/pipeline).
		// For full runs, initialize the complete pipeline.
		var sfClient sfpkg.Client
		var env *pipelineEnv

		if sfreportDryRun {
			var err error
			sfClient, err = initSalesforce()
			if err != nil {
				return eris.Wrap(err, "sfreport: init salesforce")
			}
			if sfClient == nil {
				return eris.New("sfreport: salesforce not configured (set client_id, username, key_path)")
			}
		} else {
			var err error
			env, err = initPipeline(ctx)
			if err != nil {
				return err
			}
			defer env.Close()
			sfClient = env.SF
			if sfClient == nil {
				return eris.New("sfreport: salesforce not configured (set client_id, username, key_path)")
			}
		}

		// Fetch report from SF Analytics API.
		zap.L().Info("fetching salesforce report", zap.String("report_id", sfreportReportID))
		result, err := sfClient.RunReport(ctx, sfreportReportID)
		if err != nil {
			return eris.Wrap(err, "sfreport: fetch report")
		}

		// Log report metadata for debugging column mapping.
		zap.L().Info("report metadata",
			zap.String("name", result.ReportMetadata.Name),
			zap.String("format", result.ReportMetadata.ReportFormat),
			zap.Strings("columns", result.ReportMetadata.DetailColumns),
			zap.Strings("groupings_down", result.ReportMetadata.GroupingsDown),
		)

		// Log factMap keys and sample row for debugging.
		var factKeys []string
		for k := range result.FactMap {
			factKeys = append(factKeys, k)
		}
		zap.L().Debug("factMap keys", zap.Strings("keys", factKeys))

		// Debug: dump first factMap entry to see row structure.
		for k, raw := range result.FactMap {
			zap.L().Debug("factMap sample", zap.String("key", k), zap.String("raw", string(raw[:min(len(raw), 1000)])))
			break
		}

		// Parse accounts from report rows.
		accounts, err := sfpkg.ParseReportAccounts(result)
		if err != nil {
			return eris.Wrap(err, "sfreport: parse report accounts")
		}

		// Filter accounts without a website.
		var withWebsite []sfpkg.ReportAccount
		var skippedNoWebsite int
		for _, acct := range accounts {
			if strings.TrimSpace(acct.Website) == "" {
				skippedNoWebsite++
				continue
			}
			withWebsite = append(withWebsite, acct)
		}
		if skippedNoWebsite > 0 {
			zap.L().Info("skipped accounts without website",
				zap.Int("skipped", skippedNoWebsite),
			)
		}

		// Apply limit.
		if sfreportLimit > 0 && sfreportLimit < len(withWebsite) {
			withWebsite = withWebsite[:sfreportLimit]
		}

		zap.L().Info("report accounts ready",
			zap.Int("total_in_report", len(accounts)),
			zap.Int("with_website", len(withWebsite)),
			zap.Int("to_process", len(withWebsite)),
		)

		// Dry run: print account table and exit.
		if sfreportDryRun {
			printReportAccounts(withWebsite)
			return nil
		}

		if len(withWebsite) == 0 {
			zap.L().Info("no accounts to process")
			return nil
		}

		// Convert to model.Company.
		companies := reportAccountsToCompanies(withWebsite)

		// Enable deferred SF writes.
		if sfExp, ok := env.Pipeline.ExporterByName("salesforce").(*pipeline.SalesforceExporter); ok {
			sfExp.SetDeferredMode(true)
		}

		if sfreportForce {
			env.Pipeline.SetForceReExtract(true)
		}

		// Process companies concurrently.
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(sfreportConcurrency)

		var succeeded, failed atomic.Int64

		for i, company := range companies {
			g.Go(func() error {
				logCompanyBanner(i, len(companies), company)

				result, runErr := env.Pipeline.Run(gCtx, company)
				if runErr != nil {
					failed.Add(1)
					zap.L().Error("sfreport: company failed",
						zap.String("company", company.Name),
						zap.String("sf_id", company.SalesforceID),
						zap.Error(runErr),
					)
					return nil // don't abort batch on individual failure
				}

				succeeded.Add(1)
				logCompanyResult(result)
				return nil
			})
		}

		_ = g.Wait()

		// Flush exporters (deferred SF writes + any others).
		if err := env.Pipeline.FlushExporters(ctx); err != nil {
			return eris.Wrap(err, "sfreport: flush exporters")
		}

		zap.L().Info("sfreport: batch complete",
			zap.Int("total", len(companies)),
			zap.Int64("succeeded", succeeded.Load()),
			zap.Int64("failed", failed.Load()),
		)
		return nil
	},
}

func init() {
	sfreportCmd.Flags().StringVar(&sfreportReportID, "report-id", "", "Salesforce report ID (required)")
	sfreportCmd.Flags().IntVar(&sfreportLimit, "limit", 0, "max accounts to process (0 = all)")
	sfreportCmd.Flags().IntVar(&sfreportConcurrency, "concurrency", 3, "max accounts to process concurrently")
	sfreportCmd.Flags().BoolVar(&sfreportForce, "force", false, "force re-extraction even if cached")
	sfreportCmd.Flags().BoolVar(&sfreportDryRun, "dry-run", false, "fetch report and print accounts, skip pipeline")
	sfreportCmd.Flags().BoolVar(&sfreportSandbox, "sandbox", false, "use sandbox SF credentials")
	addDirectFlag(sfreportCmd)
	_ = sfreportCmd.MarkFlagRequired("report-id")
	rootCmd.AddCommand(sfreportCmd)
}

// runSFReportViaTemporal fetches the SF report, then delegates enrichment to a Temporal workflow.
func runSFReportViaTemporal(ctx context.Context, _ *cobra.Command) error {
	// Swap sandbox if needed.
	if sfreportSandbox {
		cfg.Salesforce.UseSandbox()
	}

	sfClient, err := initSalesforce()
	if err != nil {
		return eris.Wrap(err, "sfreport: init salesforce")
	}
	if sfClient == nil {
		return eris.New("sfreport: salesforce not configured")
	}

	// Fetch report.
	result, err := sfClient.RunReport(ctx, sfreportReportID)
	if err != nil {
		return eris.Wrap(err, "sfreport: fetch report")
	}

	accounts, err := sfpkg.ParseReportAccounts(result)
	if err != nil {
		return eris.Wrap(err, "sfreport: parse report accounts")
	}

	// Filter and limit.
	var withWebsite []sfpkg.ReportAccount
	for _, acct := range accounts {
		if strings.TrimSpace(acct.Website) != "" {
			withWebsite = append(withWebsite, acct)
		}
	}
	if sfreportLimit > 0 && sfreportLimit < len(withWebsite) {
		withWebsite = withWebsite[:sfreportLimit]
	}
	if len(withWebsite) == 0 {
		zap.L().Info("no accounts to process")
		return nil
	}

	companies := reportAccountsToCompanies(withWebsite)

	c, err := temporalpkg.NewClient(cfg.Temporal)
	if err != nil {
		return err
	}
	defer c.Close()

	workflowID := fmt.Sprintf("sfreport-%d", time.Now().UnixNano())
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporalpkg.EnrichmentTaskQueue,
	}, temporalenrich.BatchEnrichWorkflow, temporalenrich.BatchEnrichParams{
		Companies:      companies,
		Concurrency:    sfreportConcurrency,
		ForceReExtract: sfreportForce,
	})
	if err != nil {
		return eris.Wrap(err, "start sfreport workflow")
	}

	zap.L().Info("sfreport workflow started",
		zap.String("workflow_id", run.GetID()),
		zap.Int("companies", len(companies)),
	)

	var batchResult temporalenrich.BatchEnrichResult
	if err := run.Get(ctx, &batchResult); err != nil {
		return eris.Wrap(err, "sfreport workflow failed")
	}

	fmt.Printf("SF report complete: %d succeeded, %d failed\n", batchResult.Succeeded, batchResult.Failed)
	return nil
}

// reportAccountsToCompanies converts SF report accounts to pipeline Company models.
func reportAccountsToCompanies(accounts []sfpkg.ReportAccount) []model.Company {
	companies := make([]model.Company, len(accounts))
	for i, acct := range accounts {
		c := model.Company{
			SalesforceID: acct.ID,
			Name:         acct.Name,
			URL:          acct.Website,
			City:         acct.City,
			State:        acct.State,
		}
		// Build Location from city + state for pipeline compatibility.
		if acct.City != "" && acct.State != "" {
			c.Location = acct.City + ", " + acct.State
		} else if acct.State != "" {
			c.Location = acct.State
		} else if acct.City != "" {
			c.Location = acct.City
		}
		companies[i] = c
	}
	return companies
}

// printReportAccounts prints a formatted table of report accounts for dry-run output.
func printReportAccounts(accounts []sfpkg.ReportAccount) {
	fmt.Printf("%-18s  %-40s  %-35s  %-20s  %-5s\n", "ID", "Name", "Website", "City", "State")
	fmt.Println(strings.Repeat("-", 125))
	for _, acct := range accounts {
		name := acct.Name
		if len(name) > 40 {
			name = name[:37] + "..."
		}
		website := acct.Website
		if len(website) > 35 {
			website = website[:32] + "..."
		}
		fmt.Printf("%-18s  %-40s  %-35s  %-20s  %-5s\n", acct.ID, name, website, acct.City, acct.State)
	}
	fmt.Printf("\nTotal: %d accounts\n", len(accounts))
}
