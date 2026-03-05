package main

import (
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/docling"
	"github.com/sells-group/research-cli/internal/fetcher"
	tmprl "github.com/sells-group/research-cli/internal/temporal"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start Temporal worker for ADV document workflows",
	RunE: func(_ *cobra.Command, _ []string) error {
		ctx := rootCmd.Context()

		// Connect to Temporal.
		tc, err := tmprl.NewClient(cfg.Temporal)
		if err != nil {
			return eris.Wrap(err, "worker: create temporal client")
		}
		defer tc.Close()

		// Create DB pool.
		pool, err := fedsyncPool(ctx)
		if err != nil {
			return eris.Wrap(err, "worker: create db pool")
		}
		defer pool.Close()

		// Create fetcher.
		f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{
			UserAgent:  cfg.Fedsync.EDGARUserAgent,
			MaxRetries: 3,
			Timeout:    30 * time.Minute,
		})

		// Create Docling client.
		dc := docling.NewClient(cfg.Fedsync.DoclingURL)

		// Build activities.
		activities := &tmprl.Activities{
			Pool:    pool,
			Fetcher: f,
			Docling: dc,
		}

		// Create and configure worker.
		w := worker.New(tc, tmprl.ADVDocumentQueue, worker.Options{})
		w.RegisterWorkflow(tmprl.ADVDocumentSyncWorkflow)
		w.RegisterActivity(activities)

		zap.L().Info("starting temporal worker",
			zap.String("task_queue", tmprl.ADVDocumentQueue),
			zap.String("host_port", cfg.Temporal.HostPort),
		)

		// Run blocks until interrupted.
		return w.Run(worker.InterruptCh())
	},
}

func init() {
	fedsyncCmd.AddCommand(workerCmd)
}
