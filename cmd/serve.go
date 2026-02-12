package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/registry"
	anthropicpkg "github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
)

var servePort int

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start webhook server for enrichment requests",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		// Init store
		st, err := initStore(ctx)
		if err != nil {
			return err
		}
		defer st.Close()

		if err := st.Migrate(ctx); err != nil {
			return eris.Wrap(err, "migrate store")
		}

		// Init clients
		notionClient := notion.NewClient(cfg.Notion.Token)
		anthropicClient := anthropicpkg.NewClient(cfg.Anthropic.Key)
		firecrawlClient := firecrawl.NewClient(cfg.Firecrawl.Key, firecrawl.WithBaseURL(cfg.Firecrawl.BaseURL))
		jinaClient := jina.NewClient(cfg.Jina.Key, jina.WithBaseURL(cfg.Jina.BaseURL))
		perplexityClient := perplexity.NewClient(cfg.Perplexity.Key, perplexity.WithBaseURL(cfg.Perplexity.BaseURL), perplexity.WithModel(cfg.Perplexity.Model))

		sfClient, err := initSalesforce()
		if err != nil {
			return err
		}

		// Init PPP client (optional)
		var pppClient ppp.Querier
		if cfg.PPP.URL != "" {
			pppClient, err = ppp.New(ctx, ppp.Config{
				URL:                 cfg.PPP.URL,
				SimilarityThreshold: cfg.PPP.SimilarityThreshold,
				MaxCandidates:       cfg.PPP.MaxCandidates,
			})
			if err != nil {
				zap.L().Warn("ppp client init failed, skipping PPP phase", zap.Error(err))
			} else {
				defer pppClient.Close()
			}
		}

		// Load registries
		questions, err := registry.LoadQuestionRegistry(ctx, notionClient, cfg.Notion.QuestionDB)
		if err != nil {
			return eris.Wrap(err, "load question registry")
		}
		fields, err := registry.LoadFieldRegistry(ctx, notionClient, cfg.Notion.FieldDB)
		if err != nil {
			return eris.Wrap(err, "load field registry")
		}

		zap.L().Info("registries loaded",
			zap.Int("questions", len(questions)),
			zap.Int("fields", len(fields.Fields)),
		)

		// Build pipeline
		p := pipeline.New(cfg, st, jinaClient, firecrawlClient, perplexityClient, anthropicClient, sfClient, notionClient, pppClient, questions, fields)

		// Set up routes
		mux := http.NewServeMux()

		mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		})

		mux.HandleFunc("POST /webhook/enrich", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				URL          string `json:"url"`
				SalesforceID string `json:"salesforce_id"`
				Name         string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
				return
			}

			if req.URL == "" {
				http.Error(w, `{"error":"url is required"}`, http.StatusBadRequest)
				return
			}

			company := model.Company{
				URL:          req.URL,
				SalesforceID: req.SalesforceID,
				Name:         req.Name,
			}

			// Run enrichment asynchronously
			go func() {
				result, err := p.Run(ctx, company)
				if err != nil {
					zap.L().Error("webhook enrichment failed",
						zap.String("company", company.URL),
						zap.Error(err),
					)
					return
				}
				zap.L().Info("webhook enrichment complete",
					zap.String("company", company.URL),
					zap.Float64("score", result.Score),
				)
			}()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{
				"status":  "accepted",
				"company": req.URL,
			})
		})

		port := servePort
		if port == 0 {
			port = cfg.Server.Port
		}

		srv := &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		}

		// Graceful shutdown
		go func() {
			<-ctx.Done()
			zap.L().Info("shutting down server")
			srv.Shutdown(ctx)
		}()

		zap.L().Info("starting server", zap.Int("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return eris.Wrap(err, "server listen")
		}

		return nil
	},
}

func init() {
	serveCmd.Flags().IntVar(&servePort, "port", 0, "server port (default from config)")
	rootCmd.AddCommand(serveCmd)
}
