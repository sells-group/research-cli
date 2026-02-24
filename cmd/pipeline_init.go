package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/estimate"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/registry"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
	"github.com/sells-group/research-cli/internal/waterfall"
	"github.com/sells-group/research-cli/internal/waterfall/provider"
	anthropicpkg "github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
	"github.com/sells-group/research-cli/pkg/google"
	"github.com/sells-group/research-cli/pkg/jina"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/perplexity"
	"github.com/sells-group/research-cli/pkg/ppp"
)

// pipelineEnv holds all initialized clients, registries, and the pipeline
// needed by the run/batch/serve commands.
type pipelineEnv struct {
	Store     store.Store
	Pipeline  *pipeline.Pipeline
	PPP       ppp.Querier // may be nil
	Questions []model.Question
	Fields    *model.FieldRegistry
	Notion    notion.Client
}

// Close releases resources held by the pipeline environment.
func (pe *pipelineEnv) Close() {
	if pe.PPP != nil {
		pe.PPP.Close()
	}
	if pe.Store != nil {
		_ = pe.Store.Close()
	}
}

// initPipeline sets up the store, all API clients, loads registries, and
// builds the Pipeline. Callers should defer env.Close().
func initPipeline(ctx context.Context) (*pipelineEnv, error) {
	if err := cfg.Validate("enrichment"); err != nil {
		return nil, err
	}

	st, err := initStore(ctx)
	if err != nil {
		return nil, err
	}

	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		return nil, eris.Wrap(err, "migrate store")
	}

	notionClient := notion.NewClient(cfg.Notion.Token)
	anthropicClient := anthropicpkg.NewClient(cfg.Anthropic.Key)
	firecrawlClient := firecrawl.NewClient(cfg.Firecrawl.Key, firecrawl.WithBaseURL(cfg.Firecrawl.BaseURL))
	jinaOpts := []jina.Option{jina.WithBaseURL(cfg.Jina.BaseURL)}
	if cfg.Jina.SearchBaseURL != "" {
		jinaOpts = append(jinaOpts, jina.WithSearchBaseURL(cfg.Jina.SearchBaseURL))
	}
	jinaClient := jina.NewClient(cfg.Jina.Key, jinaOpts...)
	perplexityClient := perplexity.NewClient(cfg.Perplexity.Key, perplexity.WithBaseURL(cfg.Perplexity.BaseURL), perplexity.WithModel(cfg.Perplexity.Model))

	sfClient, err := initSalesforce()
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	// Google Places API client (optional — ultimate fallback for reviews).
	var googleClient google.Client
	if cfg.Google.Key != "" {
		googleClient = google.NewClient(cfg.Google.Key)
		zap.L().Info("google places api enabled")
	} else {
		zap.L().Debug("RESEARCH_GOOGLE_KEY not set, Google Places API fallback disabled")
	}

	var pppClient ppp.Querier
	if cfg.PPP.URL != "" {
		// Legacy: dedicated PPP database connection.
		pppClient, err = ppp.New(ctx, ppp.Config{
			URL:                 cfg.PPP.URL,
			SimilarityThreshold: cfg.PPP.SimilarityThreshold,
			MaxCandidates:       cfg.PPP.MaxCandidates,
		})
		if err != nil {
			zap.L().Warn("ppp client init failed, skipping PPP phase", zap.Error(err))
			pppClient = nil
		}
	} else if ps, ok := st.(*store.PostgresStore); ok {
		// Use the shared Neon pool for PPP lookups (data in fed_data.ppp_loans).
		if pgxPool, ok := ps.Pool().(*pgxpool.Pool); ok {
			pppClient = ppp.NewFromPool(pgxPool, ppp.Config{
				SimilarityThreshold: cfg.PPP.SimilarityThreshold,
				MaxCandidates:       cfg.PPP.MaxCandidates,
			})
			zap.L().Info("ppp client using shared database pool")
		}
	}

	var questions []model.Question
	var fields *model.FieldRegistry

	if cfg.Notion.Token == "" || cfg.Notion.QuestionDB == "" || cfg.Notion.FieldDB == "" {
		zap.L().Warn("notion not configured, loading registries from fixture files")
		questions, err = registry.LoadQuestionsFromFile("testdata/questions.json")
		if err != nil {
			if pppClient != nil {
				pppClient.Close()
			}
			_ = st.Close()
			return nil, eris.Wrap(err, "load question fixtures")
		}
		fields, err = registry.LoadFieldsFromFile("testdata/fields.json")
		if err != nil {
			if pppClient != nil {
				pppClient.Close()
			}
			_ = st.Close()
			return nil, eris.Wrap(err, "load field fixtures")
		}
	} else {
		questions, err = registry.LoadQuestionRegistry(ctx, notionClient, cfg.Notion.QuestionDB)
		if err != nil {
			if pppClient != nil {
				pppClient.Close()
			}
			_ = st.Close()
			return nil, eris.Wrap(err, "load question registry")
		}
		fields, err = registry.LoadFieldRegistry(ctx, notionClient, cfg.Notion.FieldDB)
		if err != nil {
			if pppClient != nil {
				pppClient.Close()
			}
			_ = st.Close()
			return nil, eris.Wrap(err, "load field registry")
		}
	}

	zap.L().Info("registries loaded",
		zap.Int("questions", len(questions)),
		zap.Int("fields", len(fields.Fields)),
	)

	// Build scrape chain: Jina primary → Firecrawl fallback.
	matcher := scrape.NewPathMatcher(cfg.Crawl.ExcludePaths)
	chain := scrape.NewChain(matcher,
		scrape.NewJinaAdapter(jinaClient),
		scrape.NewFirecrawlAdapter(firecrawlClient),
	)

	// Create revenue estimator (nil when using SQLite/offline mode).
	var revenueEstimator *estimate.RevenueEstimator
	if ps, ok := st.(*store.PostgresStore); ok {
		revenueEstimator = estimate.NewRevenueEstimator(ps.Pool())
		zap.L().Info("revenue estimator enabled")
	}

	// Create waterfall executor (nil if config file not found).
	var waterfallExec *waterfall.Executor
	wfCfg, wfErr := waterfall.LoadConfig(cfg.Waterfall.ConfigPath)
	if wfErr != nil {
		zap.L().Warn("waterfall config not loaded, waterfall phase will be skipped", zap.Error(wfErr))
	} else {
		providerRegistry := provider.NewRegistry()
		// Premium providers are registered here as they become available.
		waterfallExec = waterfall.NewExecutor(wfCfg, providerRegistry)
		zap.L().Info("waterfall executor enabled",
			zap.Int("configured_fields", len(wfCfg.Fields)),
		)
	}

	p := pipeline.New(cfg, st, chain, jinaClient, firecrawlClient, perplexityClient, anthropicClient, sfClient, notionClient, googleClient, pppClient, revenueEstimator, waterfallExec, questions, fields)

	return &pipelineEnv{
		Store:     st,
		Pipeline:  p,
		PPP:       pppClient,
		Questions: questions,
		Fields:    fields,
		Notion:    notionClient,
	}, nil
}
