package main

import (
	"context"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/pipeline"
	"github.com/sells-group/research-cli/internal/registry"
	"github.com/sells-group/research-cli/internal/scrape"
	"github.com/sells-group/research-cli/internal/store"
	anthropicpkg "github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/firecrawl"
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
		pe.Store.Close()
	}
}

// initPipeline sets up the store, all API clients, loads registries, and
// builds the Pipeline. Callers should defer env.Close().
func initPipeline(ctx context.Context) (*pipelineEnv, error) {
	st, err := initStore(ctx)
	if err != nil {
		return nil, err
	}

	if err := st.Migrate(ctx); err != nil {
		st.Close()
		return nil, eris.Wrap(err, "migrate store")
	}

	notionClient := notion.NewClient(cfg.Notion.Token)
	anthropicClient := anthropicpkg.NewClient(cfg.Anthropic.Key)
	firecrawlClient := firecrawl.NewClient(cfg.Firecrawl.Key, firecrawl.WithBaseURL(cfg.Firecrawl.BaseURL))
	jinaClient := jina.NewClient(cfg.Jina.Key, jina.WithBaseURL(cfg.Jina.BaseURL))
	perplexityClient := perplexity.NewClient(cfg.Perplexity.Key, perplexity.WithBaseURL(cfg.Perplexity.BaseURL), perplexity.WithModel(cfg.Perplexity.Model))

	sfClient, err := initSalesforce()
	if err != nil {
		st.Close()
		return nil, err
	}

	var pppClient ppp.Querier
	if cfg.PPP.URL != "" {
		pppClient, err = ppp.New(ctx, ppp.Config{
			URL:                 cfg.PPP.URL,
			SimilarityThreshold: cfg.PPP.SimilarityThreshold,
			MaxCandidates:       cfg.PPP.MaxCandidates,
		})
		if err != nil {
			zap.L().Warn("ppp client init failed, skipping PPP phase", zap.Error(err))
			pppClient = nil
		}
	}

	questions, err := registry.LoadQuestionRegistry(ctx, notionClient, cfg.Notion.QuestionDB)
	if err != nil {
		if pppClient != nil {
			pppClient.Close()
		}
		st.Close()
		return nil, eris.Wrap(err, "load question registry")
	}
	fields, err := registry.LoadFieldRegistry(ctx, notionClient, cfg.Notion.FieldDB)
	if err != nil {
		if pppClient != nil {
			pppClient.Close()
		}
		st.Close()
		return nil, eris.Wrap(err, "load field registry")
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

	p := pipeline.New(cfg, st, chain, firecrawlClient, perplexityClient, anthropicClient, sfClient, notionClient, pppClient, questions, fields)

	return &pipelineEnv{
		Store:     st,
		Pipeline:  p,
		PPP:       pppClient,
		Questions: questions,
		Fields:    fields,
		Notion:    notionClient,
	}, nil
}
