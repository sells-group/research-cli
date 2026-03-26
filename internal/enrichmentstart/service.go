// Package enrichmentstart starts API-triggered enrichment workflows with idempotent business keys.
package enrichmentstart

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
)

// Clock provides time for deterministic tests.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now().UTC()
}

type workflowClient interface {
	ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error)
}

// StartResult describes the workflow start outcome for an API request.
type StartResult struct {
	WorkflowID    string    `json:"workflow_id"`
	WorkflowRunID string    `json:"workflow_run_id"`
	Reused        bool      `json:"reused"`
	DedupeKey     string    `json:"dedupe_key"`
	RequestedAt   time.Time `json:"requested_at"`
}

// Service starts enrichment workflows for API requests.
type Service struct {
	client workflowClient
	clock  Clock
}

// NewService creates an enrichment start service.
func NewService(c workflowClient) *Service {
	return &Service{
		client: c,
		clock:  realClock{},
	}
}

// WithClock overrides time for tests.
func (s *Service) WithClock(clock Clock) *Service {
	if clock != nil {
		s.clock = clock
	}
	return s
}

// StartWebhook starts or reuses a webhook-triggered enrichment workflow.
func (s *Service) StartWebhook(ctx context.Context, company model.Company, requestID string) (*StartResult, error) {
	dedupeKey := temporalpkg.WorkflowURLKey(company.URL)
	return s.start(ctx, company, temporalenrich.StartMetadata{
		RequestID:     requestID,
		TriggerSource: "webhook",
		DedupeKey:     dedupeKey,
		RequestedAt:   s.clock.Now().UTC(),
	}, temporalpkg.BuildStableWorkflowID("enrich-webhook", dedupeKey))
}

// StartRetry starts or reuses a retry-triggered enrichment workflow.
func (s *Service) StartRetry(ctx context.Context, originalRunID string, company model.Company, requestID string) (*StartResult, error) {
	dedupeKey := temporalpkg.BuildStableWorkflowID("retry", originalRunID)
	return s.start(ctx, company, temporalenrich.StartMetadata{
		RequestID:     requestID,
		TriggerSource: "retry",
		OriginalRunID: originalRunID,
		DedupeKey:     dedupeKey,
		RequestedAt:   s.clock.Now().UTC(),
	}, temporalpkg.BuildStableWorkflowID("enrich-retry", originalRunID))
}

func (s *Service) start(ctx context.Context, company model.Company, metadata temporalenrich.StartMetadata, workflowID string) (*StartResult, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("enrichment starter is not configured")
	}

	opts := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                temporalpkg.EnrichmentTaskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		Memo: map[string]interface{}{
			"request_id":      metadata.RequestID,
			"trigger_source":  metadata.TriggerSource,
			"original_run_id": metadata.OriginalRunID,
			"dedupe_key":      metadata.DedupeKey,
			"requested_at":    metadata.RequestedAt,
			"company_url":     company.URL,
		},
		TypedSearchAttributes: searchAttributes(company, metadata),
		StaticSummary:         fmt.Sprintf("enrich %s via %s", temporalpkg.WorkflowHostLabel(company.URL), metadata.TriggerSource),
		StaticDetails:         fmt.Sprintf("request_id=%s\ndedupe_key=%s\ncompany_url=%s", metadata.RequestID, metadata.DedupeKey, company.URL),
	}

	run, err := s.executeWorkflow(ctx, opts, company, metadata)
	if err != nil {
		if alreadyStarted, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
			zap.L().Info("reusing existing enrichment workflow",
				zap.String("workflow_id", workflowID),
				zap.String("workflow_run_id", alreadyStarted.RunId),
				zap.String("company", company.URL),
				zap.String("trigger_source", metadata.TriggerSource),
				zap.String("request_id", metadata.RequestID),
			)
			return &StartResult{
				WorkflowID:    workflowID,
				WorkflowRunID: alreadyStarted.RunId,
				Reused:        true,
				DedupeKey:     metadata.DedupeKey,
				RequestedAt:   metadata.RequestedAt,
			}, nil
		}
		return nil, err
	}

	result := &StartResult{
		WorkflowID:    run.GetID(),
		WorkflowRunID: run.GetRunID(),
		Reused:        false,
		DedupeKey:     metadata.DedupeKey,
		RequestedAt:   metadata.RequestedAt,
	}

	zap.L().Info("started enrichment workflow",
		zap.String("workflow_id", result.WorkflowID),
		zap.String("workflow_run_id", result.WorkflowRunID),
		zap.String("company", company.URL),
		zap.String("trigger_source", metadata.TriggerSource),
		zap.String("request_id", metadata.RequestID),
		zap.String("dedupe_key", metadata.DedupeKey),
	)

	return result, nil
}

func (s *Service) executeWorkflow(
	ctx context.Context,
	opts client.StartWorkflowOptions,
	company model.Company,
	metadata temporalenrich.StartMetadata,
) (client.WorkflowRun, error) {
	run, err := s.client.ExecuteWorkflow(ctx, opts, temporalenrich.EnrichCompanyWorkflow, temporalenrich.EnrichCompanyParams{
		Company:  company,
		Metadata: metadata,
	})
	if _, ok := err.(*serviceerror.InvalidArgument); ok && opts.TypedSearchAttributes.Size() > 0 {
		zap.L().Warn("typed search attributes unavailable for enrichment workflow start, retrying without them",
			zap.String("company", company.URL),
			zap.String("trigger_source", metadata.TriggerSource),
			zap.String("request_id", metadata.RequestID),
		)
		opts.TypedSearchAttributes = temporalsdk.NewSearchAttributes()
		return s.client.ExecuteWorkflow(ctx, opts, temporalenrich.EnrichCompanyWorkflow, temporalenrich.EnrichCompanyParams{
			Company:  company,
			Metadata: metadata,
		})
	}
	return run, err
}

func searchAttributes(company model.Company, metadata temporalenrich.StartMetadata) temporalsdk.SearchAttributes {
	updates := make([]temporalsdk.SearchAttributeUpdate, 0, 6)
	if metadata.RequestID != "" {
		updates = append(updates, temporalpkg.RequestIDSearchAttribute.ValueSet(metadata.RequestID))
	}
	if metadata.TriggerSource != "" {
		updates = append(updates, temporalpkg.TriggerSourceSearchAttribute.ValueSet(metadata.TriggerSource))
	}
	if metadata.OriginalRunID != "" {
		updates = append(updates, temporalpkg.OriginalRunIDSearchAttribute.ValueSet(metadata.OriginalRunID))
	}
	if metadata.DedupeKey != "" {
		updates = append(updates, temporalpkg.DedupeKeySearchAttribute.ValueSet(metadata.DedupeKey))
	}
	if host := temporalpkg.WorkflowHostLabel(company.URL); host != "" {
		updates = append(updates, temporalpkg.CompanyHostSearchAttribute.ValueSet(host))
	}
	if !metadata.RequestedAt.IsZero() {
		updates = append(updates, temporalpkg.RequestedAtSearchAttribute.ValueSet(metadata.RequestedAt.UTC()))
	}
	return temporalsdk.NewSearchAttributes(updates...)
}
