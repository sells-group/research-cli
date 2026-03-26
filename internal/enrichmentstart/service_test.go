package enrichmentstart

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"

	"github.com/sells-group/research-cli/internal/model"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
)

type stubClock struct {
	now time.Time
}

func (s stubClock) Now() time.Time {
	return s.now
}

func TestStartWebhook_StartsWorkflow(t *testing.T) {
	dedupeKey := temporalpkg.WorkflowURLKey("https://acme.com")
	workflowID := temporalpkg.BuildStableWorkflowID("enrich-webhook", dedupeKey)

	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return(workflowID).Once()
	mockRun.On("GetRunID").Return("run-123").Once()

	mockClient.
		On("ExecuteWorkflow", context.Background(), mockStartWorkflowMatcher(t, workflowID, "webhook"), mock.Anything, mock.Anything).
		Return(mockRun, nil).
		Once()

	svc := NewService(mockClient).WithClock(stubClock{now: time.Date(2026, time.March, 26, 8, 30, 0, 0, time.UTC)})
	result, err := svc.StartWebhook(context.Background(), model.Company{URL: "https://acme.com"}, "req-123")
	require.NoError(t, err)

	assert.False(t, result.Reused)
	assert.Equal(t, workflowID, result.WorkflowID)
	assert.Equal(t, "run-123", result.WorkflowRunID)
	assert.Equal(t, dedupeKey, result.DedupeKey)
}

func TestStartWebhook_ReusesRunningWorkflow(t *testing.T) {
	dedupeKey := temporalpkg.WorkflowURLKey("https://acme.com")
	workflowID := temporalpkg.BuildStableWorkflowID("enrich-webhook", dedupeKey)

	mockClient := &mocks.Client{}
	mockClient.
		On("ExecuteWorkflow", context.Background(), mock.Anything, mock.Anything, mock.Anything).
		Return(nil, &serviceerror.WorkflowExecutionAlreadyStarted{RunId: "run-existing"}).
		Once()

	svc := NewService(mockClient)
	result, err := svc.StartWebhook(context.Background(), model.Company{URL: "https://acme.com"}, "req-123")
	require.NoError(t, err)

	assert.True(t, result.Reused)
	assert.Equal(t, "run-existing", result.WorkflowRunID)
	assert.Equal(t, workflowID, result.WorkflowID)
}

func TestStartRetry_UsesOriginalRunID(t *testing.T) {
	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("enrich-retry-run-123").Once()
	mockRun.On("GetRunID").Return("run-retry-1").Once()

	mockClient.
		On("ExecuteWorkflow", context.Background(), mockStartWorkflowMatcher(t, "enrich-retry-run-123", "retry"), mock.Anything, mock.Anything).
		Return(mockRun, nil).
		Once()

	svc := NewService(mockClient)
	result, err := svc.StartRetry(context.Background(), "run-123", model.Company{URL: "https://acme.com"}, "req-retry")
	require.NoError(t, err)

	assert.Equal(t, "retry-run-123", result.DedupeKey)
	assert.Equal(t, "enrich-retry-run-123", result.WorkflowID)
}

func TestStartWebhook_NormalizesBareHost(t *testing.T) {
	dedupeKey := temporalpkg.WorkflowURLKey("acme.com")
	workflowID := temporalpkg.BuildStableWorkflowID("enrich-webhook", dedupeKey)

	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return(workflowID).Once()
	mockRun.On("GetRunID").Return("run-1").Once()
	mockClient.
		On("ExecuteWorkflow", context.Background(), mockStartWorkflowMatcher(t, workflowID, "webhook"), mock.Anything, mock.Anything).
		Return(mockRun, nil).
		Once()

	svc := NewService(mockClient)
	result, err := svc.StartWebhook(context.Background(), model.Company{URL: "acme.com"}, "req-1")
	require.NoError(t, err)
	assert.Equal(t, dedupeKey, result.DedupeKey)
}

func TestStartWebhook_FallsBackWithoutTypedSearchAttributesOnInvalidArgument(t *testing.T) {
	dedupeKey := temporalpkg.WorkflowURLKey("https://acme.com")
	workflowID := temporalpkg.BuildStableWorkflowID("enrich-webhook", dedupeKey)

	mockClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return(workflowID).Once()
	mockRun.On("GetRunID").Return("run-2").Once()

	mockClient.
		On("ExecuteWorkflow", context.Background(), mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.ID == workflowID && opts.TypedSearchAttributes.Size() > 0
		}), mock.Anything, mock.Anything).
		Return(nil, &serviceerror.InvalidArgument{Message: "search attributes not registered"}).
		Once()
	mockClient.
		On("ExecuteWorkflow", context.Background(), mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.ID == workflowID && opts.TypedSearchAttributes.Size() == 0
		}), mock.Anything, mock.Anything).
		Return(mockRun, nil).
		Once()

	svc := NewService(mockClient)
	result, err := svc.StartWebhook(context.Background(), model.Company{URL: "https://acme.com"}, "req-1")
	require.NoError(t, err)
	assert.False(t, result.Reused)
	assert.Equal(t, "run-2", result.WorkflowRunID)
}

func mockStartWorkflowMatcher(t *testing.T, expectedID, expectedTrigger string) interface{} {
	t.Helper()
	return mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
		if opts.ID != expectedID {
			return false
		}
		if opts.TaskQueue == "" {
			return false
		}
		if opts.WorkflowExecutionErrorWhenAlreadyStarted != true {
			return false
		}
		if opts.Memo == nil {
			return false
		}
		if opts.TypedSearchAttributes.Size() == 0 {
			return false
		}
		typed := opts.TypedSearchAttributes.GetUntypedValues()
		if typed[temporalpkg.TriggerSourceSearchAttribute] != expectedTrigger {
			return false
		}
		requestedAt, ok := typed[temporalpkg.RequestedAtSearchAttribute].(time.Time)
		if !ok || requestedAt.IsZero() {
			return false
		}
		if host, ok := typed[temporalpkg.CompanyHostSearchAttribute].(string); !ok || host == "" {
			return false
		}
		trigger, _ := opts.Memo["trigger_source"].(string)
		return trigger == expectedTrigger
	})
}
