package temporal

import (
	"github.com/sells-group/research-cli/internal/temporal/adv"
	"go.temporal.io/sdk/workflow"
)

// Re-export ADV types for backward compatibility.
// New code should import internal/temporal/adv directly.

// SyncWorkflowParams configures the ADV document sync workflow.
type SyncWorkflowParams = adv.SyncWorkflowParams

// ADVDocumentSyncWorkflow orchestrates downloading, extracting, and OCR-ing ADV documents.
// Deprecated: import internal/temporal/adv and use DocumentSyncWorkflow instead.
func ADVDocumentSyncWorkflow(ctx workflow.Context, params SyncWorkflowParams) error {
	return adv.DocumentSyncWorkflow(ctx, params)
}
