package pipeline

import (
	"context"
	"sync"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/salesforce"
)

// SalesforceExporter writes enrichment results to Salesforce.
// In immediate mode it writes per-result; in deferred mode it collects
// SFWriteIntents and bulk-writes during Flush.
type SalesforceExporter struct {
	sfClient     salesforce.Client
	notionClient notion.Client
	fields       *model.FieldRegistry
	cfg          *config.Config
	deferred     bool

	mu      sync.Mutex
	intents []*SFWriteIntent
}

// NewSalesforceExporter creates a SalesforceExporter. When deferred is true,
// SF writes are collected and executed in Flush (batch mode).
func NewSalesforceExporter(sfClient salesforce.Client, notionClient notion.Client, fields *model.FieldRegistry, cfg *config.Config, deferred bool) *SalesforceExporter {
	return &SalesforceExporter{
		sfClient:     sfClient,
		notionClient: notionClient,
		fields:       fields,
		cfg:          cfg,
		deferred:     deferred,
	}
}

// Name implements ResultExporter.
func (e *SalesforceExporter) Name() string { return "salesforce" }

// ExportResult implements ResultExporter.
func (e *SalesforceExporter) ExportResult(ctx context.Context, result *model.EnrichmentResult, gate *GateResult) error {
	if !gate.Passed || e.sfClient == nil {
		return nil
	}

	accountFields, contactFields := buildSFFieldsByObject(result.FieldValues, e.fields)
	if result.Report != "" {
		accountFields["Enrichment_Report__c"] = result.Report
	}
	ensureMinimumSFFields(accountFields, result.Company, result.FieldValues)
	injectGeoFields(accountFields, result.GeoData)

	contacts := extractContactsForSF(result.FieldValues, e.fields)
	if contacts == nil && len(contactFields) > 0 {
		contacts = []map[string]any{contactFields}
	}

	if e.deferred {
		intent := &SFWriteIntent{
			AccountFields: accountFields,
			NotionPageID:  result.Company.NotionPageID,
			Result:        result,
			Contacts:      contacts,
		}

		accountID := result.Company.SalesforceID
		if accountID != "" {
			intent.AccountOp = "update"
			intent.AccountID = accountID
		} else {
			// Dedup lookup.
			if result.Company.URL != "" {
				existing, findErr := salesforce.FindAccountByWebsite(ctx, e.sfClient, result.Company.URL)
				if findErr != nil {
					zap.L().Warn("exporter: dedup lookup failed, proceeding with create",
						zap.String("company", result.Company.Name),
						zap.Error(findErr),
					)
				} else if existing != nil {
					intent.AccountOp = "update"
					intent.AccountID = existing.ID
					intent.DedupMatch = true
					result.Company.SalesforceID = existing.ID
				}
			}
			if intent.AccountOp == "" {
				intent.AccountOp = "create"
			}
		}

		e.mu.Lock()
		e.intents = append(e.intents, intent)
		e.mu.Unlock()
		return nil
	}

	// Immediate mode.
	accountID := result.Company.SalesforceID
	if accountID != "" {
		if len(accountFields) > 0 {
			if err := salesforce.UpdateAccount(ctx, e.sfClient, accountID, accountFields); err != nil {
				return eris.Wrap(err, "exporter: sf update")
			}
		}
	} else {
		resolvedID, err := resolveOrCreateAccount(ctx, e.sfClient, e.notionClient, result, accountFields, &GateResult{Passed: true})
		if err != nil {
			return eris.Wrap(err, "exporter: sf resolve or create")
		}
		accountID = resolvedID
	}

	if accountID != "" {
		upsertContacts(ctx, e.sfClient, accountID, contacts, result.Company.Name)
	}

	return nil
}

// SetDeferredMode switches between immediate and deferred SF write modes.
// Batch commands call this after init to collect writes for bulk flush.
func (e *SalesforceExporter) SetDeferredMode(deferred bool) {
	e.deferred = deferred
}

// Flush implements ResultExporter.
func (e *SalesforceExporter) Flush(ctx context.Context) error {
	e.mu.Lock()
	intents := e.intents
	e.intents = nil
	e.mu.Unlock()

	if len(intents) == 0 {
		return nil
	}

	summary, err := FlushSFWrites(ctx, e.sfClient, e.notionClient, intents)
	if err != nil {
		return eris.Wrap(err, "exporter: flush sf writes")
	}
	if summary != nil {
		summary.LogSummary()
	}
	return nil
}
