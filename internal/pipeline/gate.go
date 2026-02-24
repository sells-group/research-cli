package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jomei/notionapi"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/notion"
	"github.com/sells-group/research-cli/pkg/salesforce"
)

var webhookClient = &http.Client{Timeout: 10 * time.Second}

// GateResult holds the outcome of the quality gate phase.
type GateResult struct {
	Score          float64        `json:"score"`
	ScoreBreakdown ScoreBreakdown `json:"score_breakdown"`
	Passed         bool           `json:"passed"`
	SFUpdated      bool           `json:"sf_updated"`
	ManualReview   bool           `json:"manual_review"`
}

// QualityGate implements Phase 9: evaluate quality score, update Salesforce,
// send to ToolJet for manual review if needed, and update Notion status.
func QualityGate(ctx context.Context, result *model.EnrichmentResult, fields *model.FieldRegistry, questions []model.Question, sfClient salesforce.Client, notionClient notion.Client, cfg *config.Config) (*GateResult, error) {
	breakdown := ComputeScore(result.FieldValues, fields, questions, result.Answers, cfg.Pipeline.QualityWeights)
	score := breakdown.Final
	result.Score = score
	threshold := cfg.Pipeline.QualityScoreThreshold

	gate := &GateResult{
		Score:          score,
		ScoreBreakdown: breakdown,
		Passed:         score >= threshold,
	}

	// Run SF/ToolJet and Notion updates concurrently — they are independent.
	g, gCtx := errgroup.WithContext(ctx)

	var sfErr, notionErr error

	// SF or ToolJet update.
	g.Go(func() error {
		if gate.Passed && sfClient != nil {
			accountFields, contactFields := buildSFFieldsByObject(result.FieldValues, fields)
			if result.Report != "" {
				accountFields["Enrichment_Report__c"] = result.Report
			}
			ensureMinimumSFFields(accountFields, result.Company, result.FieldValues)

			accountID := result.Company.SalesforceID

			if accountID != "" {
				// Existing account — update.
				if len(accountFields) > 0 {
					if err := salesforce.UpdateAccount(gCtx, sfClient, accountID, accountFields); err != nil {
						sfErr = err
						zap.L().Error("gate: salesforce update failed",
							zap.String("company", result.Company.Name),
							zap.Error(err),
						)
						return eris.Wrap(err, "gate: sf update")
					}
					gate.SFUpdated = true
				}
			} else {
				// New account — create.
				newID, err := salesforce.CreateAccount(gCtx, sfClient, accountFields)
				if err != nil {
					sfErr = err
					zap.L().Error("gate: salesforce create failed",
						zap.String("company", result.Company.Name),
						zap.Error(err),
					)
					return eris.Wrap(err, "gate: sf create")
				}
				accountID = newID
				result.Company.SalesforceID = newID
				gate.SFUpdated = true

				// Write new SF ID back to Notion.
				if notionClient != nil && result.Company.NotionPageID != "" {
					if err := writeNotionSalesforceID(gCtx, notionClient, result.Company.NotionPageID, newID); err != nil {
						zap.L().Warn("gate: failed to write SF ID to Notion",
							zap.String("company", result.Company.Name),
							zap.Error(err),
						)
					}
				}
			}

			// Create Contacts — up to 3 from the contacts field, with fallback to owner_* fields.
			if accountID != "" {
				contacts := extractContactsForSF(result.FieldValues, fields)
				if contacts == nil && len(contactFields) > 0 {
					contacts = []map[string]any{contactFields}
				}
				for i, cf := range contacts {
					if len(cf) == 0 {
						continue
					}
					if _, err := salesforce.CreateContact(gCtx, sfClient, accountID, cf); err != nil {
						zap.L().Warn("gate: salesforce create contact failed",
							zap.String("company", result.Company.Name),
							zap.Int("contact_index", i),
							zap.Error(err),
						)
					}
				}
			}
		} else if !gate.Passed {
			if cfg.ToolJet.WebhookURL != "" {
				if err := sendToToolJet(gCtx, result, cfg.ToolJet.WebhookURL); err != nil {
					zap.L().Warn("gate: tooljet webhook failed",
						zap.String("company", result.Company.Name),
						zap.Error(err),
					)
				} else {
					gate.ManualReview = true
				}
			}
		}
		return nil
	})

	// Notion update.
	g.Go(func() error {
		if result.Company.NotionPageID != "" {
			status := "Enriched"
			if !gate.Passed {
				status = "Manual Review"
			}
			if err := updateNotionStatus(gCtx, notionClient, result.Company.NotionPageID, status, result); err != nil {
				notionErr = err
				zap.L().Warn("gate: notion update failed",
					zap.String("company", result.Company.Name),
					zap.Error(err),
				)
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		// If SF succeeded but Notion failed, log inconsistency and retry Notion once.
		if sfErr == nil && gate.SFUpdated && notionErr != nil {
			zap.L().Error("gate: inconsistent state — SF updated but Notion failed, retrying Notion",
				zap.String("company", result.Company.Name),
				zap.Error(notionErr),
			)
			status := "Enriched"
			if !gate.Passed {
				status = "Manual Review"
			}
			if retryErr := updateNotionStatus(ctx, notionClient, result.Company.NotionPageID, status, result); retryErr != nil {
				zap.L().Error("gate: notion retry also failed",
					zap.String("company", result.Company.Name),
					zap.Error(retryErr),
				)
			} else {
				notionErr = nil
			}
		}
		if sfErr != nil && notionErr == nil {
			zap.L().Error("gate: inconsistent state — Notion updated but SF failed",
				zap.String("company", result.Company.Name),
				zap.Error(sfErr),
			)
		}
		return gate, err
	}

	// Handle case where SF didn't return an error to errgroup but Notion failed.
	if gate.SFUpdated && notionErr != nil {
		zap.L().Error("gate: inconsistent state — SF updated but Notion failed, retrying Notion",
			zap.String("company", result.Company.Name),
			zap.Error(notionErr),
		)
		status := "Enriched"
		if !gate.Passed {
			status = "Manual Review"
		}
		if retryErr := updateNotionStatus(ctx, notionClient, result.Company.NotionPageID, status, result); retryErr != nil {
			zap.L().Error("gate: notion retry also failed",
				zap.String("company", result.Company.Name),
				zap.Error(retryErr),
			)
		}
	}

	return gate, nil
}

func buildSFFields(fieldValues map[string]model.FieldValue) map[string]any {
	fields := make(map[string]any)
	for _, fv := range fieldValues {
		if fv.SFField != "" {
			fields[fv.SFField] = fv.Value
		}
	}
	return fields
}

// buildSFFieldsByObject splits field values into Account and Contact maps
// based on the SFObject property from the field registry.
func buildSFFieldsByObject(fieldValues map[string]model.FieldValue, registry *model.FieldRegistry) (accountFields map[string]any, contactFields map[string]any) {
	accountFields = make(map[string]any)
	contactFields = make(map[string]any)
	for _, fv := range fieldValues {
		if fv.SFField == "" {
			continue
		}
		fm := registry.ByKey(fv.FieldKey)
		if fm != nil && fm.SFObject == "Contact" {
			contactFields[fv.SFField] = fv.Value
		} else {
			accountFields[fv.SFField] = fv.Value
		}
	}
	return accountFields, contactFields
}

// ensureMinimumSFFields sets Name and Website from the Company if not already
// present in the enriched fields. Required for Account creation.
// Uses a fallback chain: company.Name → fieldValues[company_name/account_name] → domain heuristic.
func ensureMinimumSFFields(fields map[string]any, company model.Company, fieldValues map[string]model.FieldValue) {
	if fields["Name"] == nil || fields["Name"] == "" {
		if company.Name != "" {
			fields["Name"] = company.Name
		}
	}
	// Fallback: extracted company_name from T1/T2/T3.
	if fields["Name"] == nil || fields["Name"] == "" {
		for _, key := range []string{"company_name", "account_name"} {
			if fv, ok := fieldValues[key]; ok && fv.Value != nil {
				if s := fmt.Sprintf("%v", fv.Value); s != "" {
					fields["Name"] = s
					break
				}
			}
		}
	}
	// Last resort: domain heuristic.
	if fields["Name"] == nil || fields["Name"] == "" {
		if dn := domainToName(company.URL); dn != "" {
			fields["Name"] = dn
		}
	}
	if fields["Website"] == nil || fields["Website"] == "" {
		if company.URL != "" {
			fields["Website"] = company.URL
		}
	}
}

// writeNotionSalesforceID updates the SalesforceID property on the Lead Tracker page.
func writeNotionSalesforceID(ctx context.Context, client notion.Client, pageID, sfID string) error {
	_, err := client.UpdatePage(ctx, pageID, &notionapi.PageUpdateRequest{
		Properties: notionapi.Properties{
			"SalesforceID": notionapi.RichTextProperty{
				Type: notionapi.PropertyTypeRichText,
				RichText: []notionapi.RichText{
					{Type: notionapi.ObjectTypeText, Text: &notionapi.Text{Content: sfID}},
				},
			},
		},
	})
	if err != nil {
		return eris.Wrap(err, fmt.Sprintf("gate: write sf id to notion page %s", pageID))
	}
	return nil
}

func sendToToolJet(ctx context.Context, result *model.EnrichmentResult, webhookURL string) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return eris.Wrap(err, "gate: marshal tooljet payload")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		return eris.Wrap(err, "gate: create tooljet request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := webhookClient.Do(req)
	if err != nil {
		return eris.Wrap(err, "gate: tooljet request failed")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return eris.Errorf("gate: tooljet returned status %d", resp.StatusCode)
	}

	return nil
}

func updateNotionStatus(ctx context.Context, client notion.Client, pageID, status string, result *model.EnrichmentResult) error {
	now := notionapi.Date(time.Now())
	_, err := client.UpdatePage(ctx, pageID, &notionapi.PageUpdateRequest{
		Properties: notionapi.Properties{
			"Status": notionapi.StatusProperty{
				Status: notionapi.Status{
					Name: status,
				},
			},
			"Score": notionapi.NumberProperty{
				Number: result.Score,
			},
			"Fields Populated": notionapi.NumberProperty{
				Number: float64(len(result.FieldValues)),
			},
			"Enrichment Cost": notionapi.NumberProperty{
				Number: result.TotalCost,
			},
			"Last Enriched": notionapi.DateProperty{
				Date: &notionapi.DateObject{
					Start: &now,
				},
			},
		},
	})
	if err != nil {
		return eris.Wrap(err, fmt.Sprintf("gate: update notion page %s", pageID))
	}
	return nil
}

// extractContactsForSF builds up to 3 SF Contact field maps from the contacts
// FieldValue. Returns nil if no contacts field is found or it's empty.
func extractContactsForSF(fieldValues map[string]model.FieldValue, registry *model.FieldRegistry) []map[string]any {
	fv, ok := fieldValues["contacts"]
	if !ok {
		return nil
	}

	var items []map[string]string

	switch v := fv.Value.(type) {
	case []map[string]string:
		items = v
	case []any:
		for _, item := range v {
			switch m := item.(type) {
			case map[string]string:
				items = append(items, m)
			case map[string]any:
				entry := make(map[string]string)
				for k, val := range m {
					if s, ok := val.(string); ok {
						entry[k] = s
					}
				}
				items = append(items, entry)
			}
		}
	default:
		return nil
	}

	if len(items) == 0 {
		return nil
	}

	var contacts []map[string]any
	for i, c := range items {
		if i >= 3 {
			break
		}
		sf := make(map[string]any)
		mapField := func(jsonKey, sfField string) {
			if v, ok := c[jsonKey]; ok && v != "" {
				sf[sfField] = v
			}
		}
		mapField("first_name", "FirstName")
		mapField("last_name", "LastName")
		mapField("title", "Title")
		mapField("email", "Email")
		mapField("phone", "Phone")
		mapField("linkedin_url", "LinkedIn_URL__c")

		// LastName is required for SF Contact.
		if sf["LastName"] != nil {
			contacts = append(contacts, sf)
		}
	}

	if len(contacts) == 0 {
		return nil
	}
	return contacts
}
