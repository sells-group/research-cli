package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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
	Score           float64        `json:"score"`
	ScoreBreakdown  ScoreBreakdown `json:"score_breakdown"`
	Passed          bool           `json:"passed"`
	SFUpdated       bool           `json:"sf_updated"`
	DedupMatch      bool           `json:"dedup_match"`
	ManualReview    bool           `json:"manual_review"`
	MissingRequired []string       `json:"missing_required,omitempty"`
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

	// Validate required fields before writing to SF.
	if missing := validateRequiredFields(result.FieldValues, fields); len(missing) > 0 {
		gate.MissingRequired = missing
		zap.L().Warn("gate: missing required fields",
			zap.Strings("missing", missing),
			zap.String("company", result.Company.Name),
		)
	}

	// Check minimum completeness floor.
	if cfg.Pipeline.MinCompletenessThreshold > 0 && breakdown.Completeness < cfg.Pipeline.MinCompletenessThreshold {
		gate.Passed = false
		zap.L().Warn("gate: completeness below minimum floor",
			zap.Float64("completeness", breakdown.Completeness),
			zap.Float64("min_threshold", cfg.Pipeline.MinCompletenessThreshold),
			zap.String("company", result.Company.Name),
		)
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
			injectGeoFields(accountFields, result.GeoData)

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
				// No SF ID — check for existing Account by website before creating.
				resolvedID, err := resolveOrCreateAccount(gCtx, sfClient, notionClient, result, accountFields, gate)
				if err != nil {
					sfErr = err
					return eris.Wrap(err, "gate: sf resolve or create")
				}
				accountID = resolvedID
			}

			// Upsert Contacts — dedup by email/name, then create or update.
			if accountID != "" {
				contacts := extractContactsForSF(result.FieldValues, fields)
				if contacts == nil && len(contactFields) > 0 {
					contacts = []map[string]any{contactFields}
				}
				upsertContacts(gCtx, sfClient, accountID, contacts, result.Company.Name)
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

// validateRequiredFields checks that all registry-required fields have non-nil
// values in fieldValues. Returns the list of missing required field keys.
func validateRequiredFields(fieldValues map[string]model.FieldValue, registry *model.FieldRegistry) []string {
	if registry == nil {
		return nil
	}
	var missing []string
	for _, f := range registry.Required() {
		fv, ok := fieldValues[f.Key]
		if !ok || fv.Value == nil {
			missing = append(missing, f.Key)
		}
	}
	return missing
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

// injectGeoFields adds geographic enrichment data from Phase 7D to the
// Salesforce account field map. No-op if geo data is nil.
func injectGeoFields(fields map[string]any, gd *model.GeoData) {
	if gd == nil {
		return
	}
	if gd.Latitude != 0 {
		fields["Latitude__c"] = gd.Latitude
	}
	if gd.Longitude != 0 {
		fields["Longitude__c"] = gd.Longitude
	}
	if gd.MSAName != "" {
		fields["MSA_Name__c"] = gd.MSAName
	}
	if gd.CBSACode != "" {
		fields["MSA_CBSA_Code__c"] = gd.CBSACode
	}
	if gd.Classification != "" {
		fields["Urban_Classification__c"] = gd.Classification
	}
	if gd.CentroidKM != 0 {
		fields["Distance_to_MSA_Center_km__c"] = gd.CentroidKM
	}
	if gd.EdgeKM != 0 {
		fields["Distance_to_MSA_Edge_km__c"] = gd.EdgeKM
	}
	if gd.CountyFIPS != "" {
		fields["County_FIPS__c"] = gd.CountyFIPS
	}
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
func extractContactsForSF(fieldValues map[string]model.FieldValue, _ *model.FieldRegistry) []map[string]any {
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

	if len(items) > 3 {
		zap.L().Warn("gate: truncating contacts",
			zap.Int("total", len(items)),
			zap.Int("limit", 3),
		)
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

// upsertContacts queries existing contacts for an Account and matches enriched
// contacts by email (primary) or first+last name (fallback). Matched contacts
// are updated; unmatched contacts are created.
// contactUpsertResult holds counts from a contact upsert operation.
type contactUpsertResult struct {
	Created int
	Updated int
	Failed  int
}

func upsertContacts(ctx context.Context, sfClient salesforce.Client, accountID string, enrichedContacts []map[string]any, companyName string) contactUpsertResult {
	var res contactUpsertResult
	if len(enrichedContacts) == 0 || accountID == "" {
		return res
	}

	existing, err := salesforce.FindContactsByAccountID(ctx, sfClient, accountID)
	if err != nil {
		zap.L().Warn("gate: contact dedup lookup failed, creating all",
			zap.String("company", companyName),
			zap.Error(err),
		)
		// Fall back to creating all contacts.
		for i, cf := range enrichedContacts {
			if len(cf) == 0 {
				continue
			}
			if _, createErr := salesforce.CreateContact(ctx, sfClient, accountID, cf); createErr != nil {
				res.Failed++
				zap.L().Warn("gate: salesforce create contact failed",
					zap.String("company", companyName),
					zap.Int("contact_index", i),
					zap.Error(createErr),
				)
			} else {
				res.Created++
			}
		}
		return res
	}

	// Build lookup indices for matching.
	byEmail := make(map[string]salesforce.Contact)
	byName := make(map[string]salesforce.Contact)
	for _, c := range existing {
		if c.Email != "" {
			byEmail[strings.ToLower(c.Email)] = c
		}
		nameKey := strings.ToLower(c.FirstName + "|" + c.LastName)
		byName[nameKey] = c
	}

	for i, cf := range enrichedContacts {
		if len(cf) == 0 {
			continue
		}

		// Try matching by email (primary), then first+last name (fallback).
		var match *salesforce.Contact
		if email, ok := cf["Email"].(string); ok && email != "" {
			if m, found := byEmail[strings.ToLower(email)]; found {
				match = &m
			}
		}
		if match == nil {
			firstName, _ := cf["FirstName"].(string)
			lastName, _ := cf["LastName"].(string)
			nameKey := strings.ToLower(firstName + "|" + lastName)
			if m, found := byName[nameKey]; found {
				match = &m
			}
		}

		if match != nil {
			// Update existing contact.
			if updateErr := salesforce.UpdateContact(ctx, sfClient, match.ID, cf); updateErr != nil {
				res.Failed++
				zap.L().Warn("gate: salesforce update contact failed",
					zap.String("company", companyName),
					zap.String("contact_id", match.ID),
					zap.Int("contact_index", i),
					zap.Error(updateErr),
				)
			} else {
				res.Updated++
			}
		} else {
			// No match — create new contact.
			if _, createErr := salesforce.CreateContact(ctx, sfClient, accountID, cf); createErr != nil {
				res.Failed++
				zap.L().Warn("gate: salesforce create contact failed",
					zap.String("company", companyName),
					zap.Int("contact_index", i),
					zap.Error(createErr),
				)
			} else {
				res.Created++
			}
		}
	}
	return res
}

// resolveOrCreateAccount checks for an existing Account by website before creating.
// If a match is found, it updates the existing Account instead. Returns the Account ID.
func resolveOrCreateAccount(ctx context.Context, sfClient salesforce.Client, notionClient notion.Client, result *model.EnrichmentResult, accountFields map[string]any, gate *GateResult) (string, error) {
	// Attempt dedup lookup by website.
	if result.Company.URL != "" {
		existing, findErr := salesforce.FindAccountByWebsite(ctx, sfClient, result.Company.URL)
		if findErr != nil {
			zap.L().Warn("gate: dedup lookup failed, proceeding with create",
				zap.String("company", result.Company.Name),
				zap.Error(findErr),
			)
		} else if existing != nil {
			// Duplicate found — update existing Account instead of creating.
			gate.DedupMatch = true
			result.Company.SalesforceID = existing.ID
			zap.L().Info("gate: dedup match found, updating existing account",
				zap.String("company", result.Company.Name),
				zap.String("existing_sf_id", existing.ID),
				zap.String("existing_name", existing.Name),
			)

			if len(accountFields) > 0 {
				if err := salesforce.UpdateAccount(ctx, sfClient, existing.ID, accountFields); err != nil {
					return "", eris.Wrap(err, "gate: sf update (dedup)")
				}
			}
			gate.SFUpdated = true

			// Write resolved SF ID back to Notion.
			writeSFIDToNotion(ctx, notionClient, result, existing.ID)
			return existing.ID, nil
		}
	}

	// No existing Account — create new.
	newID, err := salesforce.CreateAccount(ctx, sfClient, accountFields)
	if err != nil {
		zap.L().Error("gate: salesforce create failed",
			zap.String("company", result.Company.Name),
			zap.Error(err),
		)
		return "", eris.Wrap(err, "gate: sf create")
	}
	result.Company.SalesforceID = newID
	gate.SFUpdated = true

	// Write new SF ID back to Notion.
	writeSFIDToNotion(ctx, notionClient, result, newID)
	return newID, nil
}

// writeSFIDToNotion writes the Salesforce ID back to the Notion Lead Tracker page.
func writeSFIDToNotion(ctx context.Context, notionClient notion.Client, result *model.EnrichmentResult, sfID string) {
	if notionClient != nil && result.Company.NotionPageID != "" {
		if err := writeNotionSalesforceID(ctx, notionClient, result.Company.NotionPageID, sfID); err != nil {
			zap.L().Warn("gate: failed to write SF ID to Notion",
				zap.String("company", result.Company.Name),
				zap.Error(err),
			)
		}
	}
}

// --- Deferred SF Write Support (Batch Mode) ---

// SFWriteIntent captures a deferred Salesforce write operation for batch aggregation.
// Built by PrepareGate, executed by FlushSFWrites.
type SFWriteIntent struct {
	// AccountOp is the account operation: "create", "update", or "" (no SF write needed).
	AccountOp string

	// AccountID is the existing Salesforce Account ID (populated for updates and dedup matches).
	AccountID string

	// AccountFields are the fields to write to the Account sObject.
	AccountFields map[string]any

	// Contacts are the Contact field maps to create. AccountId is injected during flush.
	Contacts []map[string]any

	// NotionPageID is the Notion page to update with the resolved SF ID.
	NotionPageID string

	// DedupMatch indicates an existing Account was found by website during dedup lookup.
	DedupMatch bool

	// Result is a back-reference to update with the resolved SF ID after flush.
	Result *model.EnrichmentResult
}

// PrepareGate computes the quality gate score, performs dedup lookup, and builds
// an SFWriteIntent without executing any SF writes. The Notion status update and
// ToolJet webhook still execute immediately. Used by batch mode to aggregate
// SF writes across many companies.
func PrepareGate(ctx context.Context, result *model.EnrichmentResult, fields *model.FieldRegistry, questions []model.Question, sfClient salesforce.Client, notionClient notion.Client, cfg *config.Config) (*GateResult, *SFWriteIntent, error) {
	breakdown := ComputeScore(result.FieldValues, fields, questions, result.Answers, cfg.Pipeline.QualityWeights)
	score := breakdown.Final
	result.Score = score
	threshold := cfg.Pipeline.QualityScoreThreshold

	gate := &GateResult{
		Score:          score,
		ScoreBreakdown: breakdown,
		Passed:         score >= threshold,
	}

	// Validate required fields before writing to SF.
	if missing := validateRequiredFields(result.FieldValues, fields); len(missing) > 0 {
		gate.MissingRequired = missing
		zap.L().Warn("gate: missing required fields",
			zap.Strings("missing", missing),
			zap.String("company", result.Company.Name),
		)
	}

	// Check minimum completeness floor.
	if cfg.Pipeline.MinCompletenessThreshold > 0 && breakdown.Completeness < cfg.Pipeline.MinCompletenessThreshold {
		gate.Passed = false
		zap.L().Warn("gate: completeness below minimum floor",
			zap.Float64("completeness", breakdown.Completeness),
			zap.Float64("min_threshold", cfg.Pipeline.MinCompletenessThreshold),
			zap.String("company", result.Company.Name),
		)
	}

	g, gCtx := errgroup.WithContext(ctx)

	var intent *SFWriteIntent
	var notionErr error

	// Build SF write intent (includes dedup lookup but no writes).
	g.Go(func() error {
		if gate.Passed && sfClient != nil {
			accountFields, contactFields := buildSFFieldsByObject(result.FieldValues, fields)
			if result.Report != "" {
				accountFields["Enrichment_Report__c"] = result.Report
			}
			ensureMinimumSFFields(accountFields, result.Company, result.FieldValues)
			injectGeoFields(accountFields, result.GeoData)

			intent = &SFWriteIntent{
				AccountFields: accountFields,
				NotionPageID:  result.Company.NotionPageID,
				Result:        result,
			}

			accountID := result.Company.SalesforceID
			if accountID != "" {
				// Existing account — update.
				intent.AccountOp = "update"
				intent.AccountID = accountID
			} else {
				// No SF ID — check for existing Account by website (dedup).
				if result.Company.URL != "" {
					existing, findErr := salesforce.FindAccountByWebsite(gCtx, sfClient, result.Company.URL)
					if findErr != nil {
						zap.L().Warn("gate: dedup lookup failed, proceeding with create",
							zap.String("company", result.Company.Name),
							zap.Error(findErr),
						)
					} else if existing != nil {
						intent.AccountOp = "update"
						intent.AccountID = existing.ID
						intent.DedupMatch = true
						gate.DedupMatch = true
						result.Company.SalesforceID = existing.ID
						zap.L().Info("gate: dedup match found (deferred)",
							zap.String("company", result.Company.Name),
							zap.String("existing_sf_id", existing.ID),
						)
					}
				}
				if intent.AccountOp == "" {
					intent.AccountOp = "create"
				}
			}

			// Collect contacts.
			contacts := extractContactsForSF(result.FieldValues, fields)
			if contacts == nil && len(contactFields) > 0 {
				contacts = []map[string]any{contactFields}
			}
			intent.Contacts = contacts
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

	// Notion update (same as QualityGate).
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

	_ = g.Wait()

	// Retry Notion on failure.
	if notionErr != nil && result.Company.NotionPageID != "" {
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

	return gate, intent, nil
}

// FlushFailure records a single failed SF write for error aggregation.
type FlushFailure struct {
	Company string `json:"company"`
	Op      string `json:"op"`
	Error   string `json:"error"`
}

// FlushSummary aggregates results from a batch SF write flush.
type FlushSummary struct {
	AccountsCreated int            `json:"accounts_created"`
	AccountsFailed  int            `json:"accounts_failed"`
	AccountsUpdated int            `json:"accounts_updated"`
	UpdatesFailed   int            `json:"updates_failed"`
	ContactsCreated int            `json:"contacts_created"`
	ContactsUpdated int            `json:"contacts_updated"`
	ContactsFailed  int            `json:"contacts_failed"`
	Failures        []FlushFailure `json:"failures,omitempty"`
}

// LogSummary emits the flush summary as a structured zap log entry.
func (s *FlushSummary) LogSummary() {
	fields := []zap.Field{
		zap.Int("accounts_created", s.AccountsCreated),
		zap.Int("accounts_failed", s.AccountsFailed),
		zap.Int("accounts_updated", s.AccountsUpdated),
		zap.Int("updates_failed", s.UpdatesFailed),
		zap.Int("contacts_created", s.ContactsCreated),
		zap.Int("contacts_updated", s.ContactsUpdated),
		zap.Int("contacts_failed", s.ContactsFailed),
		zap.Int("total_failures", len(s.Failures)),
	}

	if len(s.Failures) > 0 {
		msgs := make([]string, len(s.Failures))
		for i, f := range s.Failures {
			msgs[i] = f.Company + " (" + f.Op + "): " + f.Error
		}
		fields = append(fields, zap.Strings("failure_details", msgs))
	}

	zap.L().Info("flush: SF write summary", fields...)
}

// FlushSFWrites executes deferred SF write intents in bulk using the Collections API.
// Ordering: creates → updates → contacts → Notion SF ID writebacks.
// Returns a FlushSummary with aggregate results for batch reporting.
func FlushSFWrites(ctx context.Context, sfClient salesforce.Client, notionClient notion.Client, intents []*SFWriteIntent) (*FlushSummary, error) {
	summary := &FlushSummary{}

	if len(intents) == 0 {
		return summary, nil
	}

	// Separate by operation type.
	var creates, updates []*SFWriteIntent
	for _, intent := range intents {
		if intent == nil || intent.AccountOp == "" {
			continue
		}
		switch intent.AccountOp {
		case "create":
			creates = append(creates, intent)
		case "update":
			updates = append(updates, intent)
		}
	}

	// 1. Bulk create accounts.
	if len(creates) > 0 {
		records := make([]map[string]any, len(creates))
		for i, c := range creates {
			records[i] = c.AccountFields
		}
		results, err := salesforce.BulkCreateAccounts(ctx, sfClient, records)
		if err != nil {
			return summary, eris.Wrap(err, "flush: bulk create accounts")
		}
		for i, r := range results {
			if i >= len(creates) {
				break
			}
			if r.Success {
				creates[i].AccountID = r.ID
				creates[i].Result.Company.SalesforceID = r.ID
				summary.AccountsCreated++
			} else {
				summary.AccountsFailed++
				company := creates[i].Result.Company.Name
				errMsg := strings.Join(r.Errors, "; ")
				summary.Failures = append(summary.Failures, FlushFailure{
					Company: company,
					Op:      "account_create",
					Error:   errMsg,
				})
				zap.L().Warn("flush: account create failed",
					zap.String("company", company),
					zap.Strings("errors", r.Errors),
				)
			}
		}
	}

	// 2. Bulk update accounts.
	if len(updates) > 0 {
		accountUpdates := make([]salesforce.AccountUpdate, 0, len(updates))
		updateIntentIndex := make([]int, 0, len(updates))
		for idx, u := range updates {
			if len(u.AccountFields) > 0 {
				accountUpdates = append(accountUpdates, salesforce.AccountUpdate{
					ID:     u.AccountID,
					Fields: u.AccountFields,
				})
				updateIntentIndex = append(updateIntentIndex, idx)
			}
		}
		if len(accountUpdates) > 0 {
			results, err := salesforce.BulkUpdateAccounts(ctx, sfClient, accountUpdates)
			if err != nil {
				return summary, eris.Wrap(err, "flush: bulk update accounts")
			}
			for i, r := range results {
				if i >= len(updateIntentIndex) {
					break
				}
				intent := updates[updateIntentIndex[i]]
				if r.Success {
					summary.AccountsUpdated++
				} else {
					summary.UpdatesFailed++
					company := intent.Result.Company.Name
					errMsg := strings.Join(r.Errors, "; ")
					summary.Failures = append(summary.Failures, FlushFailure{
						Company: company,
						Op:      "account_update",
						Error:   errMsg,
					})
					zap.L().Warn("flush: account update failed",
						zap.String("account_id", accountUpdates[i].ID),
						zap.Strings("errors", r.Errors),
					)
				}
			}
		}
	}

	// 3. Upsert contacts per-intent (dedup against existing contacts).
	for _, intent := range intents {
		if intent == nil || intent.AccountID == "" || len(intent.Contacts) == 0 {
			continue
		}
		companyName := ""
		if intent.Result != nil {
			companyName = intent.Result.Company.Name
		}
		cr := upsertContacts(ctx, sfClient, intent.AccountID, intent.Contacts, companyName)
		summary.ContactsCreated += cr.Created
		summary.ContactsUpdated += cr.Updated
		summary.ContactsFailed += cr.Failed
	}

	// 4. Write SF IDs back to Notion.
	for _, intent := range intents {
		if intent == nil || intent.Result == nil {
			continue
		}
		sfID := intent.Result.Company.SalesforceID
		if sfID != "" {
			writeSFIDToNotion(ctx, notionClient, intent.Result, sfID)
		}
	}

	summary.LogSummary()
	return summary, nil
}
