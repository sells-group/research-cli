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
	Score      float64 `json:"score"`
	Passed     bool    `json:"passed"`
	SFUpdated  bool    `json:"sf_updated"`
	ManualReview bool  `json:"manual_review"`
}

// QualityGate implements Phase 9: evaluate quality score, update Salesforce,
// send to ToolJet for manual review if needed, and update Notion status.
func QualityGate(ctx context.Context, result *model.EnrichmentResult, fields *model.FieldRegistry, sfClient salesforce.Client, notionClient notion.Client, cfg *config.Config) (*GateResult, error) {
	score := ComputeScore(result.FieldValues, fields)
	result.Score = score
	threshold := cfg.Pipeline.QualityScoreThreshold

	gate := &GateResult{
		Score:  score,
		Passed: score >= threshold,
	}

	// Run SF/ToolJet and Notion updates concurrently — they are independent.
	g, gCtx := errgroup.WithContext(ctx)

	var sfErr, notionErr error

	// SF or ToolJet update.
	g.Go(func() error {
		if gate.Passed {
			if result.Company.SalesforceID != "" {
				sfFields := buildSFFields(result.FieldValues)
				if result.Report != "" {
					sfFields["Enrichment_Report__c"] = result.Report
				}
				if len(sfFields) > 0 {
					if err := salesforce.UpdateAccount(gCtx, sfClient, result.Company.SalesforceID, sfFields); err != nil {
						sfErr = err
						zap.L().Error("gate: salesforce update failed",
							zap.String("company", result.Company.Name),
							zap.Error(err),
						)
						return eris.Wrap(err, "gate: sf update")
					}
					gate.SFUpdated = true
				}
			}
		} else {
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
	defer resp.Body.Close()

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
