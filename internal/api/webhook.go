package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
	temporalpkg "github.com/sells-group/research-cli/internal/temporal"
	temporalenrich "github.com/sells-group/research-cli/internal/temporal/enrichment"
)

// enrichRequest is the JSON body for POST /webhook/enrich.
type enrichRequest struct {
	URL          string `json:"url"`
	SalesforceID string `json:"salesforce_id"`
	Name         string `json:"name"`
}

// WebhookEnrich handles POST /webhook/enrich.
func (h *Handlers) WebhookEnrich(w http.ResponseWriter, r *http.Request) {
	var req enrichRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}

	if req.URL == "" {
		WriteError(w, r, http.StatusBadRequest, "missing_url", "url is required")
		return
	}

	comp := model.Company{
		URL:          req.URL,
		SalesforceID: req.SalesforceID,
		Name:         req.Name,
	}

	// Temporal path: start a workflow instead of a goroutine.
	if h.temporalClient != nil {
		h.webhookEnrichViaWorkflow(w, r, comp)
		return
	}

	// Legacy path: run enrichment in a goroutine with semaphore.
	select {
	case h.sem <- struct{}{}:
		// Acquired slot.
	default:
		WriteError(w, r, http.StatusServiceUnavailable, "at_capacity", "too many concurrent requests")
		return
	}

	h.wg.Add(1)
	go func() {
		defer func() { <-h.sem }()
		defer h.wg.Done()
		defer func() {
			if rv := recover(); rv != nil {
				zap.L().Error("webhook enrichment panicked",
					zap.String("company", comp.URL),
					zap.Any("panic", rv),
					zap.Stack("stack"),
				)
			}
		}()
		if h.runner == nil {
			zap.L().Error("webhook enrichment skipped: pipeline not initialized",
				zap.String("company", comp.URL))
			return
		}
		jobCtx, jobCancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer jobCancel()
		result, err := h.runner.Run(jobCtx, comp)
		if err != nil {
			zap.L().Error("webhook enrichment failed",
				zap.String("company", comp.URL),
				zap.Error(err),
			)
			return
		}
		zap.L().Info("webhook enrichment complete",
			zap.String("company", comp.URL),
			zap.Float64("score", result.Score),
		)
	}()

	WriteJSON(w, http.StatusAccepted, map[string]string{
		"status":  "accepted",
		"company": req.URL,
	})
}

// webhookEnrichViaWorkflow starts an EnrichCompanyWorkflow on Temporal.
func (h *Handlers) webhookEnrichViaWorkflow(w http.ResponseWriter, r *http.Request, comp model.Company) {
	workflowID := fmt.Sprintf("enrich-webhook-%s-%d", comp.URL, time.Now().UnixNano())
	run, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporalpkg.EnrichmentTaskQueue,
	}, temporalenrich.EnrichCompanyWorkflow, temporalenrich.EnrichCompanyParams{
		Company: comp,
	})
	if err != nil {
		zap.L().Error("failed to start enrichment workflow",
			zap.String("company", comp.URL),
			zap.Error(err),
		)
		WriteError(w, r, http.StatusInternalServerError, "workflow_error", "failed to start enrichment workflow")
		return
	}

	zap.L().Info("enrichment workflow started via webhook",
		zap.String("company", comp.URL),
		zap.String("workflow_id", run.GetID()),
		zap.String("run_id", run.GetRunID()),
	)

	WriteJSON(w, http.StatusAccepted, map[string]string{
		"status":      "accepted",
		"company":     comp.URL,
		"workflow_id": run.GetID(),
		"run_id":      run.GetRunID(),
	})
}
