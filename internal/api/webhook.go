package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/model"
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
	if h.starter != nil {
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

	h.invalidateRunsCache()
	WriteJSON(w, http.StatusAccepted, map[string]string{
		"status":  "accepted",
		"company": req.URL,
	})
}

// webhookEnrichViaWorkflow starts or reuses an EnrichCompanyWorkflow.
func (h *Handlers) webhookEnrichViaWorkflow(w http.ResponseWriter, r *http.Request, comp model.Company) {
	requestID := middleware.GetReqID(r.Context())
	result, err := h.starter.StartWebhook(r.Context(), comp, requestID)
	if err != nil {
		zap.L().Error("failed to start enrichment workflow",
			zap.String("company", comp.URL),
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		WriteError(w, r, http.StatusInternalServerError, "workflow_error", "failed to start enrichment workflow")
		return
	}

	zap.L().Info("enrichment workflow accepted via webhook",
		zap.String("company", comp.URL),
		zap.String("request_id", requestID),
		zap.String("workflow_id", result.WorkflowID),
		zap.String("workflow_run_id", result.WorkflowRunID),
		zap.Bool("reused", result.Reused),
	)

	h.invalidateRunsCache()
	WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":          "accepted",
		"company":         comp.URL,
		"workflow_id":     result.WorkflowID,
		"workflow_run_id": result.WorkflowRunID,
		"reused":          result.Reused,
	})
}
