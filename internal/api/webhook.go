package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

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

	// Check semaphore capacity before accepting.
	select {
	case h.sem <- struct{}{}:
		// Acquired slot.
	default:
		WriteError(w, r, http.StatusServiceUnavailable, "at_capacity", "too many concurrent requests")
		return
	}

	company := model.Company{
		URL:          req.URL,
		SalesforceID: req.SalesforceID,
		Name:         req.Name,
	}

	// Run enrichment asynchronously with a background context so
	// in-flight jobs are not cancelled immediately on SIGINT.
	h.wg.Add(1)
	go func() {
		defer func() { <-h.sem }()
		defer h.wg.Done()
		defer func() {
			if rv := recover(); rv != nil {
				zap.L().Error("webhook enrichment panicked",
					zap.String("company", company.URL),
					zap.Any("panic", rv),
					zap.Stack("stack"),
				)
			}
		}()
		if h.runner == nil {
			zap.L().Error("webhook enrichment skipped: pipeline not initialized",
				zap.String("company", company.URL))
			return
		}
		jobCtx, jobCancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer jobCancel()
		result, err := h.runner.Run(jobCtx, company)
		if err != nil {
			zap.L().Error("webhook enrichment failed",
				zap.String("company", company.URL),
				zap.Error(err),
			)
			return
		}
		zap.L().Info("webhook enrichment complete",
			zap.String("company", company.URL),
			zap.Float64("score", result.Score),
		)
	}()

	WriteJSON(w, http.StatusAccepted, map[string]string{
		"status":  "accepted",
		"company": req.URL,
	})
}
