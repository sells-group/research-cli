package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// FedsyncProgress handles GET /api/workflows/fedsync/progress.
// Queries the Temporal workflow for fedsync_progress.
func (h *Handlers) FedsyncProgress(w http.ResponseWriter, r *http.Request) {
	if h.temporalClient == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "no_temporal", "Temporal client not configured")
		return
	}

	workflowID := r.URL.Query().Get("workflow_id")
	if workflowID == "" {
		WriteError(w, r, http.StatusBadRequest, "missing_workflow_id", "workflow_id query param required")
		return
	}

	resp, err := h.temporalClient.QueryWorkflow(r.Context(), workflowID, "", "fedsync_progress")
	if err != nil {
		zap.L().Warn("fedsync progress query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}

	var progress json.RawMessage
	if err := resp.Get(&progress); err != nil {
		WriteError(w, r, http.StatusInternalServerError, "decode_failed", err.Error())
		return
	}
	WriteJSON(w, http.StatusOK, progress)
}

// EnrichmentProgress handles GET /api/workflows/enrichment/{runID}.
// Queries the Temporal workflow for enrichment_progress.
func (h *Handlers) EnrichmentProgress(w http.ResponseWriter, r *http.Request) {
	if h.temporalClient == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "no_temporal", "Temporal client not configured")
		return
	}

	workflowID := chi.URLParam(r, "runID")
	if workflowID == "" {
		WriteError(w, r, http.StatusBadRequest, "missing_run_id", "runID path param required")
		return
	}

	resp, err := h.temporalClient.QueryWorkflow(r.Context(), workflowID, "", "enrichment_progress")
	if err != nil {
		zap.L().Warn("enrichment progress query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}

	var progress json.RawMessage
	if err := resp.Get(&progress); err != nil {
		WriteError(w, r, http.StatusInternalServerError, "decode_failed", err.Error())
		return
	}
	WriteJSON(w, http.StatusOK, progress)
}

// BatchPause handles POST /api/workflows/batch/pause.
func (h *Handlers) BatchPause(w http.ResponseWriter, r *http.Request) {
	if h.temporalClient == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "no_temporal", "Temporal client not configured")
		return
	}

	workflowID := r.URL.Query().Get("workflow_id")
	if workflowID == "" {
		WriteError(w, r, http.StatusBadRequest, "missing_workflow_id", "workflow_id query param required")
		return
	}

	if err := h.temporalClient.SignalWorkflow(r.Context(), workflowID, "", "pause_batch", nil); err != nil {
		zap.L().Warn("batch pause signal failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "signal_failed", err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// BatchResume handles POST /api/workflows/batch/resume.
func (h *Handlers) BatchResume(w http.ResponseWriter, r *http.Request) {
	if h.temporalClient == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "no_temporal", "Temporal client not configured")
		return
	}

	workflowID := r.URL.Query().Get("workflow_id")
	if workflowID == "" {
		WriteError(w, r, http.StatusBadRequest, "missing_workflow_id", "workflow_id query param required")
		return
	}

	if err := h.temporalClient.SignalWorkflow(r.Context(), workflowID, "", "resume_batch", nil); err != nil {
		zap.L().Warn("batch resume signal failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "signal_failed", err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}
