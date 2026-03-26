package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/store"
)

// ListRuns handles GET /runs with pagination and filtering.
func (h *Handlers) ListRuns(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireStore(w, r) {
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	filter := store.RunFilter{
		Status:     model.RunStatus(r.URL.Query().Get("status")),
		CompanyURL: r.URL.Query().Get("company_url"),
		Limit:      limit,
		Offset:     offset,
	}

	runs, err := h.store.ListRuns(ctx, filter)
	if err != nil {
		zap.L().Error("list runs failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to list runs")
		return
	}

	total, err := h.store.CountRuns(ctx, filter)
	if err != nil {
		zap.L().Error("count runs failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to count runs")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"runs":   runs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// GetRun handles GET /runs/{id}.
func (h *Handlers) GetRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireStore(w, r) {
		return
	}
	runID := chi.URLParam(r, "id")

	run, err := h.store.GetRun(ctx, runID)
	if err != nil {
		zap.L().Error("get run failed", zap.String("run_id", runID), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get run")
		return
	}
	if run == nil {
		WriteError(w, r, http.StatusNotFound, "not_found", "run not found")
		return
	}

	WriteJSON(w, http.StatusOK, run)
}

// GetRunProvenance handles GET /runs/{id}/provenance.
func (h *Handlers) GetRunProvenance(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireStore(w, r) {
		return
	}
	runID := chi.URLParam(r, "id")

	records, err := h.store.GetProvenance(ctx, runID)
	if err != nil {
		zap.L().Error("get provenance failed", zap.String("run_id", runID), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get provenance")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"run_id":     runID,
		"provenance": records,
	})
}

// RetryRun handles POST /runs/{id}/retry.
func (h *Handlers) RetryRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireStore(w, r) {
		return
	}
	runID := chi.URLParam(r, "id")

	run, err := h.store.GetRun(ctx, runID)
	if err != nil {
		zap.L().Error("retry run: get failed", zap.String("run_id", runID), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get run")
		return
	}
	if run == nil {
		WriteError(w, r, http.StatusNotFound, "not_found", "run not found")
		return
	}
	if run.Status != model.RunStatusFailed {
		WriteError(w, r, http.StatusBadRequest, "invalid_status", "only failed runs can be retried")
		return
	}

	comp := run.Company

	// Temporal path: start a workflow.
	if h.starter != nil {
		requestID := middleware.GetReqID(r.Context())
		result, startErr := h.starter.StartRetry(ctx, runID, comp, requestID)
		if startErr != nil {
			zap.L().Error("retry run: start workflow failed",
				zap.String("run_id", runID),
				zap.String("request_id", requestID),
				zap.Error(startErr),
			)
			WriteError(w, r, http.StatusInternalServerError, "workflow_error", "failed to start enrichment workflow")
			return
		}
		h.invalidateRunsCache()
		WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":          "accepted",
			"company":         comp.URL,
			"workflow_id":     result.WorkflowID,
			"workflow_run_id": result.WorkflowRunID,
			"original_run_id": runID,
			"reused":          result.Reused,
		})
		return
	}

	// Legacy path: run enrichment in a goroutine.
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
		if h.runner == nil {
			zap.L().Error("retry run: pipeline not initialized", zap.String("company", comp.URL))
			return
		}
		jobCtx, jobCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Minute)
		defer jobCancel()
		result, runErr := h.runner.Run(jobCtx, comp)
		if runErr != nil {
			zap.L().Error("retry enrichment failed", zap.String("company", comp.URL), zap.Error(runErr))
			return
		}
		zap.L().Info("retry enrichment complete", zap.String("company", comp.URL), zap.Float64("score", result.Score))
	}()

	h.invalidateRunsCache()
	WriteJSON(w, http.StatusAccepted, map[string]string{
		"status":          "accepted",
		"company":         comp.URL,
		"original_run_id": runID,
	})
}

// queueStatusResponse is the response body for GET /queue/status.
type queueStatusResponse struct {
	Queued   int `json:"queued"`
	Running  int `json:"running"`
	Complete int `json:"complete"`
	Failed   int `json:"failed"`
	Total    int `json:"total"`
}

// runningStatuses are all active (non-terminal, non-queued) run statuses.
var runningStatuses = map[string]bool{
	string(model.RunStatusCrawling):    true,
	string(model.RunStatusClassifying): true,
	string(model.RunStatusExtracting):  true,
	string(model.RunStatusAggregating): true,
	string(model.RunStatusWritingSF):   true,
}

// QueueStatus handles GET /queue/status.
func (h *Handlers) QueueStatus(w http.ResponseWriter, r *http.Request) {
	if !h.requireStore(w, r) {
		return
	}
	if cached, ok := h.cache.Get(apicache.KeyQueueStatus); ok {
		writeCachedJSON(w, cached)
		return
	}

	ctx := r.Context()

	counts, err := h.store.CountRunsByStatus(ctx)
	if err != nil {
		zap.L().Error("queue status failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get queue status")
		return
	}

	var running int
	for status, n := range counts {
		if runningStatuses[status] {
			running += n
		}
	}

	queued := counts[string(model.RunStatusQueued)]
	complete := counts[string(model.RunStatusComplete)]
	failed := counts[string(model.RunStatusFailed)]

	resp := queueStatusResponse{
		Queued:   queued,
		Running:  running,
		Complete: complete,
		Failed:   failed,
		Total:    queued + running + complete + failed,
	}

	if err := h.cache.Set(apicache.KeyQueueStatus, resp, 15*time.Second); err != nil {
		zap.L().Warn("cache queue status failed", zap.Error(err))
	}
	WriteJSON(w, http.StatusOK, resp)
}
