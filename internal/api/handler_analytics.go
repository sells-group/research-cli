package api

import (
	"net/http"
	"strconv"

	"go.uber.org/zap"
)

// SyncTrends handles GET /analytics/sync-trends.
func (h *Handlers) SyncTrends(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.requireAnalytics(w, r) {
		return
	}

	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	if days <= 0 {
		days = 30
	}

	results, err := h.readModel.Analytics.SyncTrends(ctx, days)
	if err != nil {
		zap.L().Error("sync trends query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get sync trends")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"days":   days,
		"trends": results,
	})
}

// IdentifierCoverage handles GET /analytics/identifier-coverage.
func (h *Handlers) IdentifierCoverage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.requireAnalytics(w, r) {
		return
	}

	results, err := h.readModel.Analytics.IdentifierCoverage(ctx)
	if err != nil {
		zap.L().Error("identifier coverage query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get identifier coverage")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"coverage": results,
	})
}

// XrefCoverage handles GET /analytics/xref-coverage.
func (h *Handlers) XrefCoverage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.requireAnalytics(w, r) {
		return
	}

	results, err := h.readModel.Analytics.XrefCoverage(ctx)
	if err != nil {
		zap.L().Error("xref coverage query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get xref coverage")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"coverage": results,
	})
}

// EnrichmentStatsHandler handles GET /analytics/enrichment-stats.
// Uses the runs table (always present) instead of enrichment_runs.
func (h *Handlers) EnrichmentStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireAnalytics(w, r) {
		return
	}

	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	if days <= 0 {
		days = 30
	}

	stats, err := h.readModel.Analytics.EnrichmentStats(ctx, days)
	if err != nil {
		zap.L().Error("enrichment stats query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get enrichment stats")
		return
	}

	WriteJSON(w, http.StatusOK, stats)
}

// CostBreakdownHandler handles GET /analytics/cost-breakdown.
func (h *Handlers) CostBreakdownHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireAnalytics(w, r) {
		return
	}

	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	if days <= 0 {
		days = 30
	}

	results, err := h.readModel.Analytics.CostBreakdown(ctx, days)
	if err != nil {
		zap.L().Error("cost breakdown query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get cost breakdown")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"days":      days,
		"breakdown": results,
	})
}
