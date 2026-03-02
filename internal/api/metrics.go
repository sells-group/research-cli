package api

import "net/http"

// Metrics handles GET /metrics and GET /api/v1/metrics.
func (h *Handlers) Metrics(w http.ResponseWriter, r *http.Request) {
	if h.collector == nil {
		WriteError(w, r, http.StatusServiceUnavailable, "metrics_unavailable", "metrics collector not configured")
		return
	}

	lookback := 24
	if h.cfg != nil && h.cfg.Monitoring.LookbackWindowHours > 0 {
		lookback = h.cfg.Monitoring.LookbackWindowHours
	}

	snap, err := h.collector.Collect(r.Context(), lookback)
	if err != nil {
		WriteError(w, r, http.StatusInternalServerError, "collect_error", err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, snap)
}
