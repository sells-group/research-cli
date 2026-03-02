package api

import "net/http"

// Health handles GET /health and GET /api/v1/health.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	if h.store != nil {
		if err := h.store.Ping(r.Context()); err != nil {
			WriteError(w, r, http.StatusServiceUnavailable, "store_unhealthy", err.Error())
			return
		}
	}
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
