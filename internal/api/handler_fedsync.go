package api

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/apicache"
)

// FedsyncStatuses handles GET /fedsync/statuses.
func (h *Handlers) FedsyncStatuses(w http.ResponseWriter, r *http.Request) {
	if cached, ok := h.cache.Get(apicache.KeyFedsyncStatuses); ok {
		writeCachedJSON(w, cached)
		return
	}

	ctx := r.Context()

	if !h.requireFedsync(w, r) {
		return
	}

	statuses, err := h.readModel.Fedsync.ListDatasetStatuses(ctx)
	if err != nil {
		zap.L().Error("fedsync statuses failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get fedsync statuses")
		return
	}

	resp := map[string]any{"datasets": statuses}
	if err := h.cache.Set(apicache.KeyFedsyncStatuses, resp, 60*time.Second); err != nil {
		zap.L().Warn("cache fedsync statuses failed", zap.Error(err))
	}
	WriteJSON(w, http.StatusOK, resp)
}

// FedsyncSyncLog handles GET /fedsync/sync-log.
func (h *Handlers) FedsyncSyncLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.requireFedsync(w, r) {
		return
	}

	entries, err := h.readModel.Fedsync.ListSyncEntries(ctx)
	if err != nil {
		zap.L().Error("fedsync sync log failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to list sync log")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"entries": entries,
	})
}
