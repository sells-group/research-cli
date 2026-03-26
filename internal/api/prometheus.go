package api

import (
	"net/http"

	"github.com/sells-group/research-cli/internal/opsmetrics"
)

// PrometheusMetrics exposes low-cardinality operational metrics.
func (h *Handlers) PrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	opsmetrics.Handler(w, r)
}
