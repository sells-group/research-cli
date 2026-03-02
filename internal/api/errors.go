package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
)

// ErrorResponse is the standard JSON error body returned by all endpoints.
type ErrorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// WriteError writes a JSON error response with the given status code.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	resp := ErrorResponse{
		Error:     message,
		Code:      code,
		RequestID: middleware.GetReqID(r.Context()),
	}
	WriteJSON(w, status, resp)
}

// WriteJSON writes v as JSON with the given HTTP status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
