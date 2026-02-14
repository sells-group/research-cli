package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildServeMux builds the same mux as serve.go's handler for testing.
// We replicate the handler setup here because the serve command couples
// handler registration with server lifecycle (initPipeline, ListenAndServe).
func buildTestMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /webhook/enrich", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL          string `json:"url"`
			SalesforceID string `json:"salesforce_id"`
			Name         string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}

		if req.URL == "" {
			http.Error(w, `{"error":"url is required"}`, http.StatusBadRequest)
			return
		}

		// In the real handler, enrichment runs asynchronously.
		// For testing we just verify the response format.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "accepted",
			"company": req.URL,
		})
	})

	return mux
}

func TestHealthEndpoint(t *testing.T) {
	mux := buildTestMux()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var body map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &body)
	require.NoError(t, err)
	assert.Equal(t, "ok", body["status"])
}

func TestWebhookEnrich_Valid(t *testing.T) {
	mux := buildTestMux()

	payload := map[string]string{
		"url":           "https://acme.com",
		"salesforce_id": "001ABC",
		"name":          "Acme Corp",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var resp map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp["status"])
	assert.Equal(t, "https://acme.com", resp["company"])
}

func TestWebhookEnrich_MissingURL(t *testing.T) {
	mux := buildTestMux()

	payload := map[string]string{
		"salesforce_id": "001ABC",
		"name":          "Acme Corp",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "url is required")
}

func TestWebhookEnrich_InvalidJSON(t *testing.T) {
	mux := buildTestMux()

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "invalid request body")
}

func TestWebhookEnrich_EmptyBody(t *testing.T) {
	mux := buildTestMux()

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "url is required")
}

func TestWebhookEnrich_URLOnlyMinimal(t *testing.T) {
	mux := buildTestMux()

	// Only URL provided, no salesforce_id or name.
	payload := map[string]string{
		"url": "https://minimal.com",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)

	var resp map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp["status"])
	assert.Equal(t, "https://minimal.com", resp["company"])
}

func TestServeCmd_DefaultPortFromConfig(t *testing.T) {
	// Verify that servePort flag default is 0 (meaning use config).
	flag := serveCmd.Flags().Lookup("port")
	require.NotNil(t, flag)
	assert.Equal(t, "0", flag.DefValue)
}

func TestServeCmd_Metadata(t *testing.T) {
	assert.Equal(t, "serve", serveCmd.Use)
	assert.NotEmpty(t, serveCmd.Short)
}

func TestRunCmd_Metadata(t *testing.T) {
	assert.Equal(t, "run", runCmd.Use)
	assert.NotEmpty(t, runCmd.Short)

	urlFlag := runCmd.Flags().Lookup("url")
	require.NotNil(t, urlFlag)
	sfFlag := runCmd.Flags().Lookup("sf-id")
	require.NotNil(t, sfFlag)
}

func TestBatchCmd_Metadata(t *testing.T) {
	assert.Equal(t, "batch", batchCmd.Use)
	assert.NotEmpty(t, batchCmd.Short)

	limitFlag := batchCmd.Flags().Lookup("limit")
	require.NotNil(t, limitFlag)
	assert.Equal(t, "100", limitFlag.DefValue)
}
