//go:build !integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildMux_HealthEndpoint(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "")

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

func TestBuildMux_WebhookEnrich_Valid_NilPipeline(t *testing.T) {
	// With a nil pipeline, the goroutine skips enrichment gracefully.
	mux, _ := buildMux(context.Background(), nil, nil, "")

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

	// Give the goroutine time to execute the nil check path.
	time.Sleep(10 * time.Millisecond)
}

func TestBuildMux_WebhookEnrich_MissingURL(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "")

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

func TestBuildMux_WebhookEnrich_InvalidJSON(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "")

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "invalid request body")
}

func TestBuildMux_WebhookEnrich_EmptyBody(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "")

	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "url is required")
}

func TestBuildMux_WebhookEnrich_URLOnly_NilPipeline(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "")

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

	time.Sleep(10 * time.Millisecond)
}

func TestBuildMux_WebhookAuth_ValidKey(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "test-secret-123")

	payload := []byte(`{"url":"https://acme.com"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-secret-123")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	time.Sleep(10 * time.Millisecond)
}

func TestBuildMux_WebhookAuth_InvalidKey(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "test-secret-123")

	payload := []byte(`{"url":"https://acme.com"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer wrong-key")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "unauthorized")
}

func TestBuildMux_WebhookAuth_MissingHeader(t *testing.T) {
	mux, _ := buildMux(context.Background(), nil, nil, "test-secret-123")

	payload := []byte(`{"url":"https://acme.com"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestBuildMux_WebhookAuth_NoSecretConfigured(t *testing.T) {
	// When no secret is configured, requests should pass through without auth.
	mux, _ := buildMux(context.Background(), nil, nil, "")

	payload := []byte(`{"url":"https://acme.com"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhook/enrich", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	time.Sleep(10 * time.Millisecond)
}
