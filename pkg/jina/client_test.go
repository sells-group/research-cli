package jina

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRead_Success(t *testing.T) {
	t.Parallel()

	want := ReadResponse{
		Code: 200,
		Data: ReadData{
			Title:   "Acme Corp",
			URL:     "https://acme.com",
			Content: "# Acme Corp\n\nWe build things.",
			Usage:   ReadUsage{Tokens: 2150},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		assert.Equal(t, "markdown", r.Header.Get("X-Return-Format"))
		assert.Equal(t, "/https://acme.com", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	got, err := client.Read(context.Background(), "https://acme.com")

	require.NoError(t, err)
	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Data.Title, got.Data.Title)
	assert.Equal(t, want.Data.Content, got.Data.Content)
	assert.Equal(t, want.Data.Usage.Tokens, got.Data.Usage.Tokens)
}

func TestRead_HTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"rate limit exceeded"}`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
}

func TestRead_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`internal error`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestRead_MalformedJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{not json`))
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(context.Background(), "https://acme.com")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestRead_ContextCancellation(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This handler should not be reached because context is cancelled
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	client := NewClient("test-key", WithBaseURL(srv.URL))
	_, err := client.Read(ctx, "https://acme.com")

	require.Error(t, err)
}

func TestRead_EmptyContent(t *testing.T) {
	t.Parallel()

	want := ReadResponse{
		Code: 200,
		Data: ReadData{
			Title:   "",
			URL:     "https://blocked.com",
			Content: "",
			Usage:   ReadUsage{Tokens: 0},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	client := NewClient("test-key", WithBaseURL(srv.URL))
	got, err := client.Read(context.Background(), "https://blocked.com")

	require.NoError(t, err)
	assert.Empty(t, got.Data.Content)
}
