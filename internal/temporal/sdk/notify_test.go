package sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotifyComplete_EmptyURL(t *testing.T) {
	a := &NotifyActivities{WebhookURL: ""}
	err := a.NotifyComplete(context.Background(), NotifyParams{
		Domain: "test",
		Synced: 5,
	})
	require.NoError(t, err)
}

func TestNotifyComplete_Success(t *testing.T) {
	var received NotifyParams
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		err := json.NewDecoder(r.Body).Decode(&received)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := &NotifyActivities{WebhookURL: srv.URL}
	err := a.NotifyComplete(context.Background(), NotifyParams{
		Domain:  "fedsync",
		Synced:  10,
		Failed:  2,
		Total:   12,
		Message: "done",
	})
	require.NoError(t, err)
	require.Equal(t, "fedsync", received.Domain)
	require.Equal(t, 10, received.Synced)
	require.Equal(t, 2, received.Failed)
	require.Equal(t, 12, received.Total)
}

func TestNotifyComplete_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	a := &NotifyActivities{WebhookURL: srv.URL}
	err := a.NotifyComplete(context.Background(), NotifyParams{Domain: "test"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestNotifyComplete_ConnectionError(t *testing.T) {
	a := &NotifyActivities{WebhookURL: "http://localhost:1"} // nothing listening
	err := a.NotifyComplete(context.Background(), NotifyParams{Domain: "test"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "notify: send webhook")
}
