package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/model"
)

func TestSendToToolJet_ErrorStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}

	err := sendToToolJet(context.Background(), result, ts.URL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tooljet returned status 500")
}

func TestSendToToolJet_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}

	err := sendToToolJet(context.Background(), result, ts.URL)
	assert.NoError(t, err)
}

func TestSendToToolJet_ConnectionError(t *testing.T) {
	result := &model.EnrichmentResult{
		Company: model.Company{Name: "Test"},
	}

	err := sendToToolJet(context.Background(), result, "http://localhost:1/bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tooljet request failed")
}

func TestBuildSFFields_Empty(t *testing.T) {
	fields := buildSFFields(nil)
	assert.Empty(t, fields)
}

func TestBuildSFFields_AllEmpty(t *testing.T) {
	fieldValues := map[string]model.FieldValue{
		"a": {SFField: "", Value: "ignored"},
		"b": {SFField: "", Value: "also ignored"},
	}

	fields := buildSFFields(fieldValues)
	assert.Empty(t, fields)
}

// TestSendToToolJet_Timeout verifies that the webhook call to ToolJet respects
// the webhookClient timeout and returns an error when the server is too slow.
func TestSendToToolJet_Timeout(t *testing.T) {
	// Save original client and restore after test.
	origClient := webhookClient
	t.Cleanup(func() { webhookClient = origClient })

	// Use a very short timeout for the test.
	webhookClient = &http.Client{Timeout: 100 * time.Millisecond}

	// Server that blocks longer than the client timeout. Use a done channel so
	// the handler exits promptly when the test finishes, avoiding slow Close().
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-done
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	defer close(done) // unblock handler before ts.Close() waits for connections

	result := &model.EnrichmentResult{
		Company: model.Company{
			Name: "Slow Webhook Corp",
		},
		FieldValues: map[string]model.FieldValue{},
	}

	start := time.Now()
	err := sendToToolJet(context.Background(), result, ts.URL)
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tooljet request failed")
	// Should have returned well before 2s, respecting the 100ms timeout.
	assert.Less(t, elapsed, 2*time.Second, "should timeout quickly, not wait for server")
}

// TestWebhookClient_HasTimeout verifies that the package-level webhookClient
// is configured with the expected 10-second timeout.
func TestWebhookClient_HasTimeout(t *testing.T) {
	assert.Equal(t, 10*time.Second, webhookClient.Timeout)
}
