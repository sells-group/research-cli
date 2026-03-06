package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
)

// NotifyActivities sends webhook notifications on workflow events.
type NotifyActivities struct {
	WebhookURL string
}

// NotifyParams is the input for NotifyComplete.
type NotifyParams struct {
	Domain  string `json:"domain"` // "fedsync", "geoscraper", etc.
	Synced  int    `json:"synced"`
	Failed  int    `json:"failed"`
	Total   int    `json:"total"`
	Message string `json:"message,omitempty"`
}

// NotifyComplete sends a completion notification to the configured webhook.
func (a *NotifyActivities) NotifyComplete(ctx context.Context, params NotifyParams) error {
	if a.WebhookURL == "" {
		return nil
	}

	body, err := json.Marshal(params)
	if err != nil {
		return eris.Wrap(err, "notify: marshal payload")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return eris.Wrap(err, "notify: create request")
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return eris.Wrap(err, "notify: send webhook")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode >= 400 {
		return eris.Errorf("notify: webhook returned %d", resp.StatusCode)
	}

	return nil
}
