package dataset

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	fetchermocks "github.com/sells-group/research-cli/internal/fetcher/mocks"
)

func TestSECEnforcement_Metadata(t *testing.T) {
	d := &SECEnforcement{}

	assert.Equal(t, "sec_enforcement", d.Name())
	assert.Equal(t, "fed_data.sec_enforcement_actions", d.Table())
	assert.Equal(t, Phase2, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestSECEnforcement_ShouldRun(t *testing.T) {
	d := &SECEnforcement{}

	t.Run("nil lastSync", func(t *testing.T) {
		assert.True(t, d.ShouldRun(time.Now(), nil))
	})

	t.Run("synced this month", func(t *testing.T) {
		now := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC)
		assert.False(t, d.ShouldRun(now, &last))
	})

	t.Run("synced last month", func(t *testing.T) {
		now := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
		last := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)
		assert.True(t, d.ShouldRun(now, &last))
	})
}

func TestSECEnforcement_Sync_EmptyResponse(t *testing.T) {
	resp := map[string]any{
		"hits": map[string]any{
			"hits":  []any{},
			"total": map[string]any{"value": 0},
		},
	}

	mf := fetchermocks.NewMockFetcher(t)
	mf.On("Download", mock.Anything, mock.AnythingOfType("string")).
		Return(jsonBody(t, resp), nil).Once()

	d := &SECEnforcement{}
	result, err := d.Sync(context.Background(), nil, mf, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
}

func TestSECEnforcement_Sync_ParsesResults(t *testing.T) {
	// Build a mock EFTS response with one hit.
	resp := map[string]any{
		"hits": map[string]any{
			"hits": []any{
				map[string]any{
					"_id": "LR-12345",
					"_source": map[string]any{
						"display_names":       []string{"Test Advisor LLC"},
						"file_date":           "2026-01-15",
						"form_type":           "LIT_REL",
						"display_description": "SEC charges advisory firm with fraud",
						"file_num":            "3-12345",
					},
				},
			},
			"total": map[string]any{"value": 1},
		},
	}

	// Validate mock response is parseable into the struct.
	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var result enforcementSearchResult
	err = json.NewDecoder(io.NopCloser(strings.NewReader(string(data)))).Decode(&result)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Hits.Total.Value)
	assert.Equal(t, "LR-12345", result.Hits.Hits[0].ID)
	assert.Equal(t, "Test Advisor LLC", result.Hits.Hits[0].Source.DisplayNames[0])
	assert.Equal(t, "2026-01-15", result.Hits.Hits[0].Source.FileDate)
	assert.Equal(t, "LIT_REL", result.Hits.Hits[0].Source.FormType)
	assert.Equal(t, "SEC charges advisory firm with fraud", result.Hits.Hits[0].Source.DisplayDesc)
	assert.Equal(t, "3-12345", result.Hits.Hits[0].Source.FileNum)
}
