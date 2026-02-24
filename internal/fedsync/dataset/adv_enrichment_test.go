package dataset

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/pkg/anthropic"
	anthropicmocks "github.com/sells-group/research-cli/pkg/anthropic/mocks"
)

func TestADVEnrichment_Metadata(t *testing.T) {
	d := &ADVEnrichment{}
	assert.Equal(t, "adv_enrichment", d.Name())
	assert.Equal(t, "fed_data.adv_brochure_enrichment", d.Table())
	assert.Equal(t, Phase3, d.Phase())
	assert.Equal(t, Monthly, d.Cadence())
}

func TestADVEnrichment_ShouldRun(t *testing.T) {
	d := &ADVEnrichment{}
	assert.True(t, d.ShouldRun(time.Now(), nil))

	now := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestADVEnrichment_ImplementsDataset(t *testing.T) {
	t.Parallel()
	var _ Dataset = &ADVEnrichment{}
}

func TestADVEnrichment_Sync_NoKey(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	ds := &ADVEnrichment{cfg: &config.Config{}}
	_, err = ds.Sync(context.Background(), pool, nil, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "anthropic API key is required")
}

func TestADVEnrichment_Sync_NoPending(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	mockClient := anthropicmocks.NewMockClient(t)

	// Brochure query returns empty
	pool.ExpectQuery("SELECT b.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "brochure_id", "text_content"}))

	// CRS query returns empty
	pool.ExpectQuery("SELECT c.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "crs_id", "text_content"}))

	ds := &ADVEnrichment{client: mockClient}
	result, err := ds.Sync(context.Background(), pool, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestADVEnrichment_EnrichBrochure_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	mockClient := anthropicmocks.NewMockClient(t)

	// Brochure query returns one row
	pool.ExpectQuery("SELECT b.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "brochure_id", "text_content"}).
			AddRow(12345, "BR001", "Sample brochure text about equity growth investing"))

	// Mock Haiku response for brochure
	mockClient.EXPECT().CreateMessage(mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		return req.Model == enrichHaikuModel
	})).Return(&anthropic.MessageResponse{
		Model: enrichHaikuModel,
		Content: []anthropic.ContentBlock{{
			Type: "text",
			Text: `{"investment_strategies":["equity growth"],"industry_specializations":["technology"],"min_account_size":500000,"fee_schedule":"1% AUM","target_clients":"High net worth individuals"}`,
		}},
		Usage: anthropic.TokenUsage{InputTokens: 100, OutputTokens: 50},
	}, nil).Once()

	// Expect brochure enrichment upsert
	brochureCols := []string{"crd_number", "brochure_id", "investment_strategies", "industry_specializations",
		"min_account_size", "fee_schedule", "target_clients", "model", "input_tokens", "output_tokens", "enriched_at"}
	expectBulkUpsert(pool, "fed_data.adv_brochure_enrichment", brochureCols, 1)

	// CRS query returns empty
	pool.ExpectQuery("SELECT c.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "crs_id", "text_content"}))

	ds := &ADVEnrichment{client: mockClient}
	result, err := ds.Sync(context.Background(), pool, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.Equal(t, int64(1), result.Metadata["brochures_enriched"])
	assert.Equal(t, int64(0), result.Metadata["crs_enriched"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestADVEnrichment_EnrichBrochure_HaikuError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	mockClient := anthropicmocks.NewMockClient(t)

	// Brochure query returns one row
	pool.ExpectQuery("SELECT b.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "brochure_id", "text_content"}).
			AddRow(12345, "BR001", "Sample brochure text"))

	// Haiku returns error → should skip gracefully
	mockClient.EXPECT().CreateMessage(mock.Anything, mock.Anything).
		Return(nil, errors.New("rate limited")).Once()

	// CRS query returns empty
	pool.ExpectQuery("SELECT c.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "crs_id", "text_content"}))

	ds := &ADVEnrichment{client: mockClient}
	result, err := ds.Sync(context.Background(), pool, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsSynced)
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestADVEnrichment_EnrichCRS_Success(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	mockClient := anthropicmocks.NewMockClient(t)

	// Brochure query returns empty
	pool.ExpectQuery("SELECT b.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "brochure_id", "text_content"}))

	// CRS query returns one row
	pool.ExpectQuery("SELECT c.crd_number").
		WithArgs(500).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "crs_id", "text_content"}).
			AddRow(99999, "CRS001", "This is our Form CRS"))

	// Mock Haiku response for CRS
	mockClient.EXPECT().CreateMessage(mock.Anything, mock.MatchedBy(func(req anthropic.MessageRequest) bool {
		return req.Model == enrichHaikuModel
	})).Return(&anthropic.MessageResponse{
		Model: enrichHaikuModel,
		Content: []anthropic.ContentBlock{{
			Type: "text",
			Text: `{"firm_type":"investment adviser","key_services":"Portfolio management","fee_types":["asset-based"],"has_disciplinary_history":false,"conflicts_of_interest":"None disclosed"}`,
		}},
		Usage: anthropic.TokenUsage{InputTokens: 80, OutputTokens: 40},
	}, nil).Once()

	// Expect CRS enrichment upsert
	crsCols := []string{"crd_number", "crs_id", "firm_type", "key_services", "fee_types",
		"has_disciplinary_history", "conflicts_of_interest", "model", "input_tokens", "output_tokens", "enriched_at"}
	expectBulkUpsert(pool, "fed_data.adv_crs_enrichment", crsCols, 1)

	ds := &ADVEnrichment{client: mockClient}
	result, err := ds.Sync(context.Background(), pool, nil, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced)
	assert.Equal(t, int64(0), result.Metadata["brochures_enriched"])
	assert.Equal(t, int64(1), result.Metadata["crs_enriched"])
	assert.NoError(t, pool.ExpectationsWereMet())
}

func TestCleanJSONFromText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "bare JSON",
			input:    `{"key": "value"}`,
			expected: `{"key": "value"}`,
		},
		{
			name:     "markdown json fence",
			input:    "```json\n{\"key\": \"value\"}\n```",
			expected: `{"key": "value"}`,
		},
		{
			name:     "markdown plain fence",
			input:    "```\n{\"key\": \"value\"}\n```",
			expected: `{"key": "value"}`,
		},
		{
			name:     "text before JSON",
			input:    "Here is the result:\n{\"key\": \"value\"}",
			expected: `{"key": "value"}`,
		},
		{
			name:     "no JSON",
			input:    "no json here",
			expected: "no json here",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanJSONFromText(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractResponseText(t *testing.T) {
	resp := &anthropic.MessageResponse{
		Content: []anthropic.ContentBlock{
			{Type: "text", Text: "hello "},
			{Type: "text", Text: "world"},
		},
	}
	assert.Equal(t, "hello world", extractResponseText(resp))
	assert.Equal(t, "", extractResponseText(nil))
}

// Verify pgxmock Query works with our enrichment query patterns.
func TestADVEnrichment_BrochureQueryError(t *testing.T) {
	pool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer pool.Close()

	mockClient := anthropicmocks.NewMockClient(t)

	pool.ExpectQuery("SELECT b.crd_number").
		WithArgs(500).
		WillReturnError(pgx.ErrNoRows)

	ds := &ADVEnrichment{client: mockClient}
	_, err = ds.Sync(context.Background(), pool, nil, t.TempDir())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query pending brochures")
}
