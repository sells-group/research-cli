//go:build !integration

package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/model"
)

func TestWriteRunResult_BasicOutput(t *testing.T) {
	var buf bytes.Buffer

	company := model.Company{
		URL:  "https://acme.com",
		Name: "Acme Corp",
	}
	result := &model.EnrichmentResult{
		Score:       0.85,
		TotalTokens: 1500,
		Answers: []model.ExtractionAnswer{
			{FieldKey: "revenue", Value: "10M", Confidence: 0.9},
			{FieldKey: "employees", Value: "100", Confidence: 0.8},
		},
	}

	err := writeRunResult(&buf, company, result)
	require.NoError(t, err)

	// Verify it's valid JSON.
	var decoded model.EnrichmentResult
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, 0.85, decoded.Score)
	assert.Equal(t, 1500, decoded.TotalTokens)
	assert.Len(t, decoded.Answers, 2)
}

func TestWriteRunResult_EmptyResult(t *testing.T) {
	var buf bytes.Buffer

	company := model.Company{URL: "https://empty.com"}
	result := &model.EnrichmentResult{}

	err := writeRunResult(&buf, company, result)
	require.NoError(t, err)

	var decoded model.EnrichmentResult
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, 0.0, decoded.Score)
}

func TestWriteRunResult_PrettyPrinted(t *testing.T) {
	var buf bytes.Buffer

	company := model.Company{URL: "https://pretty.com"}
	result := &model.EnrichmentResult{Score: 0.95}

	err := writeRunResult(&buf, company, result)
	require.NoError(t, err)

	// Should be indented.
	assert.Contains(t, buf.String(), "  ")
}
