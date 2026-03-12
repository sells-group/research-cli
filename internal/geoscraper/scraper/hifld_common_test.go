package scraper

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHifldString(t *testing.T) {
	attrs := map[string]any{
		"NAME":   "Test Plant",
		"EMPTY":  nil,
		"NUMBER": 42.0,
	}

	assert.Equal(t, "Test Plant", hifldString(attrs, "NAME"))
	assert.Equal(t, "", hifldString(attrs, "EMPTY"))
	assert.Equal(t, "", hifldString(attrs, "MISSING"))
	assert.Equal(t, "", hifldString(attrs, "NUMBER"))
}

func TestHifldFloat64(t *testing.T) {
	attrs := map[string]any{
		"CAPACITY": 500.5,
		"EMPTY":    nil,
		"TEXT":     "not a number",
	}

	assert.InDelta(t, 500.5, hifldFloat64(attrs, "CAPACITY"), 0.001)
	assert.Equal(t, 0.0, hifldFloat64(attrs, "EMPTY"))
	assert.Equal(t, 0.0, hifldFloat64(attrs, "MISSING"))
	assert.Equal(t, 0.0, hifldFloat64(attrs, "TEXT"))
}

func TestHifldFloat64_JSONNumber(t *testing.T) {
	attrs := map[string]any{
		"VALUE": json.Number("123.45"),
	}
	assert.InDelta(t, 123.45, hifldFloat64(attrs, "VALUE"), 0.001)
}

func TestHifldProperties(t *testing.T) {
	attrs := map[string]any{
		"OBJECTID": 1.0,
		"NAME":     "Test",
		"STATE":    "TX",
		"EXTRA":    "value",
		"NIL_VAL":  nil,
	}
	exclude := map[string]bool{"OBJECTID": true, "NAME": true}

	data := hifldProperties(attrs, exclude)
	var result map[string]any
	err := json.Unmarshal(data, &result)
	assert.NoError(t, err)

	assert.Equal(t, "TX", result["STATE"])
	assert.Equal(t, "value", result["EXTRA"])
	assert.NotContains(t, result, "OBJECTID")
	assert.NotContains(t, result, "NAME")
	assert.NotContains(t, result, "NIL_VAL")
}

func TestHifldShouldRun_NeverSynced(t *testing.T) {
	assert.True(t, hifldShouldRun(fixedNow(), nil))
}

func TestHifldURL_Override(t *testing.T) {
	got := hifldURL("http://test.local/query", schoolsBaseURL)
	assert.Equal(t, "http://test.local/query", got)
}

func TestHifldURL_Default(t *testing.T) {
	got := hifldURL("", schoolsBaseURL)
	assert.Equal(t, schoolsBaseURL, got)
}

func TestHifldProperties_Empty(t *testing.T) {
	data := hifldProperties(map[string]any{}, nil)
	assert.Equal(t, "{}", string(data))
}

func TestHifldProperties_NilMap(t *testing.T) {
	data := hifldProperties(nil, nil)
	assert.Equal(t, "{}", string(data))
}

func TestHifldProperties_AllExcluded(t *testing.T) {
	attrs := map[string]any{
		"NAME":     "Test",
		"OBJECTID": 1.0,
	}
	exclude := map[string]bool{"NAME": true, "OBJECTID": true}
	data := hifldProperties(attrs, exclude)
	assert.Equal(t, "{}", string(data))
}

func TestHifldProperties_MixedTypes(t *testing.T) {
	// Verifies that non-nil values of various types are included.
	attrs := map[string]any{
		"FLOAT_VAL": 42.5,
		"INT_VAL":   json.Number("99"),
		"BOOL_VAL":  true,
		"NIL_VAL":   nil,
	}
	data := hifldProperties(attrs, nil)
	assert.Contains(t, string(data), `"FLOAT_VAL":42.5`)
	assert.Contains(t, string(data), `"BOOL_VAL":true`)
	assert.NotContains(t, string(data), "NIL_VAL")
}

func TestHifldAnnualShouldRun(t *testing.T) {
	now := fixedNow()
	assert.True(t, hifldAnnualShouldRun(now, nil))

	recent := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, hifldAnnualShouldRun(now, &recent))
}
