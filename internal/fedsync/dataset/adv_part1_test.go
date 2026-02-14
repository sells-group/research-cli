package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestADVPart1_Name(t *testing.T) {
	d := &ADVPart1{}
	assert.Equal(t, "adv_part1", d.Name())
}

func TestADVPart1_Table(t *testing.T) {
	d := &ADVPart1{}
	assert.Equal(t, "fed_data.adv_firms", d.Table())
}

func TestADVPart1_Phase(t *testing.T) {
	d := &ADVPart1{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestADVPart1_Cadence(t *testing.T) {
	d := &ADVPart1{}
	assert.Equal(t, Monthly, d.Cadence())
}

func TestADVPart1_ShouldRun_NilLastSync(t *testing.T) {
	d := &ADVPart1{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestADVPart1_ShouldRun_SameMonth(t *testing.T) {
	d := &ADVPart1{}
	now := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestADVPart1_ShouldRun_PreviousMonth(t *testing.T) {
	d := &ADVPart1{}
	now := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 2, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestADVPart1_ShouldRun_PreviousYear(t *testing.T) {
	d := &ADVPart1{}
	now := time.Date(2025, 1, 5, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2024, 12, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestADVPart1_ImplementsDataset(t *testing.T) {
	var _ Dataset = &ADVPart1{}
}

func TestParseDate_Formats(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"2025-03-15", "2025-03-15"},
		{"03/15/2025", "2025-03-15"},
		{"3/5/2025", "2025-03-05"},
		{"03-15-2025", "2025-03-15"},
		{"", ""},
		{"invalid", ""},
	}

	for _, tt := range tests {
		result := parseDate(tt.input)
		if tt.expected == "" {
			assert.Nil(t, result, "input: %q", tt.input)
		} else {
			assert.NotNil(t, result, "input: %q", tt.input)
			assert.Equal(t, tt.expected, result.Format("2006-01-02"), "input: %q", tt.input)
		}
	}
}
