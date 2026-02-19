package dataset

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	// Mar 19, synced on Mar 5 → same month, skip
	now := time.Date(2025, 3, 19, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 5, 0, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestADVPart1_ShouldRun_PreviousMonth(t *testing.T) {
	d := &ADVPart1{}
	// Mar 19, synced on Feb 10 → previous month, run
	now := time.Date(2025, 3, 19, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 2, 10, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestADVPart1_ShouldRun_PreviousYear(t *testing.T) {
	d := &ADVPart1{}
	now := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)
	lastSync := time.Date(2024, 12, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestADVPart1_ImplementsDataset(t *testing.T) {
	var _ Dataset = &ADVPart1{}
}

func TestADVPart1_ImplementsFullSyncer(t *testing.T) {
	var _ FullSyncer = &ADVPart1{}
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

func TestLatestFileURL(t *testing.T) {
	entries := []foiaFileEntry{
		{DisplayName: "January", FileName: "ADV_Filing_Data_20260101_20260131.zip", Year: "2026", FileType: "advFilingData", UploadedOn: "2026-02-02 14:01:51"},
		{DisplayName: "December", FileName: "ADV_Filing_Data_20251201_20251231.zip", Year: "2025", FileType: "advFilingData", UploadedOn: "2026-01-03 10:00:00"},
		{DisplayName: "November", FileName: "ADV_Filing_Data_20251101_20251130.zip", Year: "2025", FileType: "advFilingData", UploadedOn: "2025-12-02 10:00:00"},
	}

	url, err := latestFileURL(entries, "advFilingData")
	require.NoError(t, err)
	assert.Equal(t, "https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/2026/ADV_Filing_Data_20260101_20260131.zip", url)
}

func TestLatestFileURL_Empty(t *testing.T) {
	_, err := latestFileURL(nil, "advFilingData")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no advFilingData entries")
}

func TestLatestFileURL_SingleEntry(t *testing.T) {
	entries := []foiaFileEntry{
		{FileName: "ADV_Brochures_2026_January.zip", Year: "2026", FileType: "advBrochures", UploadedOn: "2026-02-01 10:00:00"},
	}

	url, err := latestFileURL(entries, "advBrochures")
	require.NoError(t, err)
	assert.Equal(t, "https://reports.adviserinfo.sec.gov/reports/foia/advBrochures/2026/ADV_Brochures_2026_January.zip", url)
}

func TestParseAUM(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"5,000,000", 5000000},
		{"5000000.00", 5000000},
		{"5,000,000.50", 5000000},
		{"", 0},
		{"abc", 0},
		{"100", 100},
	}

	for _, tt := range tests {
		result := parseAUM(tt.input)
		assert.Equal(t, tt.expected, result, "input: %q", tt.input)
	}
}

func TestParseEmployeeRange(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"", 0},
		{"0", 0},
		{"1-10", 5},
		{"11-25", 18},
		{"26-50", 38},
		{"51-100", 75},
		{"101-250", 175},
		{"251-500", 375},
		{"More than 500", 750},
		{"42", 42},
	}

	for _, tt := range tests {
		result := parseEmployeeRange(tt.input)
		assert.Equal(t, tt.expected, result, "input: %q", tt.input)
	}
}

func TestParseOwnershipCode(t *testing.T) {
	tests := []struct {
		code     string
		expected *float64
	}{
		{"A", ptrFloat(12.5)},
		{"B", ptrFloat(37.5)},
		{"C", ptrFloat(62.5)},
		{"D", ptrFloat(87.5)},
		{"E", ptrFloat(25.0)},
		{"NA", nil},
		{"", nil},
	}

	for _, tt := range tests {
		result := parseOwnershipCode(tt.code)
		if tt.expected == nil {
			assert.Nil(t, result, "code: %q", tt.code)
		} else {
			require.NotNil(t, result, "code: %q", tt.code)
			assert.Equal(t, *tt.expected, *result, "code: %q", tt.code)
		}
	}
}

func ptrFloat(v float64) *float64 { return &v }

func TestParseBoolYN(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"Y", true},
		{"y", true},
		{" Y ", true},
		{"N", false},
		{"n", false},
		{"", false},
		{"Yes", false},
		{"1", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, parseBoolYN(tt.input), "input: %q", tt.input)
	}
}

func TestBuildClientTypesJSON_NonZero(t *testing.T) {
	header := []string{"5D(1)(a)", "5D(2)(a)", "5D(3)(a)", "5D(1)(b)", "5D(2)(b)", "5D(3)(b)"}
	colIdx := mapColumnsNormalized(header)
	record := []string{"50", "25.0", "1000000", "10", "75.0", "3000000"}

	result := buildClientTypesJSON(record, colIdx)
	require.NotNil(t, result)

	var entries []clientTypeEntry
	require.NoError(t, json.Unmarshal(result, &entries))
	assert.Len(t, entries, 2)
	assert.Equal(t, "Individuals (other than high net worth)", entries[0].Type)
	assert.Equal(t, 50, entries[0].Count)
	assert.Equal(t, int64(1000000), entries[0].RAUM)
	assert.Equal(t, "High net worth individuals", entries[1].Type)
}

func TestBuildClientTypesJSON_AllZero(t *testing.T) {
	header := []string{"other_col"}
	colIdx := mapColumnsNormalized(header)
	record := []string{"foo"}

	result := buildClientTypesJSON(record, colIdx)
	assert.Nil(t, result)
}

