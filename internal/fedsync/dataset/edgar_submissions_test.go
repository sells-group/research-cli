package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEDGARSubmissions_Name(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, "edgar_submissions", d.Name())
}

func TestEDGARSubmissions_Table(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, "fed_data.edgar_entities", d.Table())
}

func TestEDGARSubmissions_Phase(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestEDGARSubmissions_Cadence(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.Equal(t, Weekly, d.Cadence())
}

func TestEDGARSubmissions_ShouldRun_NilLastSync(t *testing.T) {
	d := &EDGARSubmissions{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestEDGARSubmissions_ShouldRun_SameWeek(t *testing.T) {
	d := &EDGARSubmissions{}
	// Wednesday March 12
	now := time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)
	// Monday March 10 (same week)
	lastSync := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestEDGARSubmissions_ShouldRun_PreviousWeek(t *testing.T) {
	d := &EDGARSubmissions{}
	// Wednesday March 12
	now := time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)
	// Friday March 7 (previous week)
	lastSync := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestEDGARSubmissions_ShouldRun_CrossYear(t *testing.T) {
	d := &EDGARSubmissions{}
	// Thursday Jan 2
	now := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	// Monday Dec 23 (previous year, different week)
	lastSync := time.Date(2024, 12, 23, 0, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestEDGARSubmissions_ImplementsDataset(t *testing.T) {
	var _ Dataset = &EDGARSubmissions{}
}

func TestSafeIndex(t *testing.T) {
	s := []string{"a", "b", "c"}
	assert.Equal(t, "a", safeIndex(s, 0))
	assert.Equal(t, "c", safeIndex(s, 2))
	assert.Equal(t, "", safeIndex(s, 3))
	assert.Equal(t, "", safeIndex(nil, 0))
}

func TestSafeIntIndex(t *testing.T) {
	s := []int{10, 20, 30}
	assert.Equal(t, 10, safeIntIndex(s, 0))
	assert.Equal(t, 30, safeIntIndex(s, 2))
	assert.Equal(t, 0, safeIntIndex(s, 5))
	assert.Equal(t, 0, safeIntIndex(nil, 0))
}
