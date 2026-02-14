package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sells-group/research-cli/internal/config"
)

func TestIACompilation_Name(t *testing.T) {
	d := &IACompilation{}
	assert.Equal(t, "ia_compilation", d.Name())
}

func TestIACompilation_Table(t *testing.T) {
	d := &IACompilation{}
	assert.Equal(t, "fed_data.adv_firms", d.Table())
}

func TestIACompilation_Phase(t *testing.T) {
	d := &IACompilation{}
	assert.Equal(t, Phase1B, d.Phase())
}

func TestIACompilation_Cadence(t *testing.T) {
	d := &IACompilation{}
	assert.Equal(t, Daily, d.Cadence())
}

func TestIACompilation_ShouldRun_NilLastSync(t *testing.T) {
	d := &IACompilation{}
	assert.True(t, d.ShouldRun(time.Now(), nil))
}

func TestIACompilation_ShouldRun_Today(t *testing.T) {
	d := &IACompilation{}
	now := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 15, 6, 0, 0, 0, time.UTC)
	assert.False(t, d.ShouldRun(now, &lastSync))
}

func TestIACompilation_ShouldRun_Yesterday(t *testing.T) {
	d := &IACompilation{}
	now := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC)
	lastSync := time.Date(2025, 3, 14, 22, 0, 0, 0, time.UTC)
	assert.True(t, d.ShouldRun(now, &lastSync))
}

func TestIACompilation_ImplementsDataset(t *testing.T) {
	var _ Dataset = &IACompilation{}
}

func TestIACompilation_UserAgent_Default(t *testing.T) {
	d := &IACompilation{}
	ua := d.userAgent()
	assert.Contains(t, ua, "research-cli")
}

func TestIACompilation_UserAgent_FromConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.Fedsync.EDGARUserAgent = "TestAgent test@example.com"
	d := &IACompilation{cfg: cfg}
	assert.Equal(t, "TestAgent test@example.com", d.userAgent())
}
