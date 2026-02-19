package waterfall

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
waterfall:
  defaults:
    confidence_threshold: 0.7
    time_decay:
      half_life_days: 365
      floor: 0.2
      curve: exponential
    max_premium_cost_usd: 2.00
  fields:
    legal_name:
      confidence_threshold: 0.85
      time_decay: { half_life_days: 1825, floor: 0.3 }
      sources:
        - { name: website_crawl, tier: 0 }
        - { name: clearbit, tier: 2 }
    employee_count:
      sources:
        - { name: website_crawl, tier: 0 }
`
	dir := t.TempDir()
	path := filepath.Join(dir, "waterfall.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	// Check defaults.
	assert.Equal(t, 0.7, cfg.Defaults.ConfidenceThreshold)
	assert.Equal(t, 365, cfg.Defaults.TimeDecay.HalfLifeDays)
	assert.Equal(t, 0.2, cfg.Defaults.TimeDecay.Floor)
	assert.Equal(t, 2.0, cfg.Defaults.MaxPremiumCostUSD)

	// Check legal_name has its own threshold.
	ln := cfg.Fields["legal_name"]
	assert.Equal(t, 0.85, ln.ConfidenceThreshold)
	assert.Equal(t, 1825, ln.TimeDecay.HalfLifeDays)
	assert.Equal(t, 2, len(ln.Sources))
	assert.Equal(t, "clearbit", ln.Sources[1].Name)
	assert.Equal(t, 2, ln.Sources[1].Tier)

	// employee_count should have inherited defaults.
	ec := cfg.Fields["employee_count"]
	assert.Equal(t, 0.7, ec.ConfidenceThreshold) // inherited
	assert.Equal(t, 365, ec.TimeDecay.HalfLifeDays) // inherited
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path.yaml")
	assert.Error(t, err)
}

func TestGetFieldConfig_Known(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay: DecayConfig{HalfLifeDays: 365, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{
			"phone": {
				ConfidenceThreshold: 0.8,
				TimeDecay: &DecayConfig{HalfLifeDays: 180, Floor: 0.15},
			},
		},
	}

	fc := cfg.GetFieldConfig("phone")
	assert.Equal(t, 0.8, fc.ConfidenceThreshold)
	assert.Equal(t, 180, fc.TimeDecay.HalfLifeDays)
}

func TestValidate_Valid(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
			MaxPremiumCostUSD:   2.0,
		},
		Fields: map[string]FieldConfig{
			"phone": {
				ConfidenceThreshold: 0.8,
				TimeDecay:           &DecayConfig{HalfLifeDays: 180, Floor: 0.15},
			},
		},
	}
	assert.NoError(t, cfg.Validate())
}

func TestValidate_ThresholdOutOfRange(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 1.5,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{},
	}
	assert.ErrorContains(t, cfg.Validate(), "confidence_threshold")
}

func TestValidate_NegativeThreshold(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: -0.1,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{},
	}
	assert.ErrorContains(t, cfg.Validate(), "confidence_threshold")
}

func TestValidate_HalfLifeZero(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 0, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{},
	}
	assert.ErrorContains(t, cfg.Validate(), "half_life_days")
}

func TestValidate_FloorOutOfRange(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 1.5},
		},
		Fields: map[string]FieldConfig{},
	}
	assert.ErrorContains(t, cfg.Validate(), "floor")
}

func TestValidate_NegativePremiumCost(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
			MaxPremiumCostUSD:   -1.0,
		},
		Fields: map[string]FieldConfig{},
	}
	assert.ErrorContains(t, cfg.Validate(), "max_premium_cost_usd")
}

func TestValidate_FieldThresholdOutOfRange(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{
			"bad_field": {ConfidenceThreshold: 2.0},
		},
	}
	assert.ErrorContains(t, cfg.Validate(), "bad_field")
}

func TestValidate_FieldDecayInvalid(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay:           DecayConfig{HalfLifeDays: 365, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{
			"phone": {
				ConfidenceThreshold: 0.8,
				TimeDecay:           &DecayConfig{HalfLifeDays: -10, Floor: 0.1},
			},
		},
	}
	assert.ErrorContains(t, cfg.Validate(), "phone")
}

func TestLoadConfig_InvalidValues(t *testing.T) {
	yaml := `
waterfall:
  defaults:
    confidence_threshold: 1.5
    time_decay:
      half_life_days: 365
      floor: 0.2
    max_premium_cost_usd: 2.00
  fields: {}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "waterfall.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0644))

	_, err := LoadConfig(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "confidence_threshold")
}

func TestGetFieldConfig_Unknown(t *testing.T) {
	cfg := &Config{
		Defaults: DefaultConfig{
			ConfidenceThreshold: 0.7,
			TimeDecay: DecayConfig{HalfLifeDays: 365, Floor: 0.2},
		},
		Fields: map[string]FieldConfig{},
	}

	fc := cfg.GetFieldConfig("unknown_field")
	assert.Equal(t, 0.7, fc.ConfidenceThreshold)
	assert.Equal(t, 365, fc.TimeDecay.HalfLifeDays)
}
