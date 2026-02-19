package waterfall

import (
	"os"

	"github.com/rotisserie/eris"
	"gopkg.in/yaml.v3"
)

// Config is the top-level waterfall configuration.
type Config struct {
	Defaults DefaultConfig          `yaml:"defaults"`
	Fields   map[string]FieldConfig `yaml:"fields"`
}

// DefaultConfig holds global defaults.
type DefaultConfig struct {
	ConfidenceThreshold float64     `yaml:"confidence_threshold"`
	TimeDecay           DecayConfig `yaml:"time_decay"`
	MaxPremiumCostUSD   float64     `yaml:"max_premium_cost_usd"`
}

// DecayConfig holds time decay parameters.
type DecayConfig struct {
	HalfLifeDays int     `yaml:"half_life_days"`
	Floor        float64 `yaml:"floor"`
	Curve        string  `yaml:"curve"` // "exponential" only for now
}

// FieldConfig configures waterfall behavior for a specific field.
type FieldConfig struct {
	ConfidenceThreshold float64        `yaml:"confidence_threshold"`
	TimeDecay           *DecayConfig   `yaml:"time_decay,omitempty"`
	Sources             []SourceConfig `yaml:"sources"`
}

// SourceConfig defines a source in a field's waterfall chain.
type SourceConfig struct {
	Name string `yaml:"name"`
	Tier int    `yaml:"tier"` // 0 = free, 2 = premium
}

// LoadConfig reads waterfall config from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, eris.Wrapf(err, "waterfall: read config %s", path)
	}

	// The YAML has a top-level "waterfall" key
	var wrapper struct {
		Waterfall Config `yaml:"waterfall"`
	}
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, eris.Wrap(err, "waterfall: parse config")
	}

	cfg := &wrapper.Waterfall
	// Apply defaults to fields missing threshold/decay
	for key, fc := range cfg.Fields {
		if fc.ConfidenceThreshold == 0 {
			fc.ConfidenceThreshold = cfg.Defaults.ConfidenceThreshold
		}
		if fc.TimeDecay == nil {
			fc.TimeDecay = &cfg.Defaults.TimeDecay
		}
		cfg.Fields[key] = fc
	}

	return cfg, nil
}

// GetFieldConfig returns the config for a field, falling back to defaults.
func (c *Config) GetFieldConfig(fieldKey string) FieldConfig {
	if fc, ok := c.Fields[fieldKey]; ok {
		return fc
	}
	return FieldConfig{
		ConfidenceThreshold: c.Defaults.ConfidenceThreshold,
		TimeDecay:           &c.Defaults.TimeDecay,
	}
}
