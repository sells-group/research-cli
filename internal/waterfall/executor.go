package waterfall

import (
	"context"
	"time"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/internal/waterfall/provider"
	"go.uber.org/zap"
)

// Executor runs the waterfall cascade for a company's field values.
type Executor struct {
	cfg      *Config
	registry *provider.Registry
	now      time.Time // injectable for testing
}

// NewExecutor creates a waterfall executor.
func NewExecutor(cfg *Config, registry *provider.Registry) *Executor {
	return &Executor{
		cfg:      cfg,
		registry: registry,
		now:      time.Now(),
	}
}

// WithNow sets a fixed time for testing.
func (e *Executor) WithNow(t time.Time) *Executor {
	e.now = t
	return e
}

// Run evaluates each field value through the waterfall cascade.
// It takes existing field values (from Phase 7 merge) and enhances them
// with time decay evaluation and optional premium provider lookups.
func (e *Executor) Run(ctx context.Context, company model.Company, fieldValues map[string]model.FieldValue) (*WaterfallResult, error) {
	result := &WaterfallResult{
		Resolutions: make(map[string]FieldResolution),
	}

	// Phase 1: Evaluate existing values with time decay.
	premiumNeeded := make(map[string]FieldConfig) // fields needing premium sources
	for fieldKey, fv := range fieldValues {
		fc := e.cfg.GetFieldConfig(fieldKey)
		decay := fc.TimeDecay
		if decay == nil {
			decay = &e.cfg.Defaults.TimeDecay
		}

		var dataAsOf time.Time
		if fv.DataAsOf != nil {
			dataAsOf = *fv.DataAsOf
		}

		effConf := EffectiveConfidence(fv.Confidence, dataAsOf, e.now, *decay)

		sv := SourceValue{
			Source:              fv.Source,
			Value:               fv.Value,
			RawConfidence:       fv.Confidence,
			EffectiveConfidence: effConf,
			DataAsOf:            fv.DataAsOf,
			Tier:                fv.Tier,
		}

		resolution := FieldResolution{
			FieldKey:     fieldKey,
			Threshold:    fc.ConfidenceThreshold,
			ThresholdMet: effConf >= fc.ConfidenceThreshold,
			Resolved:     effConf >= fc.ConfidenceThreshold,
			Attempts:     []SourceValue{sv},
		}

		if resolution.ThresholdMet {
			resolution.Winner = &sv
		} else {
			// Check if this field has premium sources configured.
			hasPremium := false
			for _, src := range fc.Sources {
				if src.Tier > 0 {
					hasPremium = true
					break
				}
			}
			if hasPremium {
				premiumNeeded[fieldKey] = fc
			}
			// Still set current as winner (best we have so far).
			resolution.Winner = &sv
		}

		result.Resolutions[fieldKey] = resolution
	}

	// Also evaluate fields that have no current value but are in the waterfall config.
	for fieldKey, fc := range e.cfg.Fields {
		if _, exists := result.Resolutions[fieldKey]; exists {
			continue
		}
		hasPremium := false
		for _, src := range fc.Sources {
			if src.Tier > 0 {
				hasPremium = true
				break
			}
		}
		if hasPremium {
			premiumNeeded[fieldKey] = fc
		}
		result.Resolutions[fieldKey] = FieldResolution{
			FieldKey:     fieldKey,
			Threshold:    fc.ConfidenceThreshold,
			ThresholdMet: false,
			Resolved:     false,
		}
	}

	// Phase 2: Premium provider lookups (if registry is available and budget allows).
	if e.registry != nil && len(premiumNeeded) > 0 {
		if err := e.queryPremiumProviders(ctx, company, premiumNeeded, result); err != nil {
			// Premium failures are non-fatal — log and continue.
			zap.L().Warn("waterfall: premium provider error",
				zap.String("company", company.URL),
				zap.Error(err),
			)
		}
	}

	// Compute totals.
	for _, res := range result.Resolutions {
		result.FieldsTotal++
		if res.Resolved {
			result.FieldsResolved++
		}
		result.TotalPremiumUSD += res.PremiumCostUSD
	}

	return result, nil
}

// queryPremiumProviders batches premium requests by provider.
func (e *Executor) queryPremiumProviders(ctx context.Context, company model.Company, needed map[string]FieldConfig, result *WaterfallResult) error {
	if e.registry == nil {
		return nil
	}

	maxBudget := e.cfg.Defaults.MaxPremiumCostUSD
	spent := 0.0

	// Group fields by provider.
	type providerRequest struct {
		fields []string
	}
	byProvider := make(map[string]*providerRequest)

	for fieldKey, fc := range needed {
		for _, src := range fc.Sources {
			if src.Tier == 0 {
				continue
			}
			p := e.registry.Get(src.Name)
			if p == nil {
				continue
			}
			if !p.CanProvide(fieldKey) {
				continue
			}
			if byProvider[src.Name] == nil {
				byProvider[src.Name] = &providerRequest{}
			}
			byProvider[src.Name].fields = append(byProvider[src.Name].fields, fieldKey)
			break // first premium provider wins for this field
		}
	}

	// Execute queries (sequentially per provider, could be parallelized later).
	for provName, req := range byProvider {
		p := e.registry.Get(provName)
		if p == nil {
			continue
		}

		cost := p.CostPerQuery(req.fields)
		if spent+cost > maxBudget {
			zap.L().Info("waterfall: premium budget exhausted",
				zap.String("provider", provName),
				zap.Float64("cost", cost),
				zap.Float64("spent", spent),
				zap.Float64("budget", maxBudget),
			)
			continue
		}

		ident := provider.CompanyIdentifier{
			Domain: company.URL,
			Name:   company.Name,
			State:  company.State,
			City:   company.City,
		}

		qr, err := p.Query(ctx, ident, req.fields)
		if err != nil {
			zap.L().Warn("waterfall: premium query failed",
				zap.String("provider", provName),
				zap.Error(err),
			)
			continue
		}

		spent += qr.CostUSD

		// Merge premium results into resolutions.
		for _, fr := range qr.Fields {
			fc := e.cfg.GetFieldConfig(fr.FieldKey)
			decay := fc.TimeDecay
			if decay == nil {
				decay = &e.cfg.Defaults.TimeDecay
			}

			var dataAsOf time.Time
			if fr.DataAsOf != nil {
				dataAsOf = *fr.DataAsOf
			}
			effConf := EffectiveConfidence(fr.Confidence, dataAsOf, e.now, *decay)

			sv := SourceValue{
				Source:              provName,
				Value:               fr.Value,
				RawConfidence:       fr.Confidence,
				EffectiveConfidence: effConf,
				DataAsOf:            fr.DataAsOf,
				Tier:                2,
			}

			res := result.Resolutions[fr.FieldKey]
			res.Attempts = append(res.Attempts, sv)
			res.PremiumCostUSD += qr.CostUSD / float64(len(qr.Fields))

			// Update winner if premium result is better.
			if res.Winner == nil || effConf > res.Winner.EffectiveConfidence {
				res.Winner = &sv
				res.ThresholdMet = effConf >= fc.ConfidenceThreshold
				res.Resolved = res.ThresholdMet
			}

			result.Resolutions[fr.FieldKey] = res
		}
	}

	return nil
}

// ApplyToFieldValues merges waterfall results back into field values.
// Returns updated field values with effective (time-decayed) confidence.
func ApplyToFieldValues(fieldValues map[string]model.FieldValue, wr *WaterfallResult) map[string]model.FieldValue {
	updated := make(map[string]model.FieldValue, len(fieldValues))

	// Copy existing values.
	for k, v := range fieldValues {
		updated[k] = v
	}

	// Apply waterfall winners.
	for fieldKey, res := range wr.Resolutions {
		if res.Winner == nil {
			continue
		}

		existing, exists := updated[fieldKey]
		if !exists {
			// New field from premium provider.
			updated[fieldKey] = model.FieldValue{
				FieldKey:   fieldKey,
				Value:      res.Winner.Value,
				Confidence: res.Winner.EffectiveConfidence,
				Source:     res.Winner.Source,
				Tier:       res.Winner.Tier,
				DataAsOf:   res.Winner.DataAsOf,
			}
			continue
		}

		// Update confidence to effective (decayed) value.
		existing.Confidence = res.Winner.EffectiveConfidence

		// If winner is from a different source (e.g., premium upgrade), replace value.
		if res.Winner.Source != existing.Source && res.Winner.EffectiveConfidence > existing.Confidence {
			existing.Value = res.Winner.Value
			existing.Source = res.Winner.Source
			existing.Tier = res.Winner.Tier
			existing.DataAsOf = res.Winner.DataAsOf
		}

		updated[fieldKey] = existing
	}

	return updated
}
