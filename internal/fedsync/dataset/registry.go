package dataset

import (
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/config"
)

// Registry maps dataset names to their implementations.
type Registry struct {
	datasets map[string]Dataset
	order    []string // insertion order for deterministic iteration
}

// NewRegistry creates a registry populated with all 26 datasets.
func NewRegistry(cfg *config.Config) *Registry {
	r := &Registry{
		datasets: make(map[string]Dataset),
	}

	// Phase 1: Market Intelligence
	r.Register(&CBP{})
	r.Register(&SUSB{})
	r.Register(&QCEW{})
	r.Register(&OEWS{})
	r.Register(&FPDS{cfg: cfg})
	r.Register(&EconCensus{cfg: cfg})
	r.Register(&PPP{})

	// Phase 1B: Buyer Intelligence (SEC/EDGAR)
	r.Register(&ADVPart1{})
	r.Register(&IACompilation{cfg: cfg})
	r.Register(&Holdings13F{cfg: cfg})
	r.Register(&FormD{cfg: cfg})
	r.Register(&EDGARSubmissions{cfg: cfg})
	r.Register(&EntityXref{})

	// Phase 2: Extended Intelligence
	r.Register(&ADVPart2{cfg: cfg})
	r.Register(&BrokerCheck{})
	r.Register(&FormBD{cfg: cfg})
	r.Register(&OSHITA{})
	r.Register(&EPAECHO{})
	r.Register(&NES{cfg: cfg})
	r.Register(&ASM{cfg: cfg})
	r.Register(&ECI{cfg: cfg})

	// Phase 3: On-Demand
	r.Register(&ADVPart3{cfg: cfg})
	r.Register(&ADVEnrichment{cfg: cfg})
	r.Register(&XBRLFacts{cfg: cfg})
	r.Register(&FRED{cfg: cfg})
	r.Register(&ABS{cfg: cfg})
	r.Register(&CPSLAUS{cfg: cfg})
	r.Register(&M3{cfg: cfg})

	return r
}

// Register adds a dataset to the registry.
func (r *Registry) Register(d Dataset) {
	name := d.Name()
	r.datasets[name] = d
	r.order = append(r.order, name)
}

// Get returns a dataset by name.
func (r *Registry) Get(name string) (Dataset, error) {
	d, ok := r.datasets[name]
	if !ok {
		return nil, eris.Errorf("dataset: unknown dataset %q", name)
	}
	return d, nil
}

// Select returns datasets matching the given criteria.
// If phase is non-nil, only datasets in that phase are returned.
// If names is non-empty, only those named datasets are returned.
func (r *Registry) Select(phase *Phase, names []string) ([]Dataset, error) {
	if len(names) > 0 {
		var result []Dataset
		for _, name := range names {
			d, err := r.Get(name)
			if err != nil {
				return nil, err
			}
			if phase != nil && d.Phase() != *phase {
				continue
			}
			result = append(result, d)
		}
		return result, nil
	}

	if phase != nil {
		return r.ByPhase(*phase), nil
	}

	return r.All(), nil
}

// ByPhase returns all datasets in the given phase, in registration order.
func (r *Registry) ByPhase(phase Phase) []Dataset {
	var result []Dataset
	for _, name := range r.order {
		if r.datasets[name].Phase() == phase {
			result = append(result, r.datasets[name])
		}
	}
	return result
}

// All returns all datasets in registration order.
func (r *Registry) All() []Dataset {
	result := make([]Dataset, 0, len(r.order))
	for _, name := range r.order {
		result = append(result, r.datasets[name])
	}
	return result
}

// AllNames returns all registered dataset names in registration order.
func (r *Registry) AllNames() []string {
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}
