package analysis

import "github.com/rotisserie/eris"

// Registry maps analyzer names to their implementations and validates dependencies.
type Registry struct {
	analyzers map[string]Analyzer
	order     []string // insertion order for deterministic iteration
}

// NewRegistry creates an empty registry. Analyzers are registered incrementally
// as individual analyzer implementations are added.
func NewRegistry() *Registry {
	return &Registry{
		analyzers: make(map[string]Analyzer),
	}
}

// Register adds an analyzer to the registry.
func (r *Registry) Register(a Analyzer) {
	name := a.Name()
	r.analyzers[name] = a
	r.order = append(r.order, name)
}

// Validate checks that all declared dependencies reference registered analyzers
// and that no cycles exist. Call after all analyzers are registered.
func (r *Registry) Validate() error {
	for _, name := range r.order {
		a := r.analyzers[name]
		for _, dep := range a.Dependencies() {
			if _, ok := r.analyzers[dep]; !ok {
				return eris.Errorf("analysis: analyzer %q depends on unknown analyzer %q", name, dep)
			}
		}
	}
	_, err := r.TopoSort()
	return err
}

// TopoSort returns analyzers in dependency order using Kahn's algorithm.
// Analyzers with no dependencies come first. Returns an error if cycles exist.
func (r *Registry) TopoSort() ([]Analyzer, error) {
	inDegree := make(map[string]int, len(r.order))
	dependents := make(map[string][]string, len(r.order))

	for _, name := range r.order {
		inDegree[name] = 0
	}
	for _, name := range r.order {
		for _, dep := range r.analyzers[name].Dependencies() {
			dependents[dep] = append(dependents[dep], name)
			inDegree[name]++
		}
	}

	// Seed queue with zero in-degree nodes, preserving registration order.
	var queue []string
	for _, name := range r.order {
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}

	var sorted []Analyzer
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		sorted = append(sorted, r.analyzers[name])

		for _, dep := range dependents[name] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if len(sorted) != len(r.order) {
		return nil, eris.New("analysis: dependency cycle detected in analyzer registry")
	}

	return sorted, nil
}

// Get returns an analyzer by name.
func (r *Registry) Get(name string) (Analyzer, error) {
	a, ok := r.analyzers[name]
	if !ok {
		return nil, eris.Errorf("analysis: unknown analyzer %q", name)
	}
	return a, nil
}

// Select returns analyzers matching the given criteria in topological order.
// When specific names are requested, their transitive dependencies are
// automatically included to ensure a valid execution plan.
func (r *Registry) Select(category *Category, names []string) ([]Analyzer, error) {
	all, err := r.TopoSort()
	if err != nil {
		return nil, err
	}

	if len(names) > 0 {
		needed := make(map[string]bool)
		var resolve func(name string) error
		resolve = func(name string) error {
			if needed[name] {
				return nil
			}
			a, ok := r.analyzers[name]
			if !ok {
				return eris.Errorf("analysis: unknown analyzer %q", name)
			}
			needed[name] = true
			for _, dep := range a.Dependencies() {
				if err := resolve(dep); err != nil {
					return err
				}
			}
			return nil
		}
		for _, name := range names {
			if err := resolve(name); err != nil {
				return nil, err
			}
		}

		var result []Analyzer
		for _, a := range all {
			if needed[a.Name()] {
				result = append(result, a)
			}
		}
		return result, nil
	}

	if category != nil {
		var result []Analyzer
		for _, a := range all {
			if a.Category() == *category {
				result = append(result, a)
			}
		}
		return result, nil
	}

	return all, nil
}

// All returns all analyzers in registration order.
func (r *Registry) All() []Analyzer {
	result := make([]Analyzer, 0, len(r.order))
	for _, name := range r.order {
		result = append(result, r.analyzers[name])
	}
	return result
}

// AllNames returns all registered analyzer names in registration order.
func (r *Registry) AllNames() []string {
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// ByCategory returns all analyzers in the given category, in registration order.
func (r *Registry) ByCategory(cat Category) []Analyzer {
	var result []Analyzer
	for _, name := range r.order {
		if r.analyzers[name].Category() == cat {
			result = append(result, r.analyzers[name])
		}
	}
	return result
}
