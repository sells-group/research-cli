package geoscraper

import "github.com/rotisserie/eris"

// Registry maps scraper names to their implementations.
type Registry struct {
	scrapers map[string]GeoScraper
	order    []string // insertion order for deterministic iteration
}

// NewRegistry creates an empty registry. Scrapers are registered incrementally
// as individual scraper implementations are added.
func NewRegistry() *Registry {
	return &Registry{
		scrapers: make(map[string]GeoScraper),
	}
}

// Register adds a scraper to the registry.
func (r *Registry) Register(s GeoScraper) {
	name := s.Name()
	r.scrapers[name] = s
	r.order = append(r.order, name)
}

// Get returns a scraper by name.
func (r *Registry) Get(name string) (GeoScraper, error) {
	s, ok := r.scrapers[name]
	if !ok {
		return nil, eris.Errorf("geoscraper: unknown scraper %q", name)
	}
	return s, nil
}

// Select returns scrapers matching the given criteria.
// If category is non-nil, only scrapers in that category are returned.
// If names is non-empty, only those named scrapers are returned.
// If states is non-empty, only StateScrapers covering those FIPS codes are returned.
func (r *Registry) Select(category *Category, names []string, states []string) ([]GeoScraper, error) {
	if len(names) > 0 {
		var result []GeoScraper
		for _, name := range names {
			s, err := r.Get(name)
			if err != nil {
				return nil, err
			}
			if category != nil && s.Category() != *category {
				continue
			}
			if len(states) > 0 && !matchesStates(s, states) {
				continue
			}
			result = append(result, s)
		}
		return result, nil
	}

	all := r.All()
	var result []GeoScraper
	for _, s := range all {
		if category != nil && s.Category() != *category {
			continue
		}
		if len(states) > 0 && !matchesStates(s, states) {
			continue
		}
		result = append(result, s)
	}
	return result, nil
}

// ByCategory returns all scrapers in the given category, in registration order.
func (r *Registry) ByCategory(cat Category) []GeoScraper {
	var result []GeoScraper
	for _, name := range r.order {
		if r.scrapers[name].Category() == cat {
			result = append(result, r.scrapers[name])
		}
	}
	return result
}

// All returns all scrapers in registration order.
func (r *Registry) All() []GeoScraper {
	result := make([]GeoScraper, 0, len(r.order))
	for _, name := range r.order {
		result = append(result, r.scrapers[name])
	}
	return result
}

// AllNames returns all registered scraper names in registration order.
func (r *Registry) AllNames() []string {
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// matchesStates checks if a scraper covers any of the given state FIPS codes.
// Non-StateScraper scrapers (national/on-demand) never match a state filter.
func matchesStates(s GeoScraper, fips []string) bool {
	ss, ok := s.(StateScraper)
	if !ok {
		return false
	}
	scraperStates := ss.States()
	for _, want := range fips {
		for _, have := range scraperStates {
			if want == have {
				return true
			}
		}
	}
	return false
}
