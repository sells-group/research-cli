// Package provider defines the interface and implementations for waterfall data providers.
package provider

import (
	"context"
	"sync"
	"time"
)

// CompanyIdentifier holds identifiers for looking up a company.
type CompanyIdentifier struct {
	Domain       string
	Name         string
	State        string
	City         string
	SalesforceID string
	CRD          string
	CIK          string
	EIN          string
}

// FieldResult is a single field value returned by a provider.
type FieldResult struct {
	FieldKey   string     `json:"field_key"`
	Value      any        `json:"value"`
	Confidence float64    `json:"confidence"`
	DataAsOf   *time.Time `json:"data_as_of,omitempty"`
}

// QueryResult is the complete response from a provider.
type QueryResult struct {
	Provider string        `json:"provider"`
	Fields   []FieldResult `json:"fields"`
	CostUSD  float64       `json:"cost_usd"`
	RawJSON  []byte        `json:"raw_json,omitempty"`
}

// Provider defines the interface for premium data providers.
type Provider interface {
	// Name returns the provider identifier (matches source name in waterfall config).
	Name() string
	// SupportedFields returns the list of field keys this provider can supply.
	SupportedFields() []string
	// CanProvide checks if the provider can supply a specific field.
	CanProvide(fieldKey string) bool
	// CostPerQuery estimates cost for querying specific fields.
	CostPerQuery(fields []string) float64
	// Query fetches data for a company.
	Query(ctx context.Context, company CompanyIdentifier, fields []string) (*QueryResult, error)
}

// Registry manages available premium providers.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

// NewRegistry creates an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]Provider),
	}
}

// Register adds a provider to the registry.
func (r *Registry) Register(p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.Name()] = p
}

// Get returns a provider by name, or nil if not found.
func (r *Registry) Get(name string) Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[name]
}

// List returns all registered provider names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}
