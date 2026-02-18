package model

import "regexp"

// FieldMapping represents a mapping from an internal field key to a Salesforce field.
type FieldMapping struct {
	ID              string         `json:"id"`
	Key             string         `json:"key"`
	SFField         string         `json:"sf_field"`
	SFObject        string         `json:"sf_object"`
	DataType        string         `json:"data_type"`
	Required        bool           `json:"required"`
	MaxLength       int            `json:"max_length,omitempty"`
	Validation      string         `json:"validation,omitempty"`
	ValidationRegex *regexp.Regexp `json:"-"` // pre-compiled from Validation at registry load
	Status          string         `json:"status"`
}

// FieldRegistry is an indexed collection of field mappings.
type FieldRegistry struct {
	Fields   []FieldMapping
	byKey    map[string]*FieldMapping
	bySFName map[string]*FieldMapping
	required []*FieldMapping
}

// NewFieldRegistry creates a FieldRegistry with indexed lookups.
// Pre-compiles validation regexes from FieldMapping.Validation patterns.
func NewFieldRegistry(fields []FieldMapping) *FieldRegistry {
	r := &FieldRegistry{
		Fields:   fields,
		byKey:    make(map[string]*FieldMapping, len(fields)),
		bySFName: make(map[string]*FieldMapping, len(fields)),
	}
	for i := range r.Fields {
		f := &r.Fields[i]
		if f.Validation != "" {
			if re, err := regexp.Compile(f.Validation); err == nil {
				f.ValidationRegex = re
			}
		}
		r.byKey[f.Key] = f
		if f.SFField != "" {
			r.bySFName[f.SFField] = f
		}
		if f.Required {
			r.required = append(r.required, f)
		}
	}
	return r
}

// ByKey returns the field mapping for the given key, or nil if not found.
func (r *FieldRegistry) ByKey(key string) *FieldMapping {
	return r.byKey[key]
}

// BySFName returns the field mapping for the given Salesforce field name, or nil if not found.
func (r *FieldRegistry) BySFName(name string) *FieldMapping {
	return r.bySFName[name]
}

// Required returns all required field mappings.
func (r *FieldRegistry) Required() []*FieldMapping {
	return r.required
}
