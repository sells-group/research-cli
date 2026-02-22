package model

import (
	"regexp"
	"strings"
)

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
			f.Validation = fixNotionRegex(f.Validation)
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

// fixNotionRegex restores backslash-escaped regex shorthand classes that
// Notion's RichText PlainText strips. For example, \d becomes d, \s becomes s.
// This function restores them when they appear inside character classes [...]
// or before quantifiers {n}, +, *, ?.
func fixNotionRegex(s string) string {
	if s == "" {
		return s
	}
	// Quick check: if it already contains backslash-escaped shorthands, skip.
	if strings.ContainsAny(s, "\\") {
		return s
	}
	var buf strings.Builder
	buf.Grow(len(s) + 8)
	inCharClass := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '[' {
			inCharClass = true
			buf.WriteByte(c)
			continue
		}
		if c == ']' && inCharClass {
			inCharClass = false
			buf.WriteByte(c)
			continue
		}
		restore := false
		if c == 'd' || c == 's' || c == 'w' {
			if inCharClass {
				// Restore unless part of a character range like a-d.
				if i == 0 || s[i-1] != '-' {
					restore = true
				}
			} else if i+1 < len(s) {
				next := s[i+1]
				if next == '{' || next == '+' || next == '*' || next == '?' {
					restore = true
				}
			}
		}
		// Escape literal hyphen inside character class that Notion stripped from \-.
		// Without this, ()-+ is parsed as a range from ) to +, not as literal )-+.
		// Only escape when between non-alphanumeric chars (preserve real ranges like 0-9, a-z).
		if c == '-' && inCharClass && i > 0 && i+1 < len(s) && s[i+1] != ']' && s[i-1] != '[' {
			prev, next := s[i-1], s[i+1]
			isAlphaNum := func(b byte) bool {
				return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
			}
			if !isAlphaNum(prev) || !isAlphaNum(next) {
				buf.WriteByte('\\')
			}
		}
		if restore {
			buf.WriteByte('\\')
		}
		buf.WriteByte(c)
	}
	return buf.String()
}
