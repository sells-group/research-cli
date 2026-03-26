package temporal

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	maxWorkflowLabelLen = 32
	defaultWorkflowKind = "workflow"
)

// BuildWorkflowID returns a normalized, timestamp-suffixed Temporal workflow ID.
func BuildWorkflowID(at time.Time, kind string, labels ...string) string {
	parts := []string{normalizeWorkflowLabel(kind)}
	if parts[0] == "" {
		parts[0] = defaultWorkflowKind
	}

	for _, label := range labels {
		normalized := normalizeWorkflowLabel(label)
		if normalized == "" {
			continue
		}
		parts = append(parts, normalized)
	}

	timestamp := at.UTC()
	parts = append(parts, fmt.Sprintf("%s%09dz", timestamp.Format("20060102t150405"), timestamp.Nanosecond()))
	return strings.Join(parts, "-")
}

// BuildStableWorkflowID returns a normalized, deterministic Temporal workflow ID.
func BuildStableWorkflowID(kind string, labels ...string) string {
	parts := []string{normalizeWorkflowLabel(kind)}
	if parts[0] == "" {
		parts[0] = defaultWorkflowKind
	}

	for _, label := range labels {
		normalized := normalizeWorkflowLabel(label)
		if normalized == "" {
			continue
		}
		parts = append(parts, normalized)
	}

	return strings.Join(parts, "-")
}

// NewWorkflowID returns a normalized Temporal workflow ID using the current UTC time.
func NewWorkflowID(kind string, labels ...string) string {
	return BuildWorkflowID(time.Now().UTC(), kind, labels...)
}

// WorkflowURLKey returns a deterministic company key derived from a URL.
func WorkflowURLKey(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	canonical := canonicalWorkflowURL(trimmed)
	base := WorkflowHostLabel(canonical)
	if base == "" {
		base = normalizeWorkflowLabel(trimmed)
	}

	sum := sha256.Sum256([]byte(canonical))
	if base == "" {
		return fmt.Sprintf("%x", sum[:4])
	}
	return fmt.Sprintf("%s-%x", base, sum[:4])
}

// WorkflowHostLabel returns a normalized host-derived label suitable for workflow IDs.
func WorkflowHostLabel(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	candidate := trimmed
	if !strings.Contains(candidate, "://") {
		candidate = "https://" + candidate
	}

	parsed, err := url.Parse(candidate)
	if err == nil {
		if host := parsed.Hostname(); host != "" {
			return normalizeWorkflowLabel(host)
		}
	}

	return normalizeWorkflowLabel(trimmed)
}

func canonicalWorkflowURL(raw string) string {
	candidate := strings.TrimSpace(raw)
	if candidate == "" {
		return ""
	}

	if !strings.Contains(candidate, "://") {
		candidate = "https://" + candidate
	}

	parsed, err := url.Parse(candidate)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(raw))
	}

	host := strings.ToLower(parsed.Hostname())
	path := strings.TrimSuffix(parsed.EscapedPath(), "/")
	if path == "" {
		return host
	}
	return host + path
}

func normalizeWorkflowLabel(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	if lower == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(lower))

	lastDash := false
	for _, r := range lower {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}

		if lastDash {
			continue
		}
		b.WriteByte('-')
		lastDash = true
	}

	normalized := strings.Trim(b.String(), "-")
	if normalized == "" {
		return ""
	}
	if len(normalized) > maxWorkflowLabelLen {
		normalized = strings.Trim(normalized[:maxWorkflowLabelLen], "-")
	}
	return normalized
}
