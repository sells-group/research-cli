package pipeline

import "testing"

func TestStateToRegion(t *testing.T) {
	tests := []struct {
		state string
		want  string
	}{
		{"CA", "West"},
		{"ca", "West"},
		{"TX", "South"},
		{"NY", "Northeast"},
		{"IL", "Midwest"},
		{"DC", "South"},
		{"WA", "West"},
		{"FL", "South"},
		{"PA", "Northeast"},
		{"OH", "Midwest"},
		{"", ""},
		{"XX", ""},
		{"  ca  ", "West"},
	}

	for _, tt := range tests {
		got := StateToRegion(tt.state)
		if got != tt.want {
			t.Errorf("StateToRegion(%q) = %q, want %q", tt.state, got, tt.want)
		}
	}
}
