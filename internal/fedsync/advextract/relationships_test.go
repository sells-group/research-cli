package advextract

import (
	"testing"
)

func TestNormalizeName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Charles Schwab & Co., Inc.", "charles schwab & co."},
		{"  Fidelity Investments  ", "fidelity investments"},
		{"TD Ameritrade, LLC", "td ameritrade"},
		{"BlackRock, Inc", "blackrock"},
		{"Vanguard", "vanguard"},
		{"State Street Corp.", "state street"},
	}

	for _, tt := range tests {
		result := normalizeName(tt.input)
		if result != tt.expected {
			t.Errorf("normalizeName(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestExtractStringValue(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{"Charles Schwab", "Charles Schwab"},
		{"  Fidelity  ", "Fidelity"},
		{nil, ""},
		{42, ""},
		{true, ""},
	}

	for _, tt := range tests {
		result := extractStringValue(tt.input)
		if result != tt.expected {
			t.Errorf("extractStringValue(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestExtractStringList(t *testing.T) {
	t.Run("array of any", func(t *testing.T) {
		input := []any{"Schwab", "Fidelity", "TD Ameritrade"}
		result := extractStringList(input)
		if len(result) != 3 {
			t.Fatalf("expected 3 items, got %d", len(result))
		}
		if result[0] != "Schwab" {
			t.Errorf("expected first item Schwab, got %s", result[0])
		}
	})

	t.Run("comma-separated string", func(t *testing.T) {
		input := "Schwab, Fidelity, TD Ameritrade"
		result := extractStringList(input)
		if len(result) != 3 {
			t.Fatalf("expected 3 items, got %d", len(result))
		}
	})

	t.Run("nil", func(t *testing.T) {
		result := extractStringList(nil)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}
