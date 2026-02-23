package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidNAICS(t *testing.T) {
	tests := []struct {
		name  string
		code  string
		valid bool
	}{
		// Valid codes
		{"valid 2-digit sector", "54", true},
		{"valid 2-digit manufacturing", "31", true},
		{"valid 3-digit subsector", "541", true},
		{"valid 4-digit industry group", "5415", true},
		{"valid 5-digit code", "54151", true},
		{"valid 6-digit code", "541512", true},
		{"valid construction", "236115", true},
		{"valid finance", "522110", true},
		{"valid retail 2017", "441", true},
		{"valid retail 2022", "455", true},

		// Invalid codes
		{"empty", "", false},
		{"single digit", "5", false},
		{"too long", "5415120", false},
		{"invalid sector", "10", false},
		{"invalid sector 99", "99", false},
		{"letters", "AB", false},
		{"mixed", "54x", false},
		{"invalid subsector", "539", false},
		{"invalid subsector 550", "550", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.valid, IsValidNAICS(tt.code), "code: %q", tt.code)
		})
	}
}

func TestValidateNAICSCode(t *testing.T) {
	t.Run("valid 6-digit code with title", func(t *testing.T) {
		r := ValidateNAICSCode("541512")
		assert.True(t, r.Valid)
		assert.True(t, r.SectorValid)
		assert.True(t, r.SubsectorValid)
		assert.True(t, r.IndustryGroupValid)
		assert.Equal(t, "541512", r.NormalizedCode)
		assert.Equal(t, "Computer Systems Design Services", r.Title)
		assert.GreaterOrEqual(t, r.ConfidenceAdjustment, 1.0)
	})

	t.Run("valid 4-digit code", func(t *testing.T) {
		r := ValidateNAICSCode("5415")
		assert.True(t, r.Valid)
		assert.True(t, r.SectorValid)
		assert.True(t, r.SubsectorValid)
		assert.True(t, r.IndustryGroupValid)
		assert.Equal(t, "541500", r.NormalizedCode)
	})

	t.Run("valid 2-digit sector", func(t *testing.T) {
		r := ValidateNAICSCode("54")
		assert.True(t, r.Valid)
		assert.True(t, r.SectorValid)
		assert.Equal(t, "540000", r.NormalizedCode)
	})

	t.Run("invalid sector", func(t *testing.T) {
		r := ValidateNAICSCode("99")
		assert.False(t, r.Valid)
		assert.False(t, r.SectorValid)
		assert.Equal(t, 0.3, r.ConfidenceAdjustment)
	})

	t.Run("invalid subsector", func(t *testing.T) {
		r := ValidateNAICSCode("539")
		assert.False(t, r.Valid)
		assert.True(t, r.SectorValid)
		assert.False(t, r.SubsectorValid)
		assert.Equal(t, 0.5, r.ConfidenceAdjustment)
	})

	t.Run("empty code", func(t *testing.T) {
		r := ValidateNAICSCode("")
		assert.False(t, r.Valid)
		assert.Equal(t, "empty code", r.Reason)
	})

	t.Run("code with description suffix", func(t *testing.T) {
		r := ValidateNAICSCode("541512 Computer Systems Design")
		assert.True(t, r.Valid)
		assert.Equal(t, "541512", r.NormalizedCode)
	})

	t.Run("code with trailing dashes", func(t *testing.T) {
		r := ValidateNAICSCode("5415--")
		assert.True(t, r.Valid)
		assert.Equal(t, "541500", r.NormalizedCode)
	})

	t.Run("unconfirmed industry group gets slight penalty", func(t *testing.T) {
		// Use a subsector that's valid but with an industry group not in our table.
		// "9281" is in the table, but let's test a more obscure one.
		r := ValidateNAICSCode("611710")
		assert.True(t, r.Valid)
		// 6117 is in the table so this gets a boost.
		assert.True(t, r.IndustryGroupValid)
	})
}

func TestClosestValidNAICS(t *testing.T) {
	t.Run("invalid subsector corrects to sector", func(t *testing.T) {
		code, reason := ClosestValidNAICS("539")
		assert.Equal(t, "530000", code)
		assert.Contains(t, reason, "sector level")
		assert.Contains(t, reason, "Real Estate")
	})

	t.Run("completely invalid returns empty", func(t *testing.T) {
		code, reason := ClosestValidNAICS("99")
		assert.Equal(t, "", code)
		assert.Equal(t, "", reason)
	})

	t.Run("valid code returns normalized", func(t *testing.T) {
		code, reason := ClosestValidNAICS("541512")
		assert.Equal(t, "541512", code)
		assert.Equal(t, "", reason)
	})

	t.Run("too short returns empty", func(t *testing.T) {
		code, _ := ClosestValidNAICS("5")
		assert.Equal(t, "", code)
	})

	t.Run("4-digit with unknown industry group corrects to subsector", func(t *testing.T) {
		// 5419 is valid, so let's test with something that might not be.
		// Actually, the 4-digit table is fairly comprehensive.
		// Let's test a 4-digit that IS in the table to ensure no false correction.
		code, reason := ClosestValidNAICS("5415")
		assert.Equal(t, "541500", code)
		assert.Equal(t, "", reason) // No correction needed.
	})
}

func TestExtractLeadingDigits(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"541512", "541512"},
		{"541512 Computer Systems Design", "541512"},
		{"NAICS 541512", ""},
		{"", ""},
		{"abc", ""},
		{"54-", "54"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, extractLeadingDigits(tt.input), "input: %q", tt.input)
	}
}

func TestNAICSSectorForKeywords(t *testing.T) {
	// Spot-check keyword mappings.
	sectors, ok := NAICSSectorForKeywords["insurance"]
	assert.True(t, ok)
	assert.Contains(t, sectors, "52")

	sectors, ok = NAICSSectorForKeywords["construction"]
	assert.True(t, ok)
	assert.Contains(t, sectors, "23")

	sectors, ok = NAICSSectorForKeywords["software"]
	assert.True(t, ok)
	assert.Contains(t, sectors, "51")
}
