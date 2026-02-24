package peextract

import (
	"strings"
	"testing"
)

func TestNormalizeForSearch(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Focus Financial Partners, LLC", "Focus Financial Partners"},
		{"Wealth Enhancement Group, Inc.", "Wealth Enhancement Group"},
		{"CI Financial Corp", "CI Financial"},
		{"Mercer Global Advisors, Inc", "Mercer Global Advisors"},
		{"Hellman & Friedman LLC", "Hellman & Friedman"},
		{"KKR Capital Markets LP", "KKR Capital Markets"},
		{"Stone Point Capital, L.L.C.", "Stone Point Capital"},
		{"Parthenon Capital Partners", "Parthenon Capital Partners"},
		{"  Trimmed Name, LLC  ", "Trimmed Name"},
	}

	for _, tt := range tests {
		got := normalizeForSearch(tt.input)
		if got != tt.want {
			t.Errorf("normalizeForSearch(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestValidateURL(t *testing.T) {
	tests := []struct {
		url        string
		firmName   string
		wantValid  bool
		wantReason string
	}{
		{"https://cdrcorp.com", "Clayton Dubilier & Rice", true, ""},
		{"https://www.linkedin.com/company/cdr", "CD&R", false, "social media URL: linkedin.com"},
		{"https://twitter.com/warburg", "Warburg Pincus", false, "social media URL: twitter.com"},
		{"https://x.com/tpg", "TPG", false, "social media URL: x.com"},
		{"https://aresfoundation.org", "Ares Management", false, "charitable foundation"},
		{"", "Some Firm", false, "empty URL"},
		{"https://www.sec.gov/cgi-bin/browse-edgar", "Test", false, "government/regulatory site"},
		{"https://aresmgmt.com", "Ares Management", true, ""},
		{"https://www.facebook.com/firm", "Test Firm", false, "social media URL: facebook.com"},
	}

	for _, tt := range tests {
		valid, reason := validateURL(tt.url, tt.firmName)
		if valid != tt.wantValid {
			t.Errorf("validateURL(%q, %q) valid = %v, want %v", tt.url, tt.firmName, valid, tt.wantValid)
		}
		if !tt.wantValid && !strings.Contains(reason, tt.wantReason) {
			t.Errorf("validateURL(%q, %q) reason = %q, want containing %q", tt.url, tt.firmName, reason, tt.wantReason)
		}
	}
}

func TestCaptureSocialURL(t *testing.T) {
	t.Run("captures each platform", func(t *testing.T) {
		c := PEFirmCandidate{}
		captureSocialURL(&c, "https://www.linkedin.com/company/blue-owl")
		captureSocialURL(&c, "https://twitter.com/BAINCAPITAL")
		captureSocialURL(&c, "https://www.facebook.com/firmpage")
		captureSocialURL(&c, "https://www.instagram.com/firmhandle")
		captureSocialURL(&c, "https://www.youtube.com/c/firmchannel")
		captureSocialURL(&c, "https://www.crunchbase.com/organization/firm")

		if c.LinkedInURL != "https://www.linkedin.com/company/blue-owl" {
			t.Errorf("LinkedInURL = %q", c.LinkedInURL)
		}
		if c.TwitterURL != "https://twitter.com/BAINCAPITAL" {
			t.Errorf("TwitterURL = %q", c.TwitterURL)
		}
		if c.FacebookURL != "https://www.facebook.com/firmpage" {
			t.Errorf("FacebookURL = %q", c.FacebookURL)
		}
		if c.InstagramURL != "https://www.instagram.com/firmhandle" {
			t.Errorf("InstagramURL = %q", c.InstagramURL)
		}
		if c.YouTubeURL != "https://www.youtube.com/c/firmchannel" {
			t.Errorf("YouTubeURL = %q", c.YouTubeURL)
		}
		if c.CrunchbaseURL != "https://www.crunchbase.com/organization/firm" {
			t.Errorf("CrunchbaseURL = %q", c.CrunchbaseURL)
		}
	})

	t.Run("x.com maps to TwitterURL", func(t *testing.T) {
		c := PEFirmCandidate{}
		captureSocialURL(&c, "https://x.com/tpg")
		if c.TwitterURL != "https://x.com/tpg" {
			t.Errorf("TwitterURL = %q, want https://x.com/tpg", c.TwitterURL)
		}
	})

	t.Run("first wins dedup", func(t *testing.T) {
		c := PEFirmCandidate{}
		captureSocialURL(&c, "https://linkedin.com/company/first")
		captureSocialURL(&c, "https://linkedin.com/company/second")
		if c.LinkedInURL != "https://linkedin.com/company/first" {
			t.Errorf("LinkedInURL = %q, want first", c.LinkedInURL)
		}
	})

	t.Run("twitter.com does not overwrite x.com", func(t *testing.T) {
		c := PEFirmCandidate{}
		captureSocialURL(&c, "https://x.com/handle")
		captureSocialURL(&c, "https://twitter.com/handle2")
		if c.TwitterURL != "https://x.com/handle" {
			t.Errorf("TwitterURL = %q, want x.com/handle", c.TwitterURL)
		}
	})

	t.Run("non-social URL ignored", func(t *testing.T) {
		c := PEFirmCandidate{}
		captureSocialURL(&c, "https://www.sec.gov/cgi-bin/browse-edgar")
		captureSocialURL(&c, "https://example.com")
		captureSocialURL(&c, "")
		if c.LinkedInURL != "" || c.TwitterURL != "" || c.FacebookURL != "" {
			t.Error("expected all social fields empty for non-social URLs")
		}
	})

	t.Run("preserves original casing", func(t *testing.T) {
		c := PEFirmCandidate{}
		captureSocialURL(&c, "https://www.LinkedIn.com/Company/Firm")
		if c.LinkedInURL != "https://www.LinkedIn.com/Company/Firm" {
			t.Errorf("LinkedInURL = %q, expected original casing preserved", c.LinkedInURL)
		}
	})
}

func TestPEFirmCandidate_Defaults(t *testing.T) {
	c := PEFirmCandidate{
		OwnerName: "Test Partners, LLC",
		OwnerType: "limited liability company",
		RIACount:  5,
		OwnedCRDs: []int{100, 200, 300, 400, 500},
	}

	if c.WebsiteURL != "" {
		t.Error("expected empty website URL by default")
	}
	if c.Source != "" {
		t.Error("expected empty source by default")
	}
	if c.HasControl {
		t.Error("expected HasControl false by default")
	}
}
