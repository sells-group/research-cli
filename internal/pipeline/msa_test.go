package pipeline

import "testing"

func TestMSAShortName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		// Curated abbreviations.
		{"Dallas-Fort Worth-Arlington, TX", "DFW"},
		{"New York-Newark-Jersey City, NY-NJ-PA", "NYC"},
		{"Washington-Arlington-Alexandria, DC-VA-MD-WV", "DC"},
		{"Minneapolis-St. Paul-Bloomington, MN-WI", "MSP"},
		{"San Francisco-Oakland-Berkeley, CA", "SF Bay Area"},
		{"Tampa-St. Petersburg-Clearwater, FL", "Tampa Bay"},
		{"Los Angeles-Long Beach-Anaheim, CA", "LA"},
		{"San Jose-Sunnyvale-Santa Clara, CA", "Silicon Valley"},
		{"Miami-Fort Lauderdale-Pompano Beach, FL", "South Florida"},
		{"Riverside-San Bernardino-Ontario, CA", "Inland Empire"},
		{"Virginia Beach-Norfolk-Newport News, VA-NC", "Hampton Roads"},
		{"Louisville/Jefferson County, KY-IN", "Louisville"},

		// Fallback: first city before "-".
		{"Atlanta-Sandy Springs-Alpharetta, GA", "Atlanta"},
		{"Houston-The Woodlands-Sugar Land, TX", "Houston"},
		{"Nashville-Davidson-Murfreesboro-Franklin, TN", "Nashville"},
		{"Columbus, OH", "Columbus"},
		{"Salt Lake City, UT", "Salt Lake City"},

		// Single-city MSAs.
		{"Pittsburgh, PA", "Pittsburgh"},
		{"Tucson, AZ", "Tucson"},

		// St. prefix handling.
		{"St. Louis, MO-IL", "St. Louis"},
		{"St. Cloud, MN", "St. Cloud"},

		// Empty input.
		{"", ""},
	}

	for _, tt := range tests {
		got := MSAShortName(tt.input)
		if got != tt.want {
			t.Errorf("MSAShortName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestLookupMSA(t *testing.T) {
	tests := []struct {
		city  string
		state string
		want  string
	}{
		{"New York", "NY", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"new york", "ny", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"Los Angeles", "CA", "Los Angeles-Long Beach-Anaheim, CA"},
		{"Chicago", "IL", "Chicago-Naperville-Elgin, IL-IN-WI"},
		{"Houston", "TX", "Houston-The Woodlands-Sugar Land, TX"},
		{"Phoenix", "AZ", "Phoenix-Mesa-Chandler, AZ"},
		{"Salt Lake City", "UT", "Salt Lake City, UT"},
		// Suburban cities added for MSA gap coverage.
		{"Morganville", "NJ", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"Freehold", "NJ", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"Edison", "NJ", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"Hoboken", "NJ", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"Princeton", "NJ", "New York-Newark-Jersey City, NY-NJ-PA"},
		{"Walnut Creek", "CA", "San Francisco-Oakland-Berkeley, CA"},
		{"Pleasanton", "CA", "San Francisco-Oakland-Berkeley, CA"},
		{"Katy", "TX", "Houston-The Woodlands-Sugar Land, TX"},
		{"Ashburn", "VA", "Washington-Arlington-Alexandria, DC-VA-MD-WV"},
		{"King of Prussia", "PA", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD"},
		{"Coral Gables", "FL", "Miami-Fort Lauderdale-Pompano Beach, FL"},
		{"Highlands Ranch", "CO", "Denver-Aurora-Lakewood, CO"},
		{"Issaquah", "WA", "Seattle-Tacoma-Bellevue, WA"},
		{"Brookline", "MA", "Boston-Cambridge-Newton, MA-NH"},
		{"Dunwoody", "GA", "Atlanta-Sandy Springs-Alpharetta, GA"},
		{"Los Gatos", "CA", "San Jose-Sunnyvale-Santa Clara, CA"},
		{"Southlake", "TX", "Dallas-Fort Worth-Arlington, TX"},
		{"", "CA", ""},
		{"Boston", "", ""},
		{"", "", ""},
		{"Smalltown", "KS", ""}, // Not in index
		{"  chicago  ", "  IL  ", "Chicago-Naperville-Elgin, IL-IN-WI"},
	}

	for _, tt := range tests {
		got := LookupMSA(tt.city, tt.state)
		if got != tt.want {
			t.Errorf("LookupMSA(%q, %q) = %q, want %q", tt.city, tt.state, got, tt.want)
		}
	}
}
