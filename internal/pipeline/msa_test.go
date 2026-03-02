package pipeline

import "testing"

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
