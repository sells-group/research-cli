package pipeline

import (
	"testing"

	"github.com/sells-group/research-cli/internal/model"
)

func TestNormalizeBusinessModel_ExactCanonical(t *testing.T) {
	t.Parallel()
	canonicals := []string{
		"Services", "Manufacturer", "Distributor", "Retailer",
		"SaaS", "Marketplace", "Software", "Construction",
		"Financial Services", "Healthcare", "Other",
	}
	for _, c := range canonicals {
		result, ok := NormalizeBusinessModel(c)
		if !ok {
			if c == "Other" {
				// "Other" maps via canonicalModels, should be ok=true.
				if result != "Other" {
					t.Errorf("NormalizeBusinessModel(%q) = %q, want %q", c, result, "Other")
				}
				continue
			}
			t.Errorf("NormalizeBusinessModel(%q) ok = false, want true", c)
		}
		if result != c {
			t.Errorf("NormalizeBusinessModel(%q) = %q, want %q", c, result, c)
		}
	}
}

func TestNormalizeBusinessModel_CaseInsensitive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"services", "Services"},
		{"SERVICES", "Services"},
		{"Services", "Services"},
		{"manufacturer", "Manufacturer"},
		{"MANUFACTURER", "Manufacturer"},
		{"financial services", "Financial Services"},
		{"FINANCIAL SERVICES", "Financial Services"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			result, ok := NormalizeBusinessModel(tc.input)
			if !ok {
				t.Errorf("NormalizeBusinessModel(%q) ok = false, want true", tc.input)
			}
			if result != tc.want {
				t.Errorf("NormalizeBusinessModel(%q) = %q, want %q", tc.input, result, tc.want)
			}
		})
	}
}

func TestNormalizeBusinessModel_LLMVariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"B2B Service Provider", "Services"},
		{"Professional Service Firm", "Services"},
		{"Consulting Firm", "Services"},
		{"Manufacturing Company", "Manufacturer"},
		{"Steel Fabrication", "Manufacturer"},
		{"General Contractor", "Construction"},
		{"Roofing Contractor", "Construction"},
		{"Financial Advisor", "Financial Services"},
		{"Wealth Management Firm", "Financial Services"},
		{"Wholesale Distributor", "Distributor"},
		{"E-Commerce Retailer", "Retailer"},
		{"Cloud Platform Provider", "SaaS"},
		{"Subscription Software", "SaaS"},
		{"Medical Device Company", "Healthcare"},
		{"Biotech Startup", "Healthcare"},
		{"Technology Company", "Software"},
		{"Insurance Broker", "Financial Services"},
		{"Staffing Agency", "Services"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			result, ok := NormalizeBusinessModel(tc.input)
			if !ok {
				t.Errorf("NormalizeBusinessModel(%q) ok = false, want true", tc.input)
			}
			if result != tc.want {
				t.Errorf("NormalizeBusinessModel(%q) = %q, want %q", tc.input, result, tc.want)
			}
		})
	}
}

func TestNormalizeBusinessModel_GrataLabels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"Services", "Services"},
		{"Manufacturer", "Manufacturer"},
		{"Distributor", "Distributor"},
		{"Retailer", "Retailer"},
		{"Software", "Software"},
		{"Construction", "Construction"},
		{"Healthcare", "Healthcare"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			result, ok := NormalizeBusinessModel(tc.input)
			if !ok {
				t.Errorf("NormalizeBusinessModel(%q) ok = false, want true", tc.input)
			}
			if result != tc.want {
				t.Errorf("NormalizeBusinessModel(%q) = %q, want %q", tc.input, result, tc.want)
			}
		})
	}
}

func TestNormalizeBusinessModel_EmptyAndUnknown(t *testing.T) {
	t.Parallel()

	// Empty input.
	result, ok := NormalizeBusinessModel("")
	if ok {
		t.Error("NormalizeBusinessModel(\"\") ok = true, want false")
	}
	if result != "" {
		t.Errorf("NormalizeBusinessModel(\"\") = %q, want \"\"", result)
	}

	// Unknown input.
	result, ok = NormalizeBusinessModel("Intergalactic Trade Federation")
	if ok {
		t.Error("NormalizeBusinessModel(\"Intergalactic Trade Federation\") ok = true, want false")
	}
	if result != "Other" {
		t.Errorf("NormalizeBusinessModel(\"Intergalactic Trade Federation\") = %q, want \"Other\"", result)
	}
}

func TestNormalizeBusinessModel_LongestMatchWins(t *testing.T) {
	t.Parallel()

	// "financial services" should match Financial Services, not Services.
	result, ok := NormalizeBusinessModel("Financial Services Company")
	if !ok {
		t.Error("NormalizeBusinessModel(\"Financial Services Company\") ok = false, want true")
	}
	if result != "Financial Services" {
		t.Errorf("NormalizeBusinessModel(\"Financial Services Company\") = %q, want \"Financial Services\"", result)
	}

	// "financial advisor" should match Financial Services, not just advisory→Services.
	result, ok = NormalizeBusinessModel("Financial Advisor")
	if !ok {
		t.Error("NormalizeBusinessModel(\"Financial Advisor\") ok = false, want true")
	}
	if result != "Financial Services" {
		t.Errorf("NormalizeBusinessModel(\"Financial Advisor\") = %q, want \"Financial Services\"", result)
	}
}

func TestNormalizeBusinessModelAnswer(t *testing.T) {
	t.Parallel()
	answers := []model.ExtractionAnswer{
		{FieldKey: "company_name", Value: "Acme Corp", Source: "t1"},
		{FieldKey: "business_model", Value: "B2B Service Provider", Source: "t1"},
		{FieldKey: "naics_code", Value: "541512", Source: "t1"},
	}

	result := NormalizeBusinessModelAnswer(answers)

	// business_model should be normalized.
	if result[1].Value != "Services" {
		t.Errorf("business_model Value = %q, want \"Services\"", result[1].Value)
	}
	if result[1].Source != "t1+bm_normalized" {
		t.Errorf("business_model Source = %q, want \"t1+bm_normalized\"", result[1].Source)
	}

	// Other answers untouched.
	if result[0].Value != "Acme Corp" {
		t.Errorf("company_name Value = %q, want \"Acme Corp\"", result[0].Value)
	}
	if result[2].Value != "541512" {
		t.Errorf("naics_code Value = %q, want \"541512\"", result[2].Value)
	}
}

func TestNormalizeBusinessModelAnswer_NoChange(t *testing.T) {
	t.Parallel()
	answers := []model.ExtractionAnswer{
		{FieldKey: "business_model", Value: "Services", Source: "t1"},
	}

	result := NormalizeBusinessModelAnswer(answers)

	// Already canonical — no change, no provenance suffix.
	if result[0].Value != "Services" {
		t.Errorf("business_model Value = %q, want \"Services\"", result[0].Value)
	}
	if result[0].Source != "t1" {
		t.Errorf("business_model Source = %q, want \"t1\" (no +bm_normalized suffix)", result[0].Source)
	}
}
