package company

import (
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestMergeGoldenRecord_BasicFields(t *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"name":        {Value: "Acme Corp", Confidence: 0.9},
		"legal_name":  {Value: "Acme Corporation LLC", Confidence: 0.9},
		"description": {Value: "Software company", Confidence: 0.8},
		"phone":       {Value: "555-1234", Confidence: 0.8},
		"email":       {Value: "info@acme.com", Confidence: 0.8},
		"website":     {Value: "https://acme.com", Confidence: 0.9},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, "Acme Corp", r.Name)
	assert.Equal(t, "Acme Corporation LLC", r.LegalName)
	assert.Equal(t, "Software company", r.Description)
	assert.Equal(t, "555-1234", r.Phone)
	assert.Equal(t, "info@acme.com", r.Email)
	assert.Equal(t, "https://acme.com", r.Website)
}

func TestMergeGoldenRecord_NumericFields(t *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"employee_count":    {Value: float64(100), Confidence: 0.8},
		"employee_estimate": {Value: float64(120), Confidence: 0.7},
		"revenue_estimate":  {Value: float64(5000000), Confidence: 0.6},
		"year_founded":      {Value: float64(2010), Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, 100, *r.EmployeeCount)
	assert.Equal(t, 120, *r.EmployeeEstimate)
	assert.Equal(t, int64(5000000), *r.RevenueEstimate)
	assert.Equal(t, 2010, r.YearFounded)
}

func TestMergeGoldenRecord_EnrichmentDetailFields(t *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"services_list":          {Value: "Consulting, Advisory, Tax", Confidence: 0.7},
		"service_area":           {Value: "Northeast US", Confidence: 0.7},
		"licenses":               {Value: "CPA, Series 65", Confidence: 0.7},
		"owner_name":             {Value: "John Smith", Confidence: 0.8},
		"customer_types":         {Value: "Small business, Enterprise", Confidence: 0.7},
		"differentiators":        {Value: "Award-winning service", Confidence: 0.7},
		"reputation_summary":     {Value: "Well-regarded firm", Confidence: 0.7},
		"acquisition_assessment": {Value: "Strong fit for M&A", Confidence: 0.7},
		"key_people":             {Value: "CEO, CFO, COO", Confidence: 0.7},
		"exec_first_name":        {Value: "Jane", Confidence: 0.8},
		"exec_last_name":         {Value: "Doe", Confidence: 0.8},
		"exec_title":             {Value: "CEO", Confidence: 0.7},
		"exec_linkedin":          {Value: "https://linkedin.com/in/janedoe", Confidence: 0.8},
		"review_count":           {Value: float64(42), Confidence: 0.8},
		"review_rating":          {Value: float64(4.5), Confidence: 0.8},
		"employees_linkedin":     {Value: float64(85), Confidence: 0.7},
		"location_count":         {Value: float64(3), Confidence: 0.7},
		"end_markets":            {Value: "Financial services, Healthcare", Confidence: 0.7},
		"linkedin_url":           {Value: "https://linkedin.com/company/acme", Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, "Consulting, Advisory, Tax", r.ServicesList)
	assert.Equal(t, "Northeast US", r.ServiceArea)
	assert.Equal(t, "CPA, Series 65", r.LicensesText)
	assert.Equal(t, "John Smith", r.OwnerName)
	assert.Equal(t, "Small business, Enterprise", r.CustomerTypes)
	assert.Equal(t, "Award-winning service", r.Differentiators)
	assert.Equal(t, "Well-regarded firm", r.ReputationSummary)
	assert.Equal(t, "Strong fit for M&A", r.AcquisitionAssessment)
	assert.Equal(t, "CEO, CFO, COO", r.KeyPeople)
	assert.Equal(t, "Jane", r.ExecFirstName)
	assert.Equal(t, "Doe", r.ExecLastName)
	assert.Equal(t, "CEO", r.ExecTitle)
	assert.Equal(t, "https://linkedin.com/in/janedoe", r.ExecLinkedIn)
	assert.Equal(t, 42, *r.ReviewCount)
	assert.Equal(t, 4.5, *r.ReviewRating)
	assert.Equal(t, 85, *r.EmployeesLinkedIn)
	assert.Equal(t, 3, *r.LocationCount)
	assert.Equal(t, "Financial services, Healthcare", r.EndMarkets)
	assert.Equal(t, "https://linkedin.com/company/acme", r.LinkedInURL)
}

func TestMergeGoldenRecord_Aliases(t *testing.T) {
	// Test canonical aliases from aggregate.go fieldKeyAliases.
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"service_mix":    {Value: "Advisory, Compliance", Confidence: 0.7},
		"locations":      {Value: float64(5), Confidence: 0.7},
		"company_name":   {Value: "Alias Corp", Confidence: 0.8},
		"address_street": {Value: "123 Main St", Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, "Advisory, Compliance", r.ServicesList)
	assert.Equal(t, 5, *r.LocationCount)
	assert.Equal(t, "Alias Corp", r.Name)
	assert.Equal(t, "123 Main St", r.Street)
}

func TestMergeGoldenRecord_NilValues(t *testing.T) {
	r := &CompanyRecord{Name: "Existing"}
	fv := map[string]model.FieldValue{
		"name":          {Value: nil, Confidence: 0.9},
		"services_list": {Value: nil, Confidence: 0.7},
		"review_count":  {Value: nil, Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	// Nil values should not overwrite existing data.
	assert.Equal(t, "Existing", r.Name)
	assert.Equal(t, "", r.ServicesList)
	assert.Nil(t, r.ReviewCount)
}

func TestMergeGoldenRecord_ConfidenceThreshold(t *testing.T) {
	r := &CompanyRecord{Name: "Original Name"}
	fv := map[string]model.FieldValue{
		"name": {Value: "Low Conf Name", Confidence: 0.3},
	}
	MergeGoldenRecord(r, fv)

	// Low confidence should not overwrite existing value.
	assert.Equal(t, "Original Name", r.Name)

	// High confidence should overwrite.
	fv["name"] = model.FieldValue{Value: "High Conf Name", Confidence: 0.9}
	MergeGoldenRecord(r, fv)
	assert.Equal(t, "High Conf Name", r.Name)
}

func TestMergeGoldenRecord_EmptyFieldAlwaysSet(t *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"services_list": {Value: "Consulting", Confidence: 0.1},
	}
	MergeGoldenRecord(r, fv)

	// Empty field should be set even with low confidence.
	assert.Equal(t, "Consulting", r.ServicesList)
}

func TestMergeGoldenRecord_ClassificationAndAddress(t *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"naics_code":         {Value: "541110", Confidence: 0.8},
		"sic_code":           {Value: "6311", Confidence: 0.8},
		"business_model":     {Value: "services", Confidence: 0.7},
		"ownership_type":     {Value: "private", Confidence: 0.7},
		"revenue_range":      {Value: "$1M-$5M", Confidence: 0.7},
		"revenue_confidence": {Value: float64(0.85), Confidence: 0.7},
		"city":               {Value: "Austin", Confidence: 0.8},
		"state":              {Value: "TX", Confidence: 0.8},
		"zip_code":           {Value: "78701", Confidence: 0.8},
		"street":             {Value: "456 Oak Ave", Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, "541110", r.NAICSCode)
	assert.Equal(t, "6311", r.SICCode)
	assert.Equal(t, "services", r.BusinessModel)
	assert.Equal(t, "private", r.OwnershipType)
	assert.Equal(t, "$1M-$5M", r.RevenueRange)
	assert.Equal(t, 0.85, *r.RevenueConfidence)
	assert.Equal(t, "Austin", r.City)
	assert.Equal(t, "TX", r.State)
	assert.Equal(t, "78701", r.ZipCode)
	assert.Equal(t, "456 Oak Ave", r.Street)
}

func TestMergeGoldenRecord_ConfidenceOverwriteAllFields(t *testing.T) {
	// Pre-populate all enrichment fields, then overwrite with higher confidence.
	r := &CompanyRecord{
		ServicesList:          "Old",
		ServiceArea:           "Old",
		LicensesText:          "Old",
		OwnerName:             "Old",
		CustomerTypes:         "Old",
		Differentiators:       "Old",
		ReputationSummary:     "Old",
		AcquisitionAssessment: "Old",
		KeyPeople:             "Old",
		ExecFirstName:         "Old",
		ExecLastName:          "Old",
		ExecTitle:             "Old",
		ExecLinkedIn:          "Old",
		EndMarkets:            "Old",
		LinkedInURL:           "Old",
	}

	fv := map[string]model.FieldValue{
		"services_list":          {Value: "New", Confidence: 0.9},
		"service_area":           {Value: "New", Confidence: 0.9},
		"licenses":               {Value: "New", Confidence: 0.9},
		"owner_name":             {Value: "New", Confidence: 0.9},
		"customer_types":         {Value: "New", Confidence: 0.9},
		"differentiators":        {Value: "New", Confidence: 0.9},
		"reputation_summary":     {Value: "New", Confidence: 0.9},
		"acquisition_assessment": {Value: "New", Confidence: 0.9},
		"key_people":             {Value: "New", Confidence: 0.9},
		"exec_first_name":        {Value: "New", Confidence: 0.9},
		"exec_last_name":         {Value: "New", Confidence: 0.9},
		"exec_title":             {Value: "New", Confidence: 0.9},
		"exec_linkedin":          {Value: "New", Confidence: 0.9},
		"end_markets":            {Value: "New", Confidence: 0.9},
		"linkedin_url":           {Value: "New", Confidence: 0.9},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, "New", r.ServicesList)
	assert.Equal(t, "New", r.ServiceArea)
	assert.Equal(t, "New", r.LicensesText)
	assert.Equal(t, "New", r.OwnerName)
	assert.Equal(t, "New", r.CustomerTypes)
	assert.Equal(t, "New", r.Differentiators)
	assert.Equal(t, "New", r.ReputationSummary)
	assert.Equal(t, "New", r.AcquisitionAssessment)
	assert.Equal(t, "New", r.KeyPeople)
	assert.Equal(t, "New", r.ExecFirstName)
	assert.Equal(t, "New", r.ExecLastName)
	assert.Equal(t, "New", r.ExecTitle)
	assert.Equal(t, "New", r.ExecLinkedIn)
	assert.Equal(t, "New", r.EndMarkets)
	assert.Equal(t, "New", r.LinkedInURL)
}

func TestMergeGoldenRecord_LowConfidenceNoOverwrite(t *testing.T) {
	r := &CompanyRecord{
		ServicesList:          "Existing",
		ServiceArea:           "Existing",
		LicensesText:          "Existing",
		OwnerName:             "Existing",
		CustomerTypes:         "Existing",
		Differentiators:       "Existing",
		ReputationSummary:     "Existing",
		AcquisitionAssessment: "Existing",
		KeyPeople:             "Existing",
		ExecFirstName:         "Existing",
		ExecLastName:          "Existing",
		ExecTitle:             "Existing",
		ExecLinkedIn:          "Existing",
		EndMarkets:            "Existing",
		LinkedInURL:           "Existing",
	}

	// All low confidence — should NOT overwrite existing values.
	fv := map[string]model.FieldValue{
		"services_list":          {Value: "Low", Confidence: 0.3},
		"service_area":           {Value: "Low", Confidence: 0.3},
		"licenses":               {Value: "Low", Confidence: 0.3},
		"owner_name":             {Value: "Low", Confidence: 0.3},
		"customer_types":         {Value: "Low", Confidence: 0.3},
		"differentiators":        {Value: "Low", Confidence: 0.3},
		"reputation_summary":     {Value: "Low", Confidence: 0.3},
		"acquisition_assessment": {Value: "Low", Confidence: 0.3},
		"key_people":             {Value: "Low", Confidence: 0.3},
		"exec_first_name":        {Value: "Low", Confidence: 0.3},
		"exec_last_name":         {Value: "Low", Confidence: 0.3},
		"exec_title":             {Value: "Low", Confidence: 0.3},
		"exec_linkedin":          {Value: "Low", Confidence: 0.3},
		"end_markets":            {Value: "Low", Confidence: 0.3},
		"linkedin_url":           {Value: "Low", Confidence: 0.3},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, "Existing", r.ServicesList)
	assert.Equal(t, "Existing", r.ServiceArea)
	assert.Equal(t, "Existing", r.LicensesText)
	assert.Equal(t, "Existing", r.OwnerName)
	assert.Equal(t, "Existing", r.CustomerTypes)
	assert.Equal(t, "Existing", r.Differentiators)
	assert.Equal(t, "Existing", r.ReputationSummary)
	assert.Equal(t, "Existing", r.AcquisitionAssessment)
	assert.Equal(t, "Existing", r.KeyPeople)
	assert.Equal(t, "Existing", r.ExecFirstName)
	assert.Equal(t, "Existing", r.ExecLastName)
	assert.Equal(t, "Existing", r.ExecTitle)
	assert.Equal(t, "Existing", r.ExecLinkedIn)
	assert.Equal(t, "Existing", r.EndMarkets)
	assert.Equal(t, "Existing", r.LinkedInURL)
}

func TestMergeGoldenRecord_UnmappedKey(_ *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"unknown_field_xyz": {Value: "something", Confidence: 0.9},
	}
	MergeGoldenRecord(r, fv)
	// Should not panic, just log debug.
}

func TestToInt_AllTypes(t *testing.T) {
	assert.Equal(t, 42, toInt(42))
	assert.Equal(t, 42, toInt(int64(42)))
	assert.Equal(t, 42, toInt(float64(42)))
	assert.Equal(t, 42, toInt(float32(42)))
	assert.Equal(t, 0, toInt("not a number"))
	assert.Equal(t, 0, toInt(nil))
}

func TestToInt64_AllTypes(t *testing.T) {
	assert.Equal(t, int64(42), toInt64(int64(42)))
	assert.Equal(t, int64(42), toInt64(42))
	assert.Equal(t, int64(42), toInt64(float64(42)))
	assert.Equal(t, int64(42), toInt64(float32(42)))
	assert.Equal(t, int64(0), toInt64("not a number"))
	assert.Equal(t, int64(0), toInt64(nil))
}

func TestToFloat64_AllTypes(t *testing.T) {
	assert.Equal(t, 4.5, toFloat64(4.5))
	assert.Equal(t, float64(42), toFloat64(float32(42)))
	assert.Equal(t, float64(42), toFloat64(42))
	assert.Equal(t, float64(42), toFloat64(int64(42)))
	assert.Equal(t, 0.0, toFloat64("not a number"))
	assert.Equal(t, 0.0, toFloat64(nil))
}

func TestMergeGoldenRecord_IntegerTypeVariants(t *testing.T) {
	// Test that toInt handles int, int64, float32 variants.
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"employee_count":    {Value: int64(200), Confidence: 0.8},
		"employee_estimate": {Value: int(250), Confidence: 0.7},
		"review_count":      {Value: float32(15), Confidence: 0.8},
		"year_founded":      {Value: int(1999), Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	assert.Equal(t, 200, *r.EmployeeCount)
	assert.Equal(t, 250, *r.EmployeeEstimate)
	assert.Equal(t, 15, *r.ReviewCount)
	assert.Equal(t, 1999, r.YearFounded)
}

func TestMergeGoldenRecord_ZeroNumericNoSet(t *testing.T) {
	r := &CompanyRecord{}
	fv := map[string]model.FieldValue{
		"employee_count":     {Value: float64(0), Confidence: 0.8},
		"review_count":       {Value: float64(0), Confidence: 0.8},
		"location_count":     {Value: float64(0), Confidence: 0.8},
		"employees_linkedin": {Value: float64(0), Confidence: 0.8},
		"review_rating":      {Value: float64(0), Confidence: 0.8},
		"year_founded":       {Value: float64(0), Confidence: 0.8},
		"revenue_estimate":   {Value: float64(0), Confidence: 0.8},
	}
	MergeGoldenRecord(r, fv)

	assert.Nil(t, r.EmployeeCount)
	assert.Nil(t, r.ReviewCount)
	assert.Nil(t, r.LocationCount)
	assert.Nil(t, r.EmployeesLinkedIn)
	assert.Nil(t, r.ReviewRating)
	assert.Equal(t, 0, r.YearFounded)
	assert.Nil(t, r.RevenueEstimate)
}
