package company

import (
	"github.com/sells-group/research-cli/internal/model"
	"go.uber.org/zap"
)

// MergeGoldenRecord updates a CompanyRecord from enrichment field values.
// It applies source-priority field merge: only overwrites a field if the
// new value has higher confidence or the existing field is empty.
func MergeGoldenRecord(record *CompanyRecord, fieldValues map[string]model.FieldValue) {
	for key, fv := range fieldValues {
		if fv.Value == nil {
			continue
		}
		applyField(record, key, fv)
	}
}

// applyField maps a FieldValue to the corresponding CompanyRecord field.
func applyField(r *CompanyRecord, key string, fv model.FieldValue) {
	s, _ := fv.Value.(string)

	switch key {
	case "name", "company_name":
		if r.Name == "" || fv.Confidence > 0.7 {
			r.Name = s
		}
	case "legal_name":
		if r.LegalName == "" || fv.Confidence > 0.8 {
			r.LegalName = s
		}
	case "description", "company_description":
		if r.Description == "" || fv.Confidence > 0.6 {
			r.Description = s
		}
	case "naics_code":
		if r.NAICSCode == "" || fv.Confidence > 0.7 {
			r.NAICSCode = s
		}
	case "sic_code":
		if r.SICCode == "" || fv.Confidence > 0.7 {
			r.SICCode = s
		}
	case "business_model":
		if r.BusinessModel == "" || fv.Confidence > 0.6 {
			r.BusinessModel = s
		}
	case "year_founded":
		if n := toInt(fv.Value); n > 0 && (r.YearFounded == 0 || fv.Confidence > 0.7) {
			r.YearFounded = n
		}
	case "ownership_type":
		if r.OwnershipType == "" || fv.Confidence > 0.6 {
			r.OwnershipType = s
		}
	case "phone":
		if r.Phone == "" || fv.Confidence > 0.7 {
			r.Phone = s
		}
	case "email":
		if r.Email == "" || fv.Confidence > 0.7 {
			r.Email = s
		}
	case "employee_count":
		if n := toInt(fv.Value); n > 0 {
			r.EmployeeCount = &n
		}
	case "employee_estimate":
		if n := toInt(fv.Value); n > 0 {
			r.EmployeeEstimate = &n
		}
	case "revenue_estimate":
		if n := toInt64(fv.Value); n > 0 {
			r.RevenueEstimate = &n
		}
	case "revenue_range":
		if r.RevenueRange == "" || fv.Confidence > 0.6 {
			r.RevenueRange = s
		}
	case "revenue_confidence":
		if f := toFloat64(fv.Value); f > 0 {
			r.RevenueConfidence = &f
		}
	case "street", "address_street":
		if r.Street == "" || fv.Confidence > 0.7 {
			r.Street = s
		}
	case "city":
		if r.City == "" || fv.Confidence > 0.7 {
			r.City = s
		}
	case "state":
		if r.State == "" || fv.Confidence > 0.7 {
			r.State = s
		}
	case "zip_code":
		if r.ZipCode == "" || fv.Confidence > 0.7 {
			r.ZipCode = s
		}
	case "website":
		if r.Website == "" || fv.Confidence > 0.8 {
			r.Website = s
		}

	// Enrichment detail fields (raw + canonical aliases).
	case "services_list", "service_mix":
		if r.ServicesList == "" || fv.Confidence > 0.6 {
			r.ServicesList = s
		}
	case "service_area":
		if r.ServiceArea == "" || fv.Confidence > 0.6 {
			r.ServiceArea = s
		}
	case "licenses":
		if r.LicensesText == "" || fv.Confidence > 0.6 {
			r.LicensesText = s
		}
	case "owner_name":
		if r.OwnerName == "" || fv.Confidence > 0.7 {
			r.OwnerName = s
		}
	case "customer_types":
		if r.CustomerTypes == "" || fv.Confidence > 0.6 {
			r.CustomerTypes = s
		}
	case "differentiators":
		if r.Differentiators == "" || fv.Confidence > 0.6 {
			r.Differentiators = s
		}
	case "reputation_summary":
		if r.ReputationSummary == "" || fv.Confidence > 0.6 {
			r.ReputationSummary = s
		}
	case "acquisition_assessment":
		if r.AcquisitionAssessment == "" || fv.Confidence > 0.6 {
			r.AcquisitionAssessment = s
		}
	case "key_people":
		if r.KeyPeople == "" || fv.Confidence > 0.6 {
			r.KeyPeople = s
		}
	case "exec_first_name":
		if r.ExecFirstName == "" || fv.Confidence > 0.7 {
			r.ExecFirstName = s
		}
	case "exec_last_name":
		if r.ExecLastName == "" || fv.Confidence > 0.7 {
			r.ExecLastName = s
		}
	case "exec_title":
		if r.ExecTitle == "" || fv.Confidence > 0.6 {
			r.ExecTitle = s
		}
	case "exec_linkedin":
		if r.ExecLinkedIn == "" || fv.Confidence > 0.7 {
			r.ExecLinkedIn = s
		}
	case "review_count":
		if n := toInt(fv.Value); n > 0 {
			r.ReviewCount = &n
		}
	case "review_rating":
		if f := toFloat64(fv.Value); f > 0 {
			r.ReviewRating = &f
		}
	case "employees_linkedin":
		if n := toInt(fv.Value); n > 0 {
			r.EmployeesLinkedIn = &n
		}
	case "location_count", "locations":
		if n := toInt(fv.Value); n > 0 {
			r.LocationCount = &n
		}
	case "end_markets":
		if r.EndMarkets == "" || fv.Confidence > 0.6 {
			r.EndMarkets = s
		}
	case "linkedin_url":
		if r.LinkedInURL == "" || fv.Confidence > 0.7 {
			r.LinkedInURL = s
		}

	default:
		zap.L().Debug("merge: unmapped field key", zap.String("key", key))
	}
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case float32:
		return int(n)
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}
