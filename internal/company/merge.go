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
