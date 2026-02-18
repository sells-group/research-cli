package pipeline

import (
	"regexp"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

// --- ValidateField: email type ---

func TestValidateField_EmailType_Valid(t *testing.T) {
	field := &model.FieldMapping{Key: "email", SFField: "Email", DataType: "email"}
	answer := model.ExtractionAnswer{FieldKey: "email", Value: "user@example.com", Confidence: 0.9}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "user@example.com", fv.Value)
}

func TestValidateField_EmailType_Invalid(t *testing.T) {
	field := &model.FieldMapping{Key: "email", SFField: "Email", DataType: "email"}
	answer := model.ExtractionAnswer{FieldKey: "email", Value: "not-an-email"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

// --- ValidateField: phone type ---

func TestValidateField_PhoneType_Valid(t *testing.T) {
	field := &model.FieldMapping{Key: "phone", SFField: "Phone", DataType: "phone"}
	answer := model.ExtractionAnswer{FieldKey: "phone", Value: "+1 (555) 867-5309"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "+1 (555) 867-5309", fv.Value)
}

func TestValidateField_PhoneType_TooShort(t *testing.T) {
	field := &model.FieldMapping{Key: "phone", SFField: "Phone", DataType: "phone"}
	answer := model.ExtractionAnswer{FieldKey: "phone", Value: "123"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_PhoneType_StripsNonNumeric(t *testing.T) {
	field := &model.FieldMapping{Key: "phone", SFField: "Phone", DataType: "phone"}
	answer := model.ExtractionAnswer{FieldKey: "phone", Value: "Call us at 555.867.5309 today!"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	// Non-numeric characters (except +, spaces, dashes, parens) are stripped.
	cleaned := fv.Value.(string)
	assert.NotContains(t, cleaned, "Call")
	assert.NotContains(t, cleaned, "today")
}

// --- ValidateField: default/unknown type ---

func TestValidateField_DefaultType(t *testing.T) {
	field := &model.FieldMapping{Key: "custom", SFField: "Custom__c", DataType: "custom_type"}
	answer := model.ExtractionAnswer{FieldKey: "custom", Value: "any value", Confidence: 0.8}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "any value", fv.Value)
}

// --- ValidateField: string with invalid regex ---

func TestValidateField_StringInvalidRegex(t *testing.T) {
	field := &model.FieldMapping{
		Key:        "f",
		SFField:    "F__c",
		DataType:   "string",
		Validation: "[invalid(regex",
	}
	answer := model.ExtractionAnswer{FieldKey: "f", Value: "anything"}

	// Invalid regex pattern should skip validation and return the value.
	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "anything", fv.Value)
}

// --- toNumber: float32, int64 paths ---

func TestToNumber_Float32(t *testing.T) {
	n, ok := toNumber(float32(42.5))
	assert.True(t, ok)
	assert.Equal(t, 42, n)
}

func TestToNumber_Int64(t *testing.T) {
	n, ok := toNumber(int64(1000))
	assert.True(t, ok)
	assert.Equal(t, 1000, n)
}

func TestToNumber_UnsupportedType(t *testing.T) {
	_, ok := toNumber(true)
	assert.False(t, ok)
}

func TestToNumber_StringWithCommas(t *testing.T) {
	n, ok := toNumber("10,500")
	assert.True(t, ok)
	assert.Equal(t, 10500, n)
}

// --- toFloat: float32, int, int64 paths ---

func TestToFloat_Float32(t *testing.T) {
	f, ok := toFloat(float32(3.14))
	assert.True(t, ok)
	assert.InDelta(t, 3.14, f, 0.01)
}

func TestToFloat_Int(t *testing.T) {
	f, ok := toFloat(42)
	assert.True(t, ok)
	assert.Equal(t, 42.0, f)
}

func TestToFloat_Int64(t *testing.T) {
	f, ok := toFloat(int64(1000))
	assert.True(t, ok)
	assert.Equal(t, 1000.0, f)
}

func TestToFloat_StringWithDollar(t *testing.T) {
	f, ok := toFloat("$1,500.50")
	assert.True(t, ok)
	assert.Equal(t, 1500.50, f)
}

func TestToFloat_InvalidString(t *testing.T) {
	_, ok := toFloat("not a number")
	assert.False(t, ok)
}

func TestToFloat_UnsupportedType(t *testing.T) {
	_, ok := toFloat(true)
	assert.False(t, ok)
}

// --- toBool: float64, int paths ---

func TestToBool_Float64_True(t *testing.T) {
	b, ok := toBool(float64(1.0))
	assert.True(t, ok)
	assert.True(t, b)
}

func TestToBool_Float64_False(t *testing.T) {
	b, ok := toBool(float64(0.0))
	assert.True(t, ok)
	assert.False(t, b)
}

func TestToBool_Int_True(t *testing.T) {
	b, ok := toBool(42)
	assert.True(t, ok)
	assert.True(t, b)
}

func TestToBool_Int_False(t *testing.T) {
	b, ok := toBool(0)
	assert.True(t, ok)
	assert.False(t, b)
}

func TestToBool_String_Invalid(t *testing.T) {
	_, ok := toBool("maybe")
	assert.False(t, ok)
}

func TestToBool_UnsupportedType(t *testing.T) {
	_, ok := toBool([]int{1, 2, 3})
	assert.False(t, ok)
}

// --- Pre-compiled validation regex ---

func TestValidationRegex_ValidPattern(t *testing.T) {
	field := &model.FieldMapping{
		Key:        "state_code",
		DataType:   "string",
		Validation: `^[A-Z]{2}$`,
	}
	// Simulate what NewFieldRegistry does.
	re, err := regexp.Compile(field.Validation)
	assert.NoError(t, err)
	field.ValidationRegex = re

	fv := ValidateField(model.ExtractionAnswer{Value: "CA", Confidence: 0.9, Tier: 1, FieldKey: "state_code"}, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "CA", fv.Value)

	fv = ValidateField(model.ExtractionAnswer{Value: "California", Confidence: 0.9, Tier: 1, FieldKey: "state_code"}, field)
	assert.Nil(t, fv)
}

func TestValidationRegex_NilRegex(t *testing.T) {
	// When no validation regex is set, all strings should pass.
	field := &model.FieldMapping{Key: "name", DataType: "string"}
	fv := ValidateField(model.ExtractionAnswer{Value: "anything", Confidence: 0.9, Tier: 1, FieldKey: "name"}, field)
	assert.NotNil(t, fv)
}

// --- MergeAnswers: all three tiers ---

func TestMergeAnswers_AllThreeTiers(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q2", FieldKey: "industry", Value: "Technology Services", Confidence: 0.5, Tier: 2},
	}
	t3 := []model.ExtractionAnswer{
		{QuestionID: "q3", FieldKey: "industry", Value: "Enterprise Technology", Confidence: 0.95, Tier: 3},
	}

	merged := MergeAnswers(t1, t2, t3)
	assert.Len(t, merged, 1)
	assert.Equal(t, "Enterprise Technology", merged[0].Value)
	assert.Equal(t, 3, merged[0].Tier)
}

func TestMergeAnswers_AllNil(t *testing.T) {
	merged := MergeAnswers(nil, nil, nil)
	assert.Empty(t, merged)
}

// --- ValidateField: HTTP URL ---

func TestValidateField_URLType_HTTP(t *testing.T) {
	field := &model.FieldMapping{Key: "website", SFField: "Website", DataType: "url"}
	answer := model.ExtractionAnswer{FieldKey: "website", Value: "http://example.com"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "http://example.com", fv.Value)
}

// --- ValidateField: float/double type ---

func TestValidateField_FloatType_Valid(t *testing.T) {
	field := &model.FieldMapping{Key: "amount", SFField: "Amount__c", DataType: "float"}
	answer := model.ExtractionAnswer{FieldKey: "amount", Value: float64(99.99), Confidence: 0.8}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, 99.99, fv.Value)
}

func TestValidateField_DecimalType_StringInput(t *testing.T) {
	field := &model.FieldMapping{Key: "amount", SFField: "Amount__c", DataType: "decimal"}
	answer := model.ExtractionAnswer{FieldKey: "amount", Value: "1,234.56"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, 1234.56, fv.Value)
}

func TestValidateField_FloatType_Invalid(t *testing.T) {
	field := &model.FieldMapping{Key: "amount", SFField: "Amount__c", DataType: "float"}
	answer := model.ExtractionAnswer{FieldKey: "amount", Value: "not-a-number"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

// --- ValidateField: integer alias ---

func TestValidateField_IntegerAlias(t *testing.T) {
	field := &model.FieldMapping{Key: "count", SFField: "Count__c", DataType: "integer"}
	answer := model.ExtractionAnswer{FieldKey: "count", Value: float64(50)}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, 50, fv.Value)
}

func TestValidateField_IntAlias(t *testing.T) {
	field := &model.FieldMapping{Key: "count", SFField: "Count__c", DataType: "int"}
	answer := model.ExtractionAnswer{FieldKey: "count", Value: "100"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, 100, fv.Value)
}

// --- BuildFieldValues: unknown field key ---

func TestBuildFieldValues_UnknownFieldSkipped(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "known", SFField: "Known__c", DataType: "string"},
	})

	answers := []model.ExtractionAnswer{
		{FieldKey: "unknown_key", Value: "value", Confidence: 0.9},
		{FieldKey: "known", Value: "good", Confidence: 0.8},
	}

	fvs := BuildFieldValues(answers, fields)
	assert.Len(t, fvs, 1)
	assert.Equal(t, "good", fvs["known"].Value)
}

// --- BuildFieldValues: validation failure ---

func TestBuildFieldValues_ValidationFailureSkipped(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "email", SFField: "Email", DataType: "email"},
	})

	answers := []model.ExtractionAnswer{
		{FieldKey: "email", Value: "not-an-email", Confidence: 0.9},
	}

	fvs := BuildFieldValues(answers, fields)
	assert.Len(t, fvs, 0)
}
