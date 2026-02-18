package pipeline

import (
	"regexp"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestMergeAnswers_HigherTierWins(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Technology Services", Confidence: 0.5, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	assert.Equal(t, "Technology Services", merged[0].Value)
	assert.Equal(t, 2, merged[0].Tier)
}

func TestMergeAnswers_SameTierHigherConfidenceWins(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Old Answer", Confidence: 0.3, Tier: 1},
		{QuestionID: "q2", FieldKey: "industry", Value: "Better Answer", Confidence: 0.9, Tier: 1},
	}

	merged := MergeAnswers(t1, nil, nil)
	assert.Len(t, merged, 1)
	assert.Equal(t, "Better Answer", merged[0].Value)
	assert.Equal(t, 0.9, merged[0].Confidence)
}

func TestMergeAnswers_LowConfidenceHigherTierIgnored(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "revenue", Value: "$10M", Confidence: 0.8, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "revenue", Value: "$15M", Confidence: 0.2, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	// T2 has low confidence (< 0.3), so T1 should win.
	assert.Equal(t, "$10M", merged[0].Value)
}

func TestMergeAnswers_NullT1_AnyT2Wins(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "year_founded", Value: nil, Confidence: 0.1, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "year_founded", Value: 2011, Confidence: 0.2, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 1)
	// T2 should win even with low confidence because T1 is null.
	assert.Equal(t, 2011, merged[0].Value)
	assert.Equal(t, 2, merged[0].Tier)
}

func TestMergeAnswers_EmptyFieldKeySkipped(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "", Value: "no key", Confidence: 0.9, Tier: 1},
	}

	merged := MergeAnswers(t1, nil, nil)
	assert.Len(t, merged, 0)
}

func TestMergeAnswers_MultipleFields(t *testing.T) {
	t1 := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.8, Tier: 1},
		{QuestionID: "q2", FieldKey: "revenue", Value: "$5M", Confidence: 0.6, Tier: 1},
	}
	t2 := []model.ExtractionAnswer{
		{QuestionID: "q3", FieldKey: "employees", Value: 100, Confidence: 0.9, Tier: 2},
	}

	merged := MergeAnswers(t1, t2, nil)
	assert.Len(t, merged, 3)
}

func TestValidateField_StringType(t *testing.T) {
	field := &model.FieldMapping{
		Key:      "industry",
		SFField:  "Industry",
		DataType: "string",
	}
	answer := model.ExtractionAnswer{
		FieldKey:   "industry",
		Value:      "Technology",
		Confidence: 0.9,
		SourceURL:  "https://acme.com",
		Tier:       1,
	}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "Technology", fv.Value)
	assert.Equal(t, "Industry", fv.SFField)
}

func TestValidateField_StringMaxLength(t *testing.T) {
	field := &model.FieldMapping{
		Key:       "name",
		SFField:   "Name",
		DataType:  "string",
		MaxLength: 5,
	}
	answer := model.ExtractionAnswer{
		FieldKey: "name",
		Value:    "Long Company Name",
	}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "Long ", fv.Value)
}

func TestValidateField_NumberType(t *testing.T) {
	field := &model.FieldMapping{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"}

	tests := []struct {
		name  string
		value any
		want  int
	}{
		{"float64", float64(100), 100},
		{"int", 50, 50},
		{"string", "1,500", 1500},
		{"string_decimal", "99.5", 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			answer := model.ExtractionAnswer{FieldKey: "employees", Value: tt.value}
			fv := ValidateField(answer, field)
			assert.NotNil(t, fv)
			assert.Equal(t, tt.want, fv.Value)
		})
	}
}

func TestValidateField_NumberType_Invalid(t *testing.T) {
	field := &model.FieldMapping{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"}
	answer := model.ExtractionAnswer{FieldKey: "employees", Value: "not-a-number"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_BooleanType(t *testing.T) {
	field := &model.FieldMapping{Key: "active", SFField: "IsActive__c", DataType: "boolean"}

	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{"true_bool", true, true},
		{"false_bool", false, false},
		{"yes_string", "yes", true},
		{"no_string", "no", false},
		{"1_string", "1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			answer := model.ExtractionAnswer{FieldKey: "active", Value: tt.value}
			fv := ValidateField(answer, field)
			assert.NotNil(t, fv)
			assert.Equal(t, tt.want, fv.Value)
		})
	}
}

func TestValidateField_URLType_Invalid(t *testing.T) {
	field := &model.FieldMapping{Key: "website", SFField: "Website", DataType: "url"}
	answer := model.ExtractionAnswer{FieldKey: "website", Value: "not-a-url"}

	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_URLType_Valid(t *testing.T) {
	field := &model.FieldMapping{Key: "website", SFField: "Website", DataType: "url"}
	answer := model.ExtractionAnswer{FieldKey: "website", Value: "https://acme.com"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, "https://acme.com", fv.Value)
}

func TestValidateField_NilField(t *testing.T) {
	answer := model.ExtractionAnswer{FieldKey: "f", Value: "v"}
	fv := ValidateField(answer, nil)
	assert.Nil(t, fv)
}

func TestValidateField_NilValue(t *testing.T) {
	field := &model.FieldMapping{Key: "f", DataType: "string"}
	answer := model.ExtractionAnswer{FieldKey: "f", Value: nil}
	fv := ValidateField(answer, field)
	assert.Nil(t, fv)
}

func TestValidateField_FloatType(t *testing.T) {
	field := &model.FieldMapping{Key: "revenue", SFField: "AnnualRevenue", DataType: "currency"}
	answer := model.ExtractionAnswer{FieldKey: "revenue", Value: "$1,500,000"}

	fv := ValidateField(answer, field)
	assert.NotNil(t, fv)
	assert.Equal(t, 1500000.0, fv.Value)
}

func TestValidateField_ValidationRegex(t *testing.T) {
	field := &model.FieldMapping{
		Key:             "state",
		SFField:         "BillingState",
		DataType:        "string",
		Validation:      `^[A-Z]{2}$`,
		ValidationRegex: regexp.MustCompile(`^[A-Z]{2}$`),
	}

	t.Run("valid", func(t *testing.T) {
		answer := model.ExtractionAnswer{FieldKey: "state", Value: "CA"}
		fv := ValidateField(answer, field)
		assert.NotNil(t, fv)
	})

	t.Run("invalid", func(t *testing.T) {
		answer := model.ExtractionAnswer{FieldKey: "state", Value: "California"}
		fv := ValidateField(answer, field)
		assert.Nil(t, fv)
	})
}

func TestBuildFieldValues(t *testing.T) {
	fields := model.NewFieldRegistry([]model.FieldMapping{
		{Key: "industry", SFField: "Industry", DataType: "string"},
		{Key: "employees", SFField: "NumberOfEmployees", DataType: "number"},
		{Key: "missing", SFField: "Missing__c", DataType: "string"},
	})

	answers := []model.ExtractionAnswer{
		{QuestionID: "q1", FieldKey: "industry", Value: "Tech", Confidence: 0.9, Tier: 1},
		{QuestionID: "q2", FieldKey: "employees", Value: float64(100), Confidence: 0.8, Tier: 1},
		{QuestionID: "q3", FieldKey: "unknown_field", Value: "ignore", Confidence: 0.7, Tier: 1},
	}

	fvs := BuildFieldValues(answers, fields)
	assert.Len(t, fvs, 2)
	assert.Equal(t, "Tech", fvs["industry"].Value)
	assert.Equal(t, 100, fvs["employees"].Value)
}
