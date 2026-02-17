package registry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sells-group/research-cli/internal/model"
)

func TestLoadQuestionsFromFile(t *testing.T) {
	// Create a temp fixture file.
	questions := []model.Question{
		{ID: "q1", Text: "What is the company name?", Tier: 1, FieldKey: "name", Status: "Active"},
		{ID: "q2", Text: "What year founded?", Tier: 1, FieldKey: "year_founded", Status: "Active"},
	}
	data, err := json.Marshal(questions)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "questions.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := LoadQuestionsFromFile(path)
	if err != nil {
		t.Fatalf("LoadQuestionsFromFile() error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 questions, got %d", len(got))
	}
	if got[0].ID != "q1" {
		t.Errorf("expected question ID q1, got %s", got[0].ID)
	}
	if got[1].FieldKey != "year_founded" {
		t.Errorf("expected field_key year_founded, got %s", got[1].FieldKey)
	}
}

func TestLoadQuestionsFromFile_NotFound(t *testing.T) {
	_, err := LoadQuestionsFromFile("/nonexistent/questions.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadQuestionsFromFile_MalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadQuestionsFromFile(path)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestLoadFieldsFromFile(t *testing.T) {
	fields := []model.FieldMapping{
		{ID: "f1", Key: "name", SFField: "Name", DataType: "string", Status: "Active"},
		{ID: "f2", Key: "phone", SFField: "Phone", DataType: "phone", Required: true, Status: "Active"},
	}
	data, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "fields.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	reg, err := LoadFieldsFromFile(path)
	if err != nil {
		t.Fatalf("LoadFieldsFromFile() error: %v", err)
	}

	if len(reg.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(reg.Fields))
	}

	// Test indexed lookups.
	if f := reg.ByKey("phone"); f == nil {
		t.Error("expected ByKey('phone') to return a mapping")
	}
	if f := reg.BySFName("Phone"); f == nil {
		t.Error("expected BySFName('Phone') to return a mapping")
	}
	if len(reg.Required()) != 1 {
		t.Errorf("expected 1 required field, got %d", len(reg.Required()))
	}
}

func TestLoadFieldsFromFile_NotFound(t *testing.T) {
	_, err := LoadFieldsFromFile("/nonexistent/fields.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadFieldsFromFile_MalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("[{bad}]"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadFieldsFromFile(path)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

// TestLoadFixtures_RealFiles loads the actual testdata fixtures to verify format.
func TestLoadFixtures_RealFiles(t *testing.T) {
	// Find testdata relative to project root.
	qPath := filepath.Join("..", "..", "testdata", "questions.json")
	if _, err := os.Stat(qPath); os.IsNotExist(err) {
		t.Skip("testdata/questions.json not found, skipping")
	}

	questions, err := LoadQuestionsFromFile(qPath)
	if err != nil {
		t.Fatalf("LoadQuestionsFromFile() error: %v", err)
	}
	if len(questions) == 0 {
		t.Error("expected at least one question from fixture")
	}

	fPath := filepath.Join("..", "..", "testdata", "fields.json")
	fields, err := LoadFieldsFromFile(fPath)
	if err != nil {
		t.Fatalf("LoadFieldsFromFile() error: %v", err)
	}
	if len(fields.Fields) == 0 {
		t.Error("expected at least one field from fixture")
	}

	// Verify every question's field_key has a matching field mapping.
	for _, q := range questions {
		if f := fields.ByKey(q.FieldKey); f == nil {
			t.Errorf("question %s has field_key %q with no matching field mapping", q.ID, q.FieldKey)
		}
	}
}
