package registry

import (
	"encoding/json"
	"os"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/model"
)

// LoadQuestionsFromFile reads a JSON array of model.Question from the given path.
func LoadQuestionsFromFile(path string) ([]model.Question, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, eris.Wrap(err, "registry: read questions fixture")
	}

	var questions []model.Question
	if err := json.Unmarshal(data, &questions); err != nil {
		return nil, eris.Wrap(err, "registry: unmarshal questions fixture")
	}

	return questions, nil
}

// LoadFieldsFromFile reads a JSON array of model.FieldMapping from the given
// path and returns an indexed FieldRegistry.
func LoadFieldsFromFile(path string) (*model.FieldRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, eris.Wrap(err, "registry: read fields fixture")
	}

	var fields []model.FieldMapping
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, eris.Wrap(err, "registry: unmarshal fields fixture")
	}

	return model.NewFieldRegistry(fields), nil
}
