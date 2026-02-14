package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/ppp"
	pppmocks "github.com/sells-group/research-cli/pkg/ppp/mocks"
)

func TestParseLocation(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedState string
		expectedCity  string
	}{
		{
			name:          "city and state",
			input:         "Austin, TX",
			expectedState: "TX",
			expectedCity:  "Austin",
		},
		{
			name:          "state only",
			input:         "TX",
			expectedState: "TX",
			expectedCity:  "",
		},
		{
			name:          "multi-word city",
			input:         "New York, NY",
			expectedState: "NY",
			expectedCity:  "New York",
		},
		{
			name:          "empty string",
			input:         "",
			expectedState: "",
			expectedCity:  "",
		},
		{
			name:          "full state name rejected",
			input:         "Austin, Texas",
			expectedState: "",
			expectedCity:  "Austin",
		},
		{
			name:          "whitespace handling",
			input:         "  Dallas ,  TX  ",
			expectedState: "TX",
			expectedCity:  "Dallas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, city := parseLocation(tt.input)
			assert.Equal(t, tt.expectedState, state)
			assert.Equal(t, tt.expectedCity, city)
		})
	}
}

func TestPPPPhase_NilClient(t *testing.T) {
	company := model.Company{Name: "Acme Corp", Location: "Austin, TX"}
	matches, err := PPPPhase(context.Background(), company, nil)
	assert.NoError(t, err)
	assert.Nil(t, matches)
}

func TestPPPPhase_EmptyLocation(t *testing.T) {
	q := pppmocks.NewMockQuerier(t)
	// FindLoans should not be called when location is empty.

	company := model.Company{Name: "Acme Corp", Location: ""}
	matches, err := PPPPhase(context.Background(), company, q)
	assert.NoError(t, err)
	assert.Nil(t, matches)
}

func TestPPPPhase_NoStateInLocation(t *testing.T) {
	q := pppmocks.NewMockQuerier(t)
	// FindLoans should not be called when state cannot be parsed.

	company := model.Company{Name: "Acme Corp", Location: "Austin, Texas"}
	matches, err := PPPPhase(context.Background(), company, q)
	assert.NoError(t, err)
	assert.Nil(t, matches)
}

func TestPPPPhase_SuccessfulLookup(t *testing.T) {
	expected := []ppp.LoanMatch{
		{
			LoanNumber:      12345,
			BorrowerName:    "ACME CORP",
			BorrowerState:   "TX",
			CurrentApproval: 150000,
			MatchTier:       1,
			MatchScore:      1.0,
		},
	}

	q := pppmocks.NewMockQuerier(t)
	q.On("FindLoans", mock.Anything, "Acme Corp", "TX", "Austin").Return(expected, nil)

	company := model.Company{Name: "Acme Corp", Location: "Austin, TX"}
	matches, err := PPPPhase(context.Background(), company, q)
	assert.NoError(t, err)
	assert.Equal(t, expected, matches)
}

func TestPPPPhase_NoMatches(t *testing.T) {
	q := pppmocks.NewMockQuerier(t)
	q.On("FindLoans", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	company := model.Company{Name: "Unknown Corp", Location: "Austin, TX"}
	matches, err := PPPPhase(context.Background(), company, q)
	assert.NoError(t, err)
	assert.Nil(t, matches)
}

func TestPPPPhase_ErrorPropagation(t *testing.T) {
	q := pppmocks.NewMockQuerier(t)
	q.On("FindLoans", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("connection refused"))

	company := model.Company{Name: "Acme Corp", Location: "Austin, TX"}
	matches, err := PPPPhase(context.Background(), company, q)
	assert.Error(t, err)
	assert.Nil(t, matches)
	assert.Contains(t, err.Error(), "ppp: find loans")
}

func TestPPPPhase_StateOnlyLocation(t *testing.T) {
	q := pppmocks.NewMockQuerier(t)
	q.On("FindLoans", mock.Anything, "Acme Corp", "TX", "").Return(nil, nil)

	company := model.Company{Name: "Acme Corp", Location: "TX"}
	matches, err := PPPPhase(context.Background(), company, q)
	assert.NoError(t, err)
	assert.Nil(t, matches)
}
