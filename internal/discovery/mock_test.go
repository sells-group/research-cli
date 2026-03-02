package discovery

import (
	"context"

	"github.com/sells-group/research-cli/pkg/anthropic"
	"github.com/sells-group/research-cli/pkg/google"
)

// mockStore implements Store for testing.
type mockStore struct {
	borrowers           []PPPBorrower
	createdRunID        string
	insertedCandidates  []Candidate
	bulkInsertCalls     int
	candidates          []Candidate
	domainExistsResults map[string]bool
	placeExistsResults  map[string]bool
	unsearchedCells     []GridCell
	updatedCellIDs      []int64
	completedRuns       []string
	failedRuns          []string
	disqualifiedIDs     []int64
	updatedScores       map[int64]map[string]float64
	promotedIDs         []int64
}

func (m *mockStore) CreateRun(_ context.Context, _ RunConfig) (string, error) {
	return m.createdRunID, nil
}

func (m *mockStore) CompleteRun(_ context.Context, runID string, _ *RunResult) error {
	m.completedRuns = append(m.completedRuns, runID)
	return nil
}

func (m *mockStore) FailRun(_ context.Context, runID string, _ string) error {
	m.failedRuns = append(m.failedRuns, runID)
	return nil
}

func (m *mockStore) BulkInsertCandidates(_ context.Context, candidates []Candidate) (int64, error) {
	m.bulkInsertCalls++
	m.insertedCandidates = append(m.insertedCandidates, candidates...)
	return int64(len(candidates)), nil
}

func (m *mockStore) UpdateCandidateScore(_ context.Context, id int64, tier string, score float64) error {
	if m.updatedScores == nil {
		m.updatedScores = make(map[int64]map[string]float64)
	}
	if m.updatedScores[id] == nil {
		m.updatedScores[id] = make(map[string]float64)
	}
	m.updatedScores[id][tier] = score
	return nil
}

func (m *mockStore) DisqualifyCandidate(_ context.Context, id int64, _ string) error {
	m.disqualifiedIDs = append(m.disqualifiedIDs, id)
	return nil
}

func (m *mockStore) ListCandidates(_ context.Context, _ string, _ ListOpts) ([]Candidate, error) {
	return m.candidates, nil
}

func (m *mockStore) MarkPromoted(_ context.Context, ids []int64) error {
	m.promotedIDs = append(m.promotedIDs, ids...)
	return nil
}

func (m *mockStore) FindNewPPPBorrowers(_ context.Context, _ []string, _ []string, _ float64, _ int) ([]PPPBorrower, error) {
	return m.borrowers, nil
}

func (m *mockStore) PlaceIDExists(_ context.Context, placeID string) (bool, error) {
	if m.placeExistsResults != nil {
		return m.placeExistsResults[placeID], nil
	}
	return false, nil
}

func (m *mockStore) DomainExists(_ context.Context, domain string) (bool, error) {
	if m.domainExistsResults != nil {
		return m.domainExistsResults[domain], nil
	}
	return false, nil
}

func (m *mockStore) GetUnsearchedCells(_ context.Context, _ string, _ int) ([]GridCell, error) {
	return m.unsearchedCells, nil
}

func (m *mockStore) UpdateCellSearched(_ context.Context, cellID int64, _ int) error {
	m.updatedCellIDs = append(m.updatedCellIDs, cellID)
	return nil
}

// mockGoogleClient implements google.Client for testing.
type mockGoogleClient struct {
	responses map[string]*google.DiscoverySearchResponse
	callCount int
}

func (m *mockGoogleClient) TextSearch(_ context.Context, _ string) (*google.TextSearchResponse, error) {
	return &google.TextSearchResponse{}, nil
}

func (m *mockGoogleClient) DiscoverySearch(_ context.Context, req google.DiscoverySearchRequest) (*google.DiscoverySearchResponse, error) {
	m.callCount++
	if m.responses != nil {
		if resp, ok := m.responses[req.TextQuery]; ok {
			return resp, nil
		}
	}
	return &google.DiscoverySearchResponse{}, nil
}

// mockAnthropicClient implements anthropic.Client for testing.
type mockAnthropicClient struct {
	response *anthropic.MessageResponse
	err      error
}

func (m *mockAnthropicClient) CreateMessage(_ context.Context, _ anthropic.MessageRequest) (*anthropic.MessageResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

func (m *mockAnthropicClient) CreateBatch(_ context.Context, _ anthropic.BatchRequest) (*anthropic.BatchResponse, error) {
	return nil, nil
}

func (m *mockAnthropicClient) GetBatch(_ context.Context, _ string) (*anthropic.BatchResponse, error) {
	return nil, nil
}

func (m *mockAnthropicClient) GetBatchResults(_ context.Context, _ string) (anthropic.BatchResultIterator, error) {
	return nil, nil
}
