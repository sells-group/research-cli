package store

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/model"
)

type invalidatingStore struct {
	Store
	cache apicache.Cache
}

// WithAPICache wraps a store and invalidates cached run-domain reads after writes.
func WithAPICache(next Store, cache apicache.Cache) Store {
	if next == nil || cache == nil {
		return next
	}
	return &invalidatingStore{
		Store: next,
		cache: cache,
	}
}

func (s *invalidatingStore) invalidateRuns() {
	_ = s.cache.DeleteDomains(apicache.DomainRuns)
}

// CreateRun implements Store.
func (s *invalidatingStore) CreateRun(ctx context.Context, company model.Company) (*model.Run, error) {
	run, err := s.Store.CreateRun(ctx, company)
	if err == nil {
		s.invalidateRuns()
	}
	return run, err
}

// UpdateRunStatus implements Store.
func (s *invalidatingStore) UpdateRunStatus(ctx context.Context, runID string, status model.RunStatus) error {
	err := s.Store.UpdateRunStatus(ctx, runID, status)
	if err == nil {
		s.invalidateRuns()
	}
	return err
}

// UpdateRunResult implements Store.
func (s *invalidatingStore) UpdateRunResult(ctx context.Context, runID string, result *model.RunResult) error {
	err := s.Store.UpdateRunResult(ctx, runID, result)
	if err == nil {
		s.invalidateRuns()
	}
	return err
}

// FailRun implements Store.
func (s *invalidatingStore) FailRun(ctx context.Context, runID string, runErr *model.RunError) error {
	err := s.Store.FailRun(ctx, runID, runErr)
	if err == nil {
		s.invalidateRuns()
	}
	return err
}

// Close implements Store.
func (s *invalidatingStore) Close() error {
	var closeErr error
	if err := s.Store.Close(); err != nil {
		closeErr = eris.Wrap(err, "close wrapped store")
	}
	if err := s.cache.Close(); err != nil && closeErr == nil {
		closeErr = eris.Wrap(err, "close api cache")
	}
	return closeErr
}
