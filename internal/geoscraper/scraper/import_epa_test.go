package scraper

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestImportEPA_Metadata(t *testing.T) {
	s := &ImportEPA{}
	assert.Equal(t, "import_epa", s.Name())
	assert.Equal(t, "geo.epa_sites", s.Table())
	assert.Equal(t, geoscraper.OnDemand, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestImportEPA_ShouldRun(t *testing.T) {
	s := &ImportEPA{}
	assert.False(t, s.ShouldRun(fixedNow(), nil))
	now := fixedNow()
	assert.False(t, s.ShouldRun(now, &now))
}

func TestImportEPA_Sync(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.epa_sites").WillReturnResult(pgxmock.NewResult("INSERT", 8700))

	s := &ImportEPA{}
	result, err := s.Sync(context.Background(), mock, nil, "")
	require.NoError(t, err)
	assert.Equal(t, int64(8700), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestImportEPA_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.epa_sites").WillReturnError(assert.AnError)

	s := &ImportEPA{}
	_, err = s.Sync(context.Background(), mock, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "import_epa")
}
