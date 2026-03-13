package scraper

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestImportPPP_Metadata(t *testing.T) {
	s := &ImportPPP{}
	assert.Equal(t, "import_ppp", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.OnDemand, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestImportPPP_ShouldRun(t *testing.T) {
	s := &ImportPPP{}
	assert.False(t, s.ShouldRun(fixedNow(), nil))
	now := fixedNow()
	assert.False(t, s.ShouldRun(now, &now))
}

func TestImportPPP_Sync(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.infrastructure").WillReturnResult(pgxmock.NewResult("INSERT", 42))

	s := &ImportPPP{}
	result, err := s.Sync(context.Background(), mock, nil, "")
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestImportPPP_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.infrastructure").WillReturnError(assert.AnError)

	s := &ImportPPP{}
	_, err = s.Sync(context.Background(), mock, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "import_ppp")
}
