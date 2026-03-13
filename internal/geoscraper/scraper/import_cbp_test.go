package scraper

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestImportCBP_Metadata(t *testing.T) {
	s := &ImportCBP{}
	assert.Equal(t, "import_cbp", s.Name())
	assert.Equal(t, "geo.cbp_summary", s.Table())
	assert.Equal(t, geoscraper.OnDemand, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestImportCBP_ShouldRun(t *testing.T) {
	s := &ImportCBP{}
	assert.False(t, s.ShouldRun(fixedNow(), nil))
	now := fixedNow()
	assert.False(t, s.ShouldRun(now, &now))
}

func TestImportCBP_Sync(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.cbp_summary").WillReturnResult(pgxmock.NewResult("INSERT", 1500))

	s := &ImportCBP{}
	result, err := s.Sync(context.Background(), mock, nil, "")
	require.NoError(t, err)
	assert.Equal(t, int64(1500), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestImportCBP_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.cbp_summary").WillReturnError(assert.AnError)

	s := &ImportCBP{}
	_, err = s.Sync(context.Background(), mock, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "import_cbp")
}
