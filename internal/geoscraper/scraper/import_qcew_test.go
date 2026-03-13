package scraper

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestImportQCEW_Metadata(t *testing.T) {
	s := &ImportQCEW{}
	assert.Equal(t, "import_qcew", s.Name())
	assert.Equal(t, "geo.qcew_summary", s.Table())
	assert.Equal(t, geoscraper.OnDemand, s.Category())
	assert.Equal(t, geoscraper.Quarterly, s.Cadence())
}

func TestImportQCEW_ShouldRun(t *testing.T) {
	s := &ImportQCEW{}
	assert.False(t, s.ShouldRun(fixedNow(), nil))
	now := fixedNow()
	assert.False(t, s.ShouldRun(now, &now))
}

func TestImportQCEW_Sync(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.qcew_summary").WillReturnResult(pgxmock.NewResult("INSERT", 3200))

	s := &ImportQCEW{}
	result, err := s.Sync(context.Background(), mock, nil, "")
	require.NoError(t, err)
	assert.Equal(t, int64(3200), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestImportQCEW_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("INSERT INTO geo.qcew_summary").WillReturnError(assert.AnError)

	s := &ImportQCEW{}
	_, err = s.Sync(context.Background(), mock, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "import_qcew")
}
