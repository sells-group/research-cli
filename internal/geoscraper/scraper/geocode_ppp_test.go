package scraper

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestGeocodePPP_Metadata(t *testing.T) {
	s := &GeocodePPP{}
	assert.Equal(t, "geocode_ppp", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.OnDemand, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestGeocodePPP_ShouldRun(t *testing.T) {
	s := &GeocodePPP{}
	assert.False(t, s.ShouldRun(fixedNow(), nil))
	now := fixedNow()
	assert.False(t, s.ShouldRun(now, &now))
}

func TestGeocodePPP_Sync(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.infrastructure").WillReturnResult(pgxmock.NewResult("UPDATE", 99))

	s := &GeocodePPP{}
	result, err := s.Sync(context.Background(), mock, nil, "")
	require.NoError(t, err)
	assert.Equal(t, int64(99), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGeocodePPP_ExecError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectExec("UPDATE geo.infrastructure").WillReturnError(assert.AnError)

	s := &GeocodePPP{}
	_, err = s.Sync(context.Background(), mock, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "geocode_ppp")
}
