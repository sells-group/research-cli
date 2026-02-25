package geocode

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTigerGeocode_Match(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError)

	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("100 S Biscayne Blvd, Miami, FL, 33131").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address"}).
				AddRow(25.772320, -80.189370, 5, "100 S Biscayne Blvd, Miami, FL 33131"),
		)

	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs(pgxmock.AnyArg(), 25.772320, -80.189370, "rooftop", 5).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	g := NewClient(mock, WithCacheEnabled(true))

	result, err := g.Geocode(context.Background(), AddressInput{
		Street:  "100 S Biscayne Blvd",
		City:    "Miami",
		State:   "FL",
		ZipCode: "33131",
	})

	require.NoError(t, err)
	assert.True(t, result.Matched)
	assert.Equal(t, "tiger", result.Source)
	assert.Equal(t, "rooftop", result.Quality)
	assert.InDelta(t, 25.772320, result.Latitude, 0.001)
	assert.InDelta(t, -80.189370, result.Longitude, 0.001)
	assert.Equal(t, 5, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerGeocode_NoMatch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError)

	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("123 Nonexistent St, Nowhere, XX, 00000").
		WillReturnError(assert.AnError)

	g := NewClient(mock, WithCacheEnabled(true))

	result, err := g.Geocode(context.Background(), AddressInput{
		Street:  "123 Nonexistent St",
		City:    "Nowhere",
		State:   "XX",
		ZipCode: "00000",
	})

	require.NoError(t, err)
	assert.False(t, result.Matched)
	assert.Equal(t, "tiger", result.Source)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerGeocode_ExceedsMaxRating(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError)

	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("123 Main St, Anytown, FL, 33101").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address"}).
				AddRow(25.0, -80.0, 60, "123 Main St, Anytown, FL 33101"),
		)

	g := NewClient(mock, WithMaxRating(50))

	result, err := g.Geocode(context.Background(), AddressInput{
		Street:  "123 Main St",
		City:    "Anytown",
		State:   "FL",
		ZipCode: "33101",
	})

	require.NoError(t, err)
	assert.False(t, result.Matched)
	assert.Equal(t, 60, result.Rating)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTigerGeocode_EmptyAddress(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	g := NewClient(mock)

	result, err := g.Geocode(context.Background(), AddressInput{})
	require.NoError(t, err)
	assert.False(t, result.Matched)
}

func TestBatchGeocode(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First address: cache miss, then geocode match.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError)
	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("100 Main St, Miami, FL, 33131").
		WillReturnRows(
			pgxmock.NewRows([]string{"lat", "lon", "rating", "matched_address"}).
				AddRow(25.77, -80.19, 3, "100 Main St, Miami, FL 33131"),
		)
	mock.ExpectExec(`INSERT INTO public.geocode_cache`).
		WithArgs(pgxmock.AnyArg(), 25.77, -80.19, "rooftop", 3).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	// Second address: cache miss, no geocode match.
	mock.ExpectQuery(`SELECT latitude, longitude, quality, rating FROM public.geocode_cache`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(assert.AnError)
	mock.ExpectQuery(`SELECT\s+ST_Y`).
		WithArgs("999 Fake Ave, Nowhere, XX, 00000").
		WillReturnError(assert.AnError)

	g := NewClient(mock, WithCacheEnabled(true))

	results, err := g.BatchGeocode(context.Background(), []AddressInput{
		{Street: "100 Main St", City: "Miami", State: "FL", ZipCode: "33131"},
		{Street: "999 Fake Ave", City: "Nowhere", State: "XX", ZipCode: "00000"},
	})

	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Matched)
	assert.False(t, results[1].Matched)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRatingToQuality(t *testing.T) {
	tests := []struct {
		rating  int
		quality string
	}{
		{0, "rooftop"},
		{5, "rooftop"},
		{9, "rooftop"},
		{10, "range"},
		{15, "range"},
		{19, "range"},
		{20, "centroid"},
		{49, "centroid"},
		{50, "approximate"},
		{100, "approximate"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.quality, ratingToQuality(tt.rating), "rating %d", tt.rating)
	}
}
