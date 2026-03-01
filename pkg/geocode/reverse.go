package geocode

import (
	"context"
	"database/sql"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// ReverseResult holds the result of a reverse geocode operation.
type ReverseResult struct {
	Street     string `json:"street"`
	City       string `json:"city"`
	State      string `json:"state"`
	ZipCode    string `json:"zip_code"`
	CountyFIPS string `json:"county_fips"`
	Rating     int    `json:"rating"`
}

// ReverseGeocode converts a lat/lng to a street address using PostGIS TIGER data.
func ReverseGeocode(ctx context.Context, pool db.Pool, lat, lng float64) (*ReverseResult, error) {
	var fullAddr sql.NullString
	var state sql.NullString
	var zip sql.NullString
	var countyFIPS sql.NullString
	var rating int

	err := pool.QueryRow(ctx, `
		SELECT
			pprint_addy(addy),
			(addy).stateusps,
			(addy).zip,
			(addy).statefp || (addy).countyfp,
			rating
		FROM reverse_geocode(ST_SetSRID(ST_MakePoint($1, $2), 4326), 1)`,
		lng, lat,
	).Scan(&fullAddr, &state, &zip, &countyFIPS, &rating)
	if err != nil {
		zap.L().Debug("reverse geocode: no result",
			zap.Float64("lat", lat),
			zap.Float64("lng", lng),
			zap.Error(err),
		)
		return nil, eris.Wrap(err, "geocode: reverse geocode")
	}

	result := &ReverseResult{
		Rating: rating,
	}
	if fullAddr.Valid {
		result.Street = fullAddr.String
	}
	if state.Valid {
		result.State = state.String
	}
	if zip.Valid {
		result.ZipCode = zip.String
	}
	if countyFIPS.Valid {
		result.CountyFIPS = countyFIPS.String
	}

	return result, nil
}
