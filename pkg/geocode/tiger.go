package geocode

import (
	"context"
	"database/sql"

	"go.uber.org/zap"
)

// tigerGeocode performs a single geocode using PostGIS tiger geocoder.
func (g *geocoder) tigerGeocode(ctx context.Context, addr AddressInput) (*Result, error) {
	oneLine := formatOneLine(addr)
	if oneLine == "" {
		return &Result{Matched: false, Source: "tiger"}, nil
	}

	var lat, lon float64
	var rating int
	var matchedAddr string
	var countyFIPS sql.NullString

	row := g.pool.QueryRow(ctx, `
		SELECT
			ST_Y(geomout) AS lat,
			ST_X(geomout) AS lon,
			rating,
			pprint_addy(addy) AS matched_address,
			(addy).statefp || (addy).countyfp AS county_fips
		FROM geocode($1, 1)`,
		oneLine,
	)

	err := row.Scan(&lat, &lon, &rating, &matchedAddr, &countyFIPS)
	if err != nil {
		// No rows = no match (not an error).
		zap.L().Debug("tiger geocode: no match",
			zap.String("address", oneLine),
			zap.Error(err),
		)
		return &Result{Matched: false, Source: "tiger"}, nil
	}

	// Check quality threshold.
	if rating > g.maxRating {
		zap.L().Debug("tiger geocode: rating exceeds threshold",
			zap.String("address", oneLine),
			zap.Int("rating", rating),
			zap.Int("max_rating", g.maxRating),
		)
		return &Result{Matched: false, Source: "tiger", Rating: rating}, nil
	}

	result := &Result{
		Latitude:  lat,
		Longitude: lon,
		Source:    "tiger",
		Quality:   ratingToQuality(rating),
		Matched:   true,
		Rating:    rating,
	}
	if countyFIPS.Valid {
		result.CountyFIPS = countyFIPS.String
	}
	return result, nil
}

// ratingToQuality maps PostGIS geocoder rating to quality taxonomy.
// Lower ratings are better: 0 = exact match.
func ratingToQuality(rating int) string {
	switch {
	case rating < 10:
		return "rooftop"
	case rating < 20:
		return "range"
	case rating < 50:
		return "centroid"
	default:
		return "approximate"
	}
}
