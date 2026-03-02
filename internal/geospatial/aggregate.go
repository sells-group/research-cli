package geospatial

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

// CountyStats holds aggregated statistics for a county.
type CountyStats struct {
	GEOID               string  `json:"geoid"`
	CountyName          string  `json:"county_name"`
	StateFIPS           string  `json:"state_fips"`
	POICount            int     `json:"poi_count"`
	InfrastructureCount int     `json:"infrastructure_count"`
	EPASiteCount        int     `json:"epa_site_count"`
	TotalCapacity       float64 `json:"total_capacity"`
}

// InfrastructureDensity holds infrastructure density metrics for a county.
type InfrastructureDensity struct {
	GEOID         string  `json:"geoid"`
	CountyName    string  `json:"county_name"`
	Type          string  `json:"type"`
	Count         int     `json:"count"`
	TotalCapacity float64 `json:"total_capacity"`
}

// DemographicSummary holds aggregated demographic data for a geographic level.
type DemographicSummary struct {
	GeoLevel        string  `json:"geo_level"`
	TotalAreas      int     `json:"total_areas"`
	TotalPopulation int64   `json:"total_population"`
	AvgMedianIncome float64 `json:"avg_median_income"`
	AvgMedianAge    float64 `json:"avg_median_age"`
	TotalHousing    int64   `json:"total_housing"`
	Year            int     `json:"year"`
}

// CountyStatsAll computes aggregate statistics for all counties by joining
// with POI, infrastructure, and EPA site tables via spatial containment.
func CountyStatsAll(ctx context.Context, pool db.Pool) ([]CountyStats, error) {
	sql := `
		SELECT
			c.geoid,
			c.name AS county_name,
			c.state_fips,
			COALESCE(p.poi_count, 0) AS poi_count,
			COALESCE(i.infra_count, 0) AS infrastructure_count,
			COALESCE(e.epa_count, 0) AS epa_site_count,
			COALESCE(i.total_capacity, 0) AS total_capacity
		FROM geo.counties c
		LEFT JOIN (
			SELECT c2.geoid, COUNT(*) AS poi_count
			FROM geo.counties c2
			JOIN geo.poi p ON ST_Contains(c2.geom, p.geom)
			GROUP BY c2.geoid
		) p ON c.geoid = p.geoid
		LEFT JOIN (
			SELECT c2.geoid, COUNT(*) AS infra_count, SUM(inf.capacity) AS total_capacity
			FROM geo.counties c2
			JOIN geo.infrastructure inf ON ST_Contains(c2.geom, inf.geom)
			GROUP BY c2.geoid
		) i ON c.geoid = i.geoid
		LEFT JOIN (
			SELECT c2.geoid, COUNT(*) AS epa_count
			FROM geo.counties c2
			JOIN geo.epa_sites ep ON ST_Contains(c2.geom, ep.geom)
			GROUP BY c2.geoid
		) e ON c.geoid = e.geoid
		ORDER BY c.state_fips, c.geoid
	`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, eris.Wrap(err, "geo: query county stats")
	}
	defer rows.Close()

	var stats []CountyStats
	for rows.Next() {
		var s CountyStats
		if err := rows.Scan(
			&s.GEOID, &s.CountyName, &s.StateFIPS,
			&s.POICount, &s.InfrastructureCount, &s.EPASiteCount,
			&s.TotalCapacity,
		); err != nil {
			return nil, eris.Wrap(err, "geo: scan county stats row")
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate county stats rows")
	}
	return stats, nil
}

// InfrastructureDensityByCounty returns infrastructure count and capacity
// grouped by county and infrastructure type.
func InfrastructureDensityByCounty(ctx context.Context, pool db.Pool, stateFIPS string) ([]InfrastructureDensity, error) {
	sql := `
		SELECT
			c.geoid,
			c.name AS county_name,
			i.type,
			COUNT(*) AS count,
			COALESCE(SUM(i.capacity), 0) AS total_capacity
		FROM geo.counties c
		JOIN geo.infrastructure i ON ST_Contains(c.geom, i.geom)
		WHERE c.state_fips = $1
		GROUP BY c.geoid, c.name, i.type
		ORDER BY c.geoid, i.type
	`
	rows, err := pool.Query(ctx, sql, stateFIPS)
	if err != nil {
		return nil, eris.Wrap(err, "geo: query infrastructure density")
	}
	defer rows.Close()

	var results []InfrastructureDensity
	for rows.Next() {
		var d InfrastructureDensity
		if err := rows.Scan(&d.GEOID, &d.CountyName, &d.Type, &d.Count, &d.TotalCapacity); err != nil {
			return nil, eris.Wrap(err, "geo: scan infrastructure density row")
		}
		results = append(results, d)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate infrastructure density rows")
	}
	return results, nil
}

// DemographicsByLevel returns aggregated demographic summaries grouped by
// geographic level for a given year.
func DemographicsByLevel(ctx context.Context, pool db.Pool, year int) ([]DemographicSummary, error) {
	sql := `
		SELECT
			geo_level,
			COUNT(*) AS total_areas,
			COALESCE(SUM(total_population), 0) AS total_population,
			COALESCE(AVG(median_income), 0) AS avg_median_income,
			COALESCE(AVG(median_age), 0) AS avg_median_age,
			COALESCE(SUM(housing_units), 0) AS total_housing,
			year
		FROM geo.demographics
		WHERE year = $1
		GROUP BY geo_level, year
		ORDER BY geo_level
	`
	rows, err := pool.Query(ctx, sql, year)
	if err != nil {
		return nil, eris.Wrap(err, "geo: query demographics by level")
	}
	defer rows.Close()

	var summaries []DemographicSummary
	for rows.Next() {
		var s DemographicSummary
		if err := rows.Scan(
			&s.GeoLevel, &s.TotalAreas, &s.TotalPopulation,
			&s.AvgMedianIncome, &s.AvgMedianAge, &s.TotalHousing, &s.Year,
		); err != nil {
			return nil, eris.Wrap(err, "geo: scan demographics summary row")
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate demographics summary rows")
	}
	return summaries, nil
}
