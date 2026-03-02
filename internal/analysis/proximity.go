package analysis

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

const (
	proximityBatchSize = 1000
	proximityName      = "proximity_matrix"
)

// ProximityMatrix computes nearest-neighbor distances from every parcel centroid
// to 15 infrastructure/environmental/POI feature types plus census context.
// Results are written to geo.parcel_proximity.
type ProximityMatrix struct{}

// Name implements Analyzer.
func (p *ProximityMatrix) Name() string { return proximityName }

// Category implements Analyzer.
func (p *ProximityMatrix) Category() Category { return Spatial }

// Dependencies implements Analyzer.
func (p *ProximityMatrix) Dependencies() []string { return nil }

// Validate implements Analyzer.
func (p *ProximityMatrix) Validate(ctx context.Context, pool db.Pool) error {
	var count int64
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM geo.parcels").Scan(&count); err != nil {
		return eris.Wrap(err, "proximity_matrix: validate parcels")
	}
	if count == 0 {
		return eris.New("proximity_matrix: geo.parcels is empty — load parcel data first")
	}
	return nil
}

// Run implements Analyzer.
func (p *ProximityMatrix) Run(ctx context.Context, pool db.Pool, _ RunOpts) (*RunResult, error) {
	log := zap.L().With(zap.String("analyzer", proximityName))

	var totalRows int64
	cursor := ""

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rows, err := p.processBatch(ctx, pool, cursor)
		if err != nil {
			return nil, eris.Wrap(err, "proximity_matrix: process batch")
		}
		if rows == 0 {
			break
		}

		totalRows += rows
		log.Info("batch complete",
			zap.Int64("batch_rows", rows),
			zap.Int64("total_rows", totalRows),
			zap.String("cursor", cursor),
		)

		// Advance cursor to the last parcel_geoid in this batch.
		nextCursor, ok := p.advanceCursor(ctx, pool, cursor)
		if !ok {
			break
		}
		cursor = nextCursor
	}

	log.Info("proximity matrix complete", zap.Int64("total_rows", totalRows))

	return &RunResult{
		RowsAffected: totalRows,
		Metadata: map[string]any{
			"batch_size":    proximityBatchSize,
			"feature_types": 15,
		},
	}, nil
}

// advanceCursor fetches the parcel_geoid at the end of the current batch window.
// Returns the geoid and true if there are more rows, or ("", false) if exhausted.
func (p *ProximityMatrix) advanceCursor(ctx context.Context, pool db.Pool, cursor string) (string, bool) {
	var lastGeoid string
	err := pool.QueryRow(ctx,
		fmt.Sprintf(
			"SELECT parcel_geoid FROM geo.parcels WHERE parcel_geoid > $1 ORDER BY parcel_geoid LIMIT 1 OFFSET %d",
			proximityBatchSize-1,
		),
		cursor,
	).Scan(&lastGeoid)
	if err != nil {
		return "", false
	}
	return lastGeoid, true
}

// processBatch computes proximity distances for one batch of parcels and upserts
// the results into geo.parcel_proximity. Returns the number of rows affected.
func (p *ProximityMatrix) processBatch(ctx context.Context, pool db.Pool, cursor string) (int64, error) {
	query := buildProximityQuery()
	tag, err := pool.Exec(ctx, query, cursor, proximityBatchSize)
	if err != nil {
		return 0, eris.Wrap(err, "proximity_matrix: exec batch query")
	}
	return tag.RowsAffected(), nil
}

// buildProximityQuery returns the SQL that computes all 15 distances + 3 census
// lookups for a batch of parcels using LEFT JOIN LATERAL with KNN-GiST operators.
func buildProximityQuery() string {
	return `
INSERT INTO geo.parcel_proximity (
    parcel_geoid, parcel_geom,
    dist_power_plant, dist_substation, dist_transmission_line, dist_pipeline,
    dist_telecom_tower, dist_epa_site, dist_flood_zone, dist_wetland,
    dist_primary_road, dist_highway,
    dist_hospital, dist_school, dist_airport, dist_fire_station, dist_water_body,
    nearest_power_plant_id, nearest_substation_id, nearest_epa_site_id, nearest_flood_zone_id,
    county_geoid, cbsa_code, census_tract_geoid,
    computed_at
)
SELECT
    p.parcel_geoid,
    p.centroid,
    pp.dist, sub.dist, tl.dist, pl.dist,
    tt.dist, epa.dist, fz.dist, wl.dist,
    pr.dist, hw.dist,
    hosp.dist, sch.dist, ap.dist, fs.dist, wb.dist,
    pp.source_id, sub.source_id, epa.source_id, fz.source_id,
    cty.geoid, cbsa.cbsa_code, ct.geoid,
    now()
FROM (
    SELECT parcel_geoid, centroid
    FROM geo.parcels
    WHERE parcel_geoid > $1
    ORDER BY parcel_geoid
    LIMIT $2
) p
-- Infrastructure: power plants
LEFT JOIN LATERAL (
    SELECT source_id, ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.infrastructure WHERE type = 'power_plant'
    ORDER BY geom <-> p.centroid LIMIT 1
) pp ON true
-- Infrastructure: substations
LEFT JOIN LATERAL (
    SELECT source_id, ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.infrastructure WHERE type = 'substation'
    ORDER BY geom <-> p.centroid LIMIT 1
) sub ON true
-- Infrastructure: transmission lines
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.infrastructure WHERE type = 'transmission_line'
    ORDER BY geom <-> p.centroid LIMIT 1
) tl ON true
-- Infrastructure: pipelines
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.infrastructure WHERE type = 'pipeline'
    ORDER BY geom <-> p.centroid LIMIT 1
) pl ON true
-- Infrastructure: telecom towers
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.infrastructure WHERE type = 'telecom_tower'
    ORDER BY geom <-> p.centroid LIMIT 1
) tt ON true
-- EPA sites
LEFT JOIN LATERAL (
    SELECT source_id, ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.epa_sites
    ORDER BY geom <-> p.centroid LIMIT 1
) epa ON true
-- Flood zones
LEFT JOIN LATERAL (
    SELECT source_id, ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.flood_zones
    ORDER BY geom <-> p.centroid LIMIT 1
) fz ON true
-- Wetlands
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.wetlands
    ORDER BY geom <-> p.centroid LIMIT 1
) wl ON true
-- Roads: primary roads (all roads in table are primary-level)
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.roads
    ORDER BY geom <-> p.centroid LIMIT 1
) pr ON true
-- Roads: highways (interstate only)
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.roads WHERE route_type = 'interstate'
    ORDER BY geom <-> p.centroid LIMIT 1
) hw ON true
-- POI: hospitals
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.poi WHERE subcategory = 'hospital'
    ORDER BY geom <-> p.centroid LIMIT 1
) hosp ON true
-- POI: schools
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.poi WHERE subcategory = 'school'
    ORDER BY geom <-> p.centroid LIMIT 1
) sch ON true
-- POI: airports
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.poi WHERE subcategory = 'airport'
    ORDER BY geom <-> p.centroid LIMIT 1
) ap ON true
-- POI: fire stations
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.poi WHERE subcategory = 'fire_station'
    ORDER BY geom <-> p.centroid LIMIT 1
) fs ON true
-- POI: water bodies
LEFT JOIN LATERAL (
    SELECT ST_Distance(geom::geography, p.centroid::geography) AS dist
    FROM geo.poi WHERE subcategory = 'water_body'
    ORDER BY geom <-> p.centroid LIMIT 1
) wb ON true
-- Census: county
LEFT JOIN LATERAL (
    SELECT geoid
    FROM geo.counties
    WHERE ST_Contains(geom, p.centroid)
    LIMIT 1
) cty ON true
-- Census: CBSA
LEFT JOIN LATERAL (
    SELECT cbsa_code
    FROM geo.cbsa
    WHERE ST_Contains(geom, p.centroid)
    LIMIT 1
) cbsa ON true
-- Census: tract
LEFT JOIN LATERAL (
    SELECT geoid
    FROM geo.census_tracts
    WHERE ST_Contains(geom, p.centroid)
    LIMIT 1
) ct ON true
ON CONFLICT (parcel_geoid) DO UPDATE SET
    parcel_geom               = EXCLUDED.parcel_geom,
    dist_power_plant          = EXCLUDED.dist_power_plant,
    dist_substation           = EXCLUDED.dist_substation,
    dist_transmission_line    = EXCLUDED.dist_transmission_line,
    dist_pipeline             = EXCLUDED.dist_pipeline,
    dist_telecom_tower        = EXCLUDED.dist_telecom_tower,
    dist_epa_site             = EXCLUDED.dist_epa_site,
    dist_flood_zone           = EXCLUDED.dist_flood_zone,
    dist_wetland              = EXCLUDED.dist_wetland,
    dist_primary_road         = EXCLUDED.dist_primary_road,
    dist_highway              = EXCLUDED.dist_highway,
    dist_hospital             = EXCLUDED.dist_hospital,
    dist_school               = EXCLUDED.dist_school,
    dist_airport              = EXCLUDED.dist_airport,
    dist_fire_station         = EXCLUDED.dist_fire_station,
    dist_water_body           = EXCLUDED.dist_water_body,
    nearest_power_plant_id    = EXCLUDED.nearest_power_plant_id,
    nearest_substation_id     = EXCLUDED.nearest_substation_id,
    nearest_epa_site_id       = EXCLUDED.nearest_epa_site_id,
    nearest_flood_zone_id     = EXCLUDED.nearest_flood_zone_id,
    county_geoid              = EXCLUDED.county_geoid,
    cbsa_code                 = EXCLUDED.cbsa_code,
    census_tract_geoid        = EXCLUDED.census_tract_geoid,
    computed_at               = EXCLUDED.computed_at
`
}
