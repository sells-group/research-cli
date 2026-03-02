package analysis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

const (
	scoreBatchSize = 1000
	scoreName      = "parcel_scores"
)

// defaultWeights defines the composite score formula weights.
// Composite = (infra*0.30 + connectivity*0.25 + demographic*0.20) × (1 - 0.15*envRisk) × (1 - 0.10*floodRisk)
var defaultWeights = map[string]float64{
	"infrastructure": 0.30,
	"connectivity":   0.25,
	"demographic":    0.20,
	"environmental":  0.15,
	"flood":          0.10,
}

// ParcelScore computes composite scores for every parcel based on infrastructure
// proximity, environmental constraints, connectivity, demographics, and flood risk.
// Results are written to geo.parcel_scores.
type ParcelScore struct{}

// Name implements Analyzer.
func (ps *ParcelScore) Name() string { return scoreName }

// Category implements Analyzer.
func (ps *ParcelScore) Category() Category { return Scoring }

// Dependencies implements Analyzer.
func (ps *ParcelScore) Dependencies() []string { return []string{"proximity_matrix"} }

// Validate implements Analyzer.
func (ps *ParcelScore) Validate(ctx context.Context, pool db.Pool) error {
	var count int64
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM geo.parcel_proximity").Scan(&count); err != nil {
		return eris.Wrap(err, "parcel_scores: validate parcel_proximity")
	}
	if count == 0 {
		return eris.New("parcel_scores: geo.parcel_proximity is empty — run proximity_matrix first")
	}
	return nil
}

// Run implements Analyzer.
func (ps *ParcelScore) Run(ctx context.Context, pool db.Pool, _ RunOpts) (*RunResult, error) {
	log := zap.L().With(zap.String("analyzer", scoreName))

	var totalRows int64
	cursor := ""

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rows, err := ps.processBatch(ctx, pool, cursor)
		if err != nil {
			return nil, eris.Wrap(err, "parcel_scores: process batch")
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

		nextCursor, ok := ps.advanceCursor(ctx, pool, cursor)
		if !ok {
			break
		}
		cursor = nextCursor
	}

	// Rank pass: assign opportunity_rank based on composite_score.
	if totalRows > 0 {
		if err := ps.computeRanks(ctx, pool); err != nil {
			return nil, eris.Wrap(err, "parcel_scores: compute ranks")
		}
	}

	log.Info("parcel scoring complete", zap.Int64("total_rows", totalRows))

	weightsJSON, _ := json.Marshal(defaultWeights)

	return &RunResult{
		RowsAffected: totalRows,
		Metadata: map[string]any{
			"batch_size": scoreBatchSize,
			"weights":    string(weightsJSON),
		},
	}, nil
}

// advanceCursor fetches the parcel_geoid at the end of the current batch window
// from geo.parcel_proximity. Returns the geoid and true if there are more rows,
// or ("", false) if exhausted.
func (ps *ParcelScore) advanceCursor(ctx context.Context, pool db.Pool, cursor string) (string, bool) {
	var lastGeoid string
	err := pool.QueryRow(ctx,
		fmt.Sprintf(
			"SELECT parcel_geoid FROM geo.parcel_proximity WHERE parcel_geoid > $1 ORDER BY parcel_geoid LIMIT 1 OFFSET %d",
			scoreBatchSize-1,
		),
		cursor,
	).Scan(&lastGeoid)
	if err != nil {
		return "", false
	}
	return lastGeoid, true
}

// processBatch computes sub-scores for one batch of parcels and upserts the
// results into geo.parcel_scores. Returns the number of rows affected.
func (ps *ParcelScore) processBatch(ctx context.Context, pool db.Pool, cursor string) (int64, error) {
	query := buildScoreQuery()
	tag, err := pool.Exec(ctx, query, cursor, scoreBatchSize)
	if err != nil {
		return 0, eris.Wrap(err, "parcel_scores: exec batch query")
	}
	return tag.RowsAffected(), nil
}

// computeRanks assigns opportunity_rank to all scored parcels using a window function.
func (ps *ParcelScore) computeRanks(ctx context.Context, pool db.Pool) error {
	_, err := pool.Exec(ctx, buildRankQuery())
	if err != nil {
		return eris.Wrap(err, "parcel_scores: exec rank query")
	}
	return nil
}

// buildScoreQuery returns the CTE-based SQL that computes all sub-scores for a
// batch of parcels and upserts the results into geo.parcel_scores.
func buildScoreQuery() string {
	weightsJSON, _ := json.Marshal(defaultWeights)
	return fmt.Sprintf(`
WITH batch AS (
    SELECT pp.parcel_geoid, pp.parcel_geom,
           pp.dist_substation, pp.dist_transmission_line, pp.dist_pipeline,
           pp.dist_telecom_tower, pp.dist_power_plant, pp.dist_fire_station, pp.dist_water_body,
           pp.dist_primary_road, pp.dist_highway, pp.dist_airport, pp.dist_hospital, pp.dist_school,
           pp.dist_flood_zone, pp.dist_wetland, pp.dist_epa_site,
           pp.census_tract_geoid,
           p.acreage
    FROM geo.parcel_proximity pp
    JOIN geo.parcels p ON p.parcel_geoid = pp.parcel_geoid
    WHERE pp.parcel_geoid > $1
    ORDER BY pp.parcel_geoid
    LIMIT $2
),
infra AS (
    SELECT parcel_geoid,
           (
               0.25 * exp(-COALESCE(dist_substation, 99999) / 15000.0)
             + 0.20 * exp(-COALESCE(dist_transmission_line, 99999) / 10000.0)
             + 0.15 * exp(-COALESCE(dist_pipeline, 99999) / 20000.0)
             + 0.15 * exp(-COALESCE(dist_telecom_tower, 99999) / 8000.0)
             + 0.10 * exp(-COALESCE(dist_power_plant, 99999) / 50000.0)
             + 0.10 * exp(-COALESCE(dist_fire_station, 99999) / 10000.0)
             + 0.05 * exp(-COALESCE(dist_water_body, 99999) / 5000.0)
           ) AS infrastructure_score
    FROM batch
),
connectivity AS (
    SELECT parcel_geoid,
           (
               0.30 * exp(-COALESCE(dist_primary_road, 99999) / 5000.0)
             + 0.25 * exp(-COALESCE(dist_highway, 99999) / 20000.0)
             + 0.20 * exp(-COALESCE(dist_airport, 99999) / 50000.0)
             + 0.15 * exp(-COALESCE(dist_hospital, 99999) / 15000.0)
             + 0.10 * exp(-COALESCE(dist_school, 99999) / 10000.0)
           ) AS connectivity_score
    FROM batch
),
constraints AS (
    SELECT b.parcel_geoid,
           EXISTS (
               SELECT 1 FROM geo.flood_zones fz
               WHERE ST_Intersects(fz.geom, b.parcel_geom)
           ) AS in_flood_zone,
           EXISTS (
               SELECT 1 FROM geo.wetlands wl
               WHERE ST_Intersects(wl.geom, b.parcel_geom)
           ) AS in_wetland,
           EXISTS (
               SELECT 1 FROM geo.epa_sites es
               WHERE ST_DWithin(es.geom::geography, b.parcel_geom::geography, 1609)
           ) AS near_epa_site
    FROM batch b
),
risk AS (
    SELECT c.parcel_geoid,
           (
               0.40 * (CASE WHEN c.in_flood_zone THEN 1.0 ELSE 0.0 END)
             + 0.35 * (CASE WHEN c.in_wetland THEN 1.0 ELSE 0.0 END)
             + 0.25 * (CASE WHEN c.near_epa_site THEN 1.0 ELSE 0.0 END)
           ) AS environmental_risk,
           CASE
               WHEN c.in_flood_zone THEN 1.0
               ELSE exp(-COALESCE(b.dist_flood_zone, 99999) / 5000.0)
           END AS flood_risk
    FROM constraints c
    JOIN batch b ON b.parcel_geoid = c.parcel_geoid
),
demo AS (
    SELECT b.parcel_geoid,
           COALESCE(LEAST(1.0, d.median_income / 150000.0), 0.5) AS demographic_score
    FROM batch b
    LEFT JOIN geo.demographics d ON d.tract_geoid = b.census_tract_geoid
),
assembly AS (
    SELECT b.parcel_geoid,
           (SELECT count(*) FROM geo.parcels adj
            WHERE adj.parcel_geoid != b.parcel_geoid
              AND ST_Touches(adj.geom, (SELECT geom FROM geo.parcels WHERE parcel_geoid = b.parcel_geoid))
           ) AS adjacent_count
    FROM batch b
)
INSERT INTO geo.parcel_scores (
    parcel_geoid,
    infrastructure_score, connectivity_score, environmental_risk, flood_risk, demographic_score,
    composite_score, weight_config, properties, computed_at
)
SELECT
    b.parcel_geoid,
    i.infrastructure_score,
    conn.connectivity_score,
    r.environmental_risk,
    r.flood_risk,
    dm.demographic_score,
    (0.30 * i.infrastructure_score + 0.25 * conn.connectivity_score + 0.20 * dm.demographic_score)
        * (1.0 - 0.15 * r.environmental_risk)
        * (1.0 - 0.10 * r.flood_risk),
    '%s'::jsonb,
    jsonb_build_object(
        'adjacent_parcels', a.adjacent_count,
        'acreage', b.acreage,
        'in_flood_zone', cs.in_flood_zone,
        'in_wetland', cs.in_wetland,
        'near_epa_site', cs.near_epa_site
    ),
    now()
FROM batch b
JOIN infra i ON i.parcel_geoid = b.parcel_geoid
JOIN connectivity conn ON conn.parcel_geoid = b.parcel_geoid
JOIN constraints cs ON cs.parcel_geoid = b.parcel_geoid
JOIN risk r ON r.parcel_geoid = b.parcel_geoid
JOIN demo dm ON dm.parcel_geoid = b.parcel_geoid
JOIN assembly a ON a.parcel_geoid = b.parcel_geoid
ON CONFLICT (parcel_geoid) DO UPDATE SET
    infrastructure_score = EXCLUDED.infrastructure_score,
    connectivity_score   = EXCLUDED.connectivity_score,
    environmental_risk   = EXCLUDED.environmental_risk,
    flood_risk           = EXCLUDED.flood_risk,
    demographic_score    = EXCLUDED.demographic_score,
    composite_score      = EXCLUDED.composite_score,
    weight_config        = EXCLUDED.weight_config,
    properties           = EXCLUDED.properties,
    computed_at          = EXCLUDED.computed_at
`, string(weightsJSON))
}

// buildRankQuery returns the SQL that assigns opportunity_rank to all scored parcels.
func buildRankQuery() string {
	return `
UPDATE geo.parcel_scores SET opportunity_rank = ranked.rnk
FROM (
    SELECT parcel_geoid,
           RANK() OVER (ORDER BY composite_score DESC) AS rnk
    FROM geo.parcel_scores
    WHERE composite_score IS NOT NULL
) ranked
WHERE geo.parcel_scores.parcel_geoid = ranked.parcel_geoid
`
}
