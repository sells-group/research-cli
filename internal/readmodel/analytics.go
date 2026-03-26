package readmodel

import (
	"context"

	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

type postgresAnalytics struct {
	pool db.Pool
}

// SyncTrends implements AnalyticsReader.
func (p *postgresAnalytics) SyncTrends(ctx context.Context, days int) ([]SyncTrend, error) {
	if days <= 0 {
		days = 30
	}

	rows, err := p.pool.Query(ctx, `
		SELECT
			sync_date,
			dataset,
			rows_synced
		FROM fed_data.mv_sync_daily_trends
		WHERE sync_date >= current_date - $1::int
		ORDER BY sync_date DESC, dataset`,
		days,
	)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: sync trends")
	}
	defer rows.Close()

	var trends []SyncTrend
	for rows.Next() {
		var row SyncTrend
		if err := rows.Scan(&row.Date, &row.Dataset, &row.RowsSynced); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan sync trend")
		}
		trends = append(trends, row)
	}

	if trends == nil {
		trends = []SyncTrend{}
	}
	return trends, eris.Wrap(rows.Err(), "readmodel: iterate sync trends")
}

// IdentifierCoverage implements AnalyticsReader.
func (p *postgresAnalytics) IdentifierCoverage(ctx context.Context) ([]IdentifierCoverage, error) {
	var totalCompanies int
	if err := p.pool.QueryRow(ctx, `SELECT COUNT(*) FROM companies`).Scan(&totalCompanies); err != nil {
		return nil, eris.Wrap(err, "readmodel: count companies for identifier coverage")
	}

	rows, err := p.pool.Query(ctx, `
		SELECT system, COUNT(*) AS count
		FROM company_identifiers
		GROUP BY system
		ORDER BY count DESC`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: identifier coverage")
	}
	defer rows.Close()

	var coverage []IdentifierCoverage
	for rows.Next() {
		var row IdentifierCoverage
		if err := rows.Scan(&row.System, &row.Count); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan identifier coverage")
		}
		row.Total = totalCompanies
		if totalCompanies > 0 {
			row.Percentage = float64(row.Count) / float64(totalCompanies) * 100
		}
		coverage = append(coverage, row)
	}

	if coverage == nil {
		coverage = []IdentifierCoverage{}
	}
	return coverage, eris.Wrap(rows.Err(), "readmodel: iterate identifier coverage")
}

// XrefCoverage implements AnalyticsReader.
func (p *postgresAnalytics) XrefCoverage(ctx context.Context) ([]XrefCoverage, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT source_a, source_b, COUNT(*) AS link_count,
			ROUND(AVG(confidence)::numeric, 3) AS avg_confidence
		FROM fed_data.entity_xref_multi
		GROUP BY source_a, source_b
		ORDER BY link_count DESC`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: xref coverage")
	}
	defer rows.Close()

	var coverage []XrefCoverage
	for rows.Next() {
		var row XrefCoverage
		if err := rows.Scan(&row.SourceA, &row.SourceB, &row.Count, &row.AvgConfidence); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan xref coverage")
		}
		coverage = append(coverage, row)
	}

	if coverage == nil {
		coverage = []XrefCoverage{}
	}
	return coverage, eris.Wrap(rows.Err(), "readmodel: iterate xref coverage")
}

// EnrichmentStats implements AnalyticsReader.
func (p *postgresAnalytics) EnrichmentStats(ctx context.Context, days int) (*EnrichmentStats, error) {
	if days <= 0 {
		days = 30
	}

	stats := &EnrichmentStats{
		ScoreDistribution: []ScoreDistributionBucket{},
		PhaseDurations:    []PhaseDuration{},
	}

	var avgScore *float64
	if err := p.pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			ROUND(AVG(CASE
				WHEN result->>'score' IS NOT NULL THEN (result->>'score')::numeric
				ELSE NULL
			END)::numeric, 3)
		FROM runs
		WHERE created_at >= now() - ($1 || ' days')::interval`,
		days,
	).Scan(&stats.TotalRuns, &avgScore); err != nil {
		return nil, eris.Wrap(err, "readmodel: enrichment stats")
	}
	if avgScore != nil {
		stats.AvgScore = *avgScore
	}

	scoreRows, err := p.pool.Query(ctx, `
		SELECT
			FLOOR((result->>'score')::numeric * 10) * 10 AS bucket,
			COUNT(*) AS count
		FROM runs
		WHERE created_at >= now() - ($1 || ' days')::interval
			AND result->>'score' IS NOT NULL
		GROUP BY bucket
		ORDER BY bucket`,
		days,
	)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: score distribution")
	}
	defer scoreRows.Close()

	for scoreRows.Next() {
		var row ScoreDistributionBucket
		if err := scoreRows.Scan(&row.Bucket, &row.Count); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan score distribution")
		}
		stats.ScoreDistribution = append(stats.ScoreDistribution, row)
	}
	if err := scoreRows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate score distribution")
	}

	durationRows, err := p.pool.Query(ctx, `
		SELECT
			name,
			ROUND(AVG((result->>'duration_ms')::numeric))::bigint AS avg_ms
		FROM run_phases
		WHERE started_at >= now() - ($1 || ' days')::interval
			AND result->>'duration_ms' IS NOT NULL
		GROUP BY name
		ORDER BY name`,
		days,
	)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: phase durations")
	}
	defer durationRows.Close()

	for durationRows.Next() {
		var row PhaseDuration
		if err := durationRows.Scan(&row.Phase, &row.AvgMS); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan phase duration")
		}
		stats.PhaseDurations = append(stats.PhaseDurations, row)
	}

	return stats, eris.Wrap(durationRows.Err(), "readmodel: iterate phase durations")
}

// CostBreakdown implements AnalyticsReader.
func (p *postgresAnalytics) CostBreakdown(ctx context.Context, days int) ([]CostBreakdownRow, error) {
	if days <= 0 {
		days = 30
	}

	rows, err := p.pool.Query(ctx, `
		SELECT
			date_trunc('day', created_at)::date AS cost_date,
			'all' AS tier,
			COALESCE(SUM((result->>'total_cost')::numeric), 0) AS cost,
			COALESCE(SUM((result->>'total_tokens')::bigint), 0) AS tokens
		FROM runs
		WHERE created_at >= now() - ($1 || ' days')::interval
			AND result IS NOT NULL
		GROUP BY cost_date
		ORDER BY cost_date DESC`,
		days,
	)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: cost breakdown")
	}
	defer rows.Close()

	var breakdown []CostBreakdownRow
	for rows.Next() {
		var row CostBreakdownRow
		if err := rows.Scan(&row.Date, &row.Tier, &row.Cost, &row.Tokens); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan cost breakdown")
		}
		breakdown = append(breakdown, row)
	}

	if breakdown == nil {
		breakdown = []CostBreakdownRow{}
	}
	return breakdown, eris.Wrap(rows.Err(), "readmodel: iterate cost breakdown")
}
