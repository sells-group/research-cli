package geospatial

import (
	"context"
	"fmt"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"
)

// knownViews is the allowlist of materialized views that can be refreshed.
var knownViews = []string{
	"geo.mv_county_economics",
	"geo.mv_cbsa_summary",
	"geo.mv_epa_by_county",
	"geo.mv_infrastructure_by_county",
	"geo.mv_adv_firms_by_state",
}

// viewHasUniqueIndex tracks which views have a unique index and can use
// REFRESH CONCURRENTLY. All views created in migration 008 have unique indexes.
var viewHasUniqueIndex = map[string]bool{
	"geo.mv_county_economics":         true,
	"geo.mv_cbsa_summary":             true,
	"geo.mv_epa_by_county":            true,
	"geo.mv_infrastructure_by_county": true,
	"geo.mv_adv_firms_by_state":       true,
}

// ViewInfo holds the name and row count of a materialized view.
type ViewInfo struct {
	Name     string `json:"name"`
	RowCount int64  `json:"row_count"`
}

// isKnownView reports whether the given name is in the allowlist.
func isKnownView(name string) bool {
	for _, v := range knownViews {
		if v == name {
			return true
		}
	}
	return false
}

// RefreshView refreshes a single materialized view by name.
// The view name must be in the allowlist to prevent SQL injection.
// Views with a unique index are refreshed CONCURRENTLY.
func RefreshView(ctx context.Context, pool db.Pool, viewName string) error {
	if !isKnownView(viewName) {
		return eris.Errorf("geo: unknown view %q", viewName)
	}

	keyword := "REFRESH MATERIALIZED VIEW"
	if viewHasUniqueIndex[viewName] {
		keyword = "REFRESH MATERIALIZED VIEW CONCURRENTLY"
	}

	// viewName is validated against an allowlist, so it's safe to interpolate.
	sql := fmt.Sprintf("%s %s", keyword, viewName) //nolint:gosec

	zap.L().Info("refreshing materialized view",
		zap.String("view", viewName),
		zap.Bool("concurrent", viewHasUniqueIndex[viewName]),
	)

	if _, err := pool.Exec(ctx, sql); err != nil {
		return eris.Wrapf(err, "geo: refresh view %s", viewName)
	}
	return nil
}

// RefreshAllViews refreshes all known geo.mv_* materialized views.
func RefreshAllViews(ctx context.Context, pool db.Pool) error {
	for _, name := range knownViews {
		if err := RefreshView(ctx, pool, name); err != nil {
			return eris.Wrapf(err, "geo: refresh all views failed at %s", name)
		}
	}
	return nil
}

// ListViews returns the names and approximate row counts of all materialized
// views in the geo schema.
func ListViews(ctx context.Context, pool db.Pool) ([]ViewInfo, error) {
	const q = `
		SELECT schemaname || '.' || matviewname, COALESCE(n.reltuples::bigint, 0)
		FROM pg_matviews m
		LEFT JOIN pg_class n ON n.relname = m.matviewname AND n.relnamespace = (
			SELECT oid FROM pg_namespace WHERE nspname = m.schemaname
		)
		WHERE m.schemaname = 'geo'
		ORDER BY m.matviewname
	`

	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, eris.Wrap(err, "geo: list materialized views")
	}
	defer rows.Close()

	var views []ViewInfo
	for rows.Next() {
		var v ViewInfo
		if err := rows.Scan(&v.Name, &v.RowCount); err != nil {
			return nil, eris.Wrap(err, "geo: scan view info")
		}
		views = append(views, v)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate view rows")
	}
	return views, nil
}
