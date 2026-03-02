package geoscraper

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/geospatial"
)

// PostSyncGeocode enqueues addresses from newly synced rows for geocoding.
// It queries the target table for rows missing coordinates and enqueues them
// into the geo.geocode_queue for processing.
func PostSyncGeocode(ctx context.Context, pool db.Pool, queue *geospatial.GeocodeQueue, table string, _ *SyncResult) error {
	log := zap.L().With(zap.String("component", "geoscraper.postsync"), zap.String("table", table))

	// Query rows that have an address but no geocoded coordinates.
	query := fmt.Sprintf(
		`SELECT source_id, address FROM %s
		 WHERE address IS NOT NULL AND address != ''
		   AND (latitude IS NULL OR longitude IS NULL)
		 LIMIT 10000`,
		pgx.Identifier(sanitizeTableParts(table)).Sanitize(),
	)

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return eris.Wrapf(err, "postsync: query ungeocoded rows from %s", table)
	}
	defer rows.Close()

	var items []geospatial.QueueItem
	for rows.Next() {
		var sourceID, address string
		if err := rows.Scan(&sourceID, &address); err != nil {
			return eris.Wrap(err, "postsync: scan row")
		}
		items = append(items, geospatial.QueueItem{
			SourceID: sourceID,
			Address:  address,
		})
	}
	if err := rows.Err(); err != nil {
		return eris.Wrap(err, "postsync: iterate rows")
	}

	if len(items) == 0 {
		log.Debug("no ungeocoded rows found")
		return nil
	}

	log.Info("enqueuing addresses for geocoding", zap.Int("count", len(items)))

	if err := queue.EnqueueBatch(ctx, table, items); err != nil {
		return eris.Wrapf(err, "postsync: enqueue batch for %s", table)
	}

	// For small batches, process immediately.
	if len(items) <= 100 {
		processed, err := queue.ProcessBatch(ctx)
		if err != nil {
			log.Warn("postsync: immediate geocode failed", zap.Error(err))
		} else {
			log.Info("postsync: immediate geocode complete", zap.Int("processed", processed))
		}
	}

	return nil
}

// sanitizeTableParts splits a schema-qualified table name into parts for pgx.Identifier.
func sanitizeTableParts(table string) []string {
	for i, c := range table {
		if c == '.' {
			return []string{table[:i], table[i+1:]}
		}
	}
	return []string{table}
}
