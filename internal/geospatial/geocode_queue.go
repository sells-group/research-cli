package geospatial

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/pkg/geocode"
)

// QueueItem represents an item to enqueue for geocoding.
type QueueItem struct {
	SourceID string
	Address  string
}

// GeocodeQueue manages a geocoding work queue backed by geo.geocode_queue.
type GeocodeQueue struct {
	pool      db.Pool
	geocoder  geocode.Client
	batchSize int
}

// NewGeocodeQueue creates a GeocodeQueue with the given pool, geocoder, and batch size.
func NewGeocodeQueue(pool db.Pool, geocoder geocode.Client, batchSize int) *GeocodeQueue {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &GeocodeQueue{
		pool:      pool,
		geocoder:  geocoder,
		batchSize: batchSize,
	}
}

// Enqueue inserts or updates a single address in the geocode queue.
func (q *GeocodeQueue) Enqueue(ctx context.Context, sourceTable, sourceID, address string) error {
	_, err := q.pool.Exec(ctx, `
		INSERT INTO geo.geocode_queue (source_table, source_id, address, status, attempts, created_at, updated_at)
		VALUES ($1, $2, $3, 'pending', 0, now(), now())
		ON CONFLICT (source_table, source_id) DO UPDATE SET
			address = EXCLUDED.address,
			status = 'pending',
			attempts = 0,
			error = NULL,
			updated_at = now()`,
		sourceTable, sourceID, address,
	)
	return eris.Wrap(err, "geocode queue: enqueue")
}

// EnqueueBatch inserts multiple items into the geocode queue for a given source table.
func (q *GeocodeQueue) EnqueueBatch(ctx context.Context, sourceTable string, items []QueueItem) error {
	if len(items) == 0 {
		return nil
	}

	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return eris.Wrap(err, "geocode queue: begin tx for batch enqueue")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, item := range items {
		_, err := tx.Exec(ctx, `
			INSERT INTO geo.geocode_queue (source_table, source_id, address, status, attempts, created_at, updated_at)
			VALUES ($1, $2, $3, 'pending', 0, now(), now())
			ON CONFLICT (source_table, source_id) DO UPDATE SET
				address = EXCLUDED.address,
				status = 'pending',
				attempts = 0,
				error = NULL,
				updated_at = now()`,
			sourceTable, item.SourceID, item.Address,
		)
		if err != nil {
			return eris.Wrapf(err, "geocode queue: enqueue batch item %s", item.SourceID)
		}
	}

	return eris.Wrap(tx.Commit(ctx), "geocode queue: commit batch enqueue")
}

// queueRow holds a row claimed from the geocode queue.
type queueRow struct {
	ID          int
	SourceTable string
	SourceID    string
	Address     string
}

// ProcessBatch claims up to batchSize pending items, geocodes them, and updates results.
// Returns the number of items processed.
func (q *GeocodeQueue) ProcessBatch(ctx context.Context) (int, error) {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return 0, eris.Wrap(err, "geocode queue: begin tx")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim pending rows with FOR UPDATE SKIP LOCKED to allow concurrent workers.
	rows, err := tx.Query(ctx, `
		SELECT id, source_table, source_id, address
		FROM geo.geocode_queue
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		q.batchSize,
	)
	if err != nil {
		return 0, eris.Wrap(err, "geocode queue: claim rows")
	}

	var claimed []queueRow
	for rows.Next() {
		var r queueRow
		if err := rows.Scan(&r.ID, &r.SourceTable, &r.SourceID, &r.Address); err != nil {
			rows.Close()
			return 0, eris.Wrap(err, "geocode queue: scan row")
		}
		claimed = append(claimed, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, eris.Wrap(err, "geocode queue: iterate rows")
	}

	if len(claimed) == 0 {
		_ = tx.Commit(ctx)
		return 0, nil
	}

	// Mark claimed rows as processing.
	ids := make([]int, len(claimed))
	for i, r := range claimed {
		ids[i] = r.ID
	}
	_, err = tx.Exec(ctx, `
		UPDATE geo.geocode_queue
		SET status = 'processing', attempts = attempts + 1, updated_at = now()
		WHERE id = ANY($1)`,
		ids,
	)
	if err != nil {
		return 0, eris.Wrap(err, "geocode queue: mark processing")
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, eris.Wrap(err, "geocode queue: commit claim")
	}

	// Geocode each claimed row outside the transaction.
	processed := 0
	for _, row := range claimed {
		result, gcErr := q.geocoder.Geocode(ctx, geocode.AddressInput{
			ID:     row.SourceID,
			Street: row.Address,
		})

		if gcErr != nil {
			zap.L().Warn("geocode queue: geocode failed",
				zap.Int("queue_id", row.ID),
				zap.String("source_id", row.SourceID),
				zap.Error(gcErr),
			)
			q.markFailed(ctx, row.ID, gcErr.Error())
			processed++
			continue
		}

		resultJSON, err := json.Marshal(result)
		if err != nil {
			q.markFailed(ctx, row.ID, fmt.Sprintf("marshal result: %v", err))
			processed++
			continue
		}

		q.markComplete(ctx, row.ID, resultJSON)
		processed++
	}

	return processed, nil
}

// markComplete updates a queue row to complete with the geocode result.
func (q *GeocodeQueue) markComplete(ctx context.Context, id int, resultJSON []byte) {
	_, err := q.pool.Exec(ctx, `
		UPDATE geo.geocode_queue
		SET status = 'complete', result = $2, error = NULL, updated_at = now()
		WHERE id = $1`,
		id, resultJSON,
	)
	if err != nil {
		zap.L().Error("geocode queue: mark complete", zap.Int("queue_id", id), zap.Error(err))
	}
}

// markFailed updates a queue row to failed with an error message.
func (q *GeocodeQueue) markFailed(ctx context.Context, id int, errMsg string) {
	_, err := q.pool.Exec(ctx, `
		UPDATE geo.geocode_queue
		SET status = 'failed', error = $2, updated_at = now()
		WHERE id = $1`,
		id, errMsg,
	)
	if err != nil {
		zap.L().Error("geocode queue: mark failed", zap.Int("queue_id", id), zap.Error(err))
	}
}

// WriteBack writes geocode results back to a source table's latitude/longitude columns.
func (q *GeocodeQueue) WriteBack(ctx context.Context, sourceTable, sourceID string, result *geocode.Result) error {
	if result == nil || !result.Matched {
		return nil
	}

	// Use parameterized source_id but table name is trusted (from our own queue).
	query := fmt.Sprintf(`
		UPDATE %s
		SET latitude = $1, longitude = $2, updated_at = now()
		WHERE id = $3`,
		pgx.Identifier{sourceTable}.Sanitize(),
	)
	_, err := q.pool.Exec(ctx, query, result.Latitude, result.Longitude, sourceID)
	return eris.Wrapf(err, "geocode queue: write back %s/%s", sourceTable, sourceID)
}
