package geospatial

import (
	"context"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// FedBridge links fed_data address records into the geo.geocode_queue for geocoding.
type FedBridge struct {
	pool  db.Pool
	queue *GeocodeQueue
}

// NewFedBridge creates a new FedBridge.
func NewFedBridge(pool db.Pool, queue *GeocodeQueue) *FedBridge {
	return &FedBridge{pool: pool, queue: queue}
}

// EnqueueADVFirms finds ADV firms with street addresses but no geocode queue entry,
// and enqueues them for geocoding. Returns the number of items enqueued.
func (b *FedBridge) EnqueueADVFirms(ctx context.Context, limit int) (int, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT crd_number::text, COALESCE(street1, '') || ', ' || COALESCE(city, '') || ', ' || COALESCE(state, '') || ' ' || COALESCE(zip, '')
		FROM fed_data.adv_firms
		WHERE street1 IS NOT NULL AND street1 != ''
		  AND NOT EXISTS (
		      SELECT 1 FROM geo.geocode_queue gq
		      WHERE gq.source_table = 'fed_data.adv_firms' AND gq.source_id = crd_number::text
		  )
		ORDER BY crd_number
		LIMIT $1`,
		limit,
	)
	if err != nil {
		return 0, eris.Wrap(err, "fedbridge: query adv firms")
	}
	defer rows.Close()

	var items []QueueItem
	for rows.Next() {
		var item QueueItem
		if err := rows.Scan(&item.SourceID, &item.Address); err != nil {
			return 0, eris.Wrap(err, "fedbridge: scan adv firm")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return 0, eris.Wrap(err, "fedbridge: iterate adv firms")
	}

	if len(items) == 0 {
		return 0, nil
	}

	if err := b.queue.EnqueueBatch(ctx, "fed_data.adv_firms", items); err != nil {
		return 0, eris.Wrap(err, "fedbridge: enqueue adv firms")
	}

	zap.L().Info("fedbridge: enqueued ADV firms for geocoding", zap.Int("count", len(items)))
	return len(items), nil
}

// EnqueueEPAFacilities finds EPA facilities that have addresses but no coordinates
// and no geocode queue entry, and enqueues them for geocoding.
// Returns the number of items enqueued.
func (b *FedBridge) EnqueueEPAFacilities(ctx context.Context, limit int) (int, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT registry_id, COALESCE(fac_name, '') || ', ' || COALESCE(fac_city, '') || ', ' || COALESCE(fac_state, '') || ' ' || COALESCE(fac_zip, '')
		FROM fed_data.epa_facilities
		WHERE (fac_lat IS NULL OR fac_long IS NULL)
		  AND fac_city IS NOT NULL AND fac_state IS NOT NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM geo.geocode_queue gq
		      WHERE gq.source_table = 'fed_data.epa_facilities' AND gq.source_id = registry_id
		  )
		ORDER BY registry_id
		LIMIT $1`,
		limit,
	)
	if err != nil {
		return 0, eris.Wrap(err, "fedbridge: query epa facilities")
	}
	defer rows.Close()

	var items []QueueItem
	for rows.Next() {
		var item QueueItem
		if err := rows.Scan(&item.SourceID, &item.Address); err != nil {
			return 0, eris.Wrap(err, "fedbridge: scan epa facility")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return 0, eris.Wrap(err, "fedbridge: iterate epa facilities")
	}

	if len(items) == 0 {
		return 0, nil
	}

	if err := b.queue.EnqueueBatch(ctx, "fed_data.epa_facilities", items); err != nil {
		return 0, eris.Wrap(err, "fedbridge: enqueue epa facilities")
	}

	zap.L().Info("fedbridge: enqueued EPA facilities for geocoding", zap.Int("count", len(items)))
	return len(items), nil
}
