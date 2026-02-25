package tiger

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

const defaultBatchSize = 50000

// BulkLoad loads parsed rows into a tiger_data table using COPY protocol.
// For per-state products, loads into tiger_data.{st}_{table}.
// Batches in chunks of batchSize rows (0 = default 50,000).
func BulkLoad(ctx context.Context, pool db.Pool, product Product, stateAbbr string, rows [][]any, batchSize int) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	tableName := product.Table
	if !product.National && stateAbbr != "" {
		tableName = fmt.Sprintf("%s_%s", strings.ToLower(stateAbbr), product.Table)
	}

	columns := make([]string, len(product.Columns))
	copy(columns, product.Columns)
	if product.GeomType != "" {
		columns = append(columns, "the_geom")
	}

	log := zap.L().With(
		zap.String("component", "tiger.copy"),
		zap.String("table", "tiger_data."+tableName),
		zap.Int("total_rows", len(rows)),
	)

	var total int64
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		n, err := pool.CopyFrom(
			ctx,
			pgx.Identifier{"tiger_data", tableName},
			columns,
			pgx.CopyFromRows(batch),
		)
		if err != nil {
			return total, eris.Wrapf(err, "tiger: COPY into tiger_data.%s (batch %d-%d)", tableName, i, end)
		}
		total += n

		log.Debug("batch loaded",
			zap.Int("batch_start", i),
			zap.Int("batch_end", end),
			zap.Int64("batch_rows", n),
		)
	}

	return total, nil
}

// TruncateTable truncates a tiger_data table before reloading.
func TruncateTable(ctx context.Context, pool db.Pool, product Product, stateAbbr string) error {
	tableName := product.Table
	if !product.National && stateAbbr != "" {
		tableName = fmt.Sprintf("%s_%s", strings.ToLower(stateAbbr), product.Table)
	}

	sql := fmt.Sprintf("TRUNCATE %s", pgx.Identifier{"tiger_data", tableName}.Sanitize())
	if _, err := pool.Exec(ctx, sql); err != nil {
		return eris.Wrapf(err, "tiger: truncate tiger_data.%s", tableName)
	}
	return nil
}
