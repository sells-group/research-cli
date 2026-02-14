package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
)

// CopyFrom bulk-inserts rows into a table using PostgreSQL COPY protocol.
// This is the fastest way to insert large volumes of data.
func CopyFrom(ctx context.Context, pool *pgxpool.Pool, table string, columns []string, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	copySource := pgx.CopyFromRows(rows)
	n, err := pool.CopyFrom(ctx, pgx.Identifier{table}, columns, copySource)
	if err != nil {
		return 0, eris.Wrapf(err, "db: COPY INTO %s", table)
	}

	return n, nil
}

// CopyFromSchema bulk-inserts rows into a schema-qualified table using PostgreSQL COPY protocol.
func CopyFromSchema(ctx context.Context, pool *pgxpool.Pool, schema, table string, columns []string, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	copySource := pgx.CopyFromRows(rows)
	n, err := pool.CopyFrom(ctx, pgx.Identifier{schema, table}, columns, copySource)
	if err != nil {
		return 0, eris.Wrapf(err, "db: COPY INTO %s.%s", schema, table)
	}

	return n, nil
}
