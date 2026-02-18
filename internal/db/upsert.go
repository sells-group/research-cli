package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"
)

// UpsertConfig defines the parameters for a bulk upsert operation.
type UpsertConfig struct {
	Table        string   // target table (e.g., "fed_data.cbp_data")
	Columns      []string // all columns being inserted
	ConflictKeys []string // columns forming the unique constraint
	UpdateCols   []string // columns to update on conflict; nil = all non-conflict columns
}

// BulkUpsert performs a bulk upsert via a temp table and INSERT ... ON CONFLICT.
// 1. Creates a temp table with the same columns
// 2. COPY rows into the temp table
// 3. INSERT INTO target SELECT ... FROM temp ON CONFLICT (keys) DO UPDATE SET ...
// 4. Drops the temp table
func BulkUpsert(ctx context.Context, pool Pool, cfg UpsertConfig, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	if len(cfg.Columns) == 0 {
		return 0, eris.New("db: upsert: no columns specified")
	}
	if len(cfg.ConflictKeys) == 0 {
		return 0, eris.New("db: upsert: no conflict keys specified")
	}

	updateCols := cfg.UpdateCols
	if updateCols == nil {
		conflictSet := make(map[string]bool, len(cfg.ConflictKeys))
		for _, k := range cfg.ConflictKeys {
			conflictSet[k] = true
		}
		for _, c := range cfg.Columns {
			if !conflictSet[c] {
				updateCols = append(updateCols, c)
			}
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, eris.Wrap(err, "db: upsert: begin tx")
	}
	defer tx.Rollback(ctx)

	tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll(cfg.Table, ".", "_"))

	// Create temp table with same structure as target
	createSQL := fmt.Sprintf(
		"CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP",
		pgx.Identifier{tempTable}.Sanitize(),
		sanitizeTable(cfg.Table),
	)
	if _, err := tx.Exec(ctx, createSQL); err != nil {
		return 0, eris.Wrapf(err, "db: upsert: create temp table for %s", cfg.Table)
	}

	// COPY rows into temp table
	copySource := pgx.CopyFromRows(rows)
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{tempTable}, cfg.Columns, copySource); err != nil {
		return 0, eris.Wrapf(err, "db: upsert: COPY into temp table for %s", cfg.Table)
	}

	// Build INSERT ... ON CONFLICT ... DO UPDATE
	colList := quoteAndJoin(cfg.Columns)
	conflictList := quoteAndJoin(cfg.ConflictKeys)

	var setClauses []string
	for _, col := range updateCols {
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", pgx.Identifier{col}.Sanitize(), pgx.Identifier{col}.Sanitize()))
	}

	upsertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
		sanitizeTable(cfg.Table),
		colList,
		colList,
		pgx.Identifier{tempTable}.Sanitize(),
		conflictList,
		strings.Join(setClauses, ", "),
	)

	tag, err := tx.Exec(ctx, upsertSQL)
	if err != nil {
		return 0, eris.Wrapf(err, "db: upsert: INSERT ON CONFLICT for %s", cfg.Table)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, eris.Wrap(err, "db: upsert: commit tx")
	}

	return tag.RowsAffected(), nil
}

// MultiUpsertEntry represents a single table's upsert within a multi-table transaction.
type MultiUpsertEntry struct {
	Config UpsertConfig
	Rows   [][]any
}

// BulkUpsertMulti performs bulk upserts for multiple tables within a single transaction.
// This avoids the overhead of separate transactions when flushing to multiple tables.
func BulkUpsertMulti(ctx context.Context, pool Pool, entries []MultiUpsertEntry) (map[string]int64, error) {
	results := make(map[string]int64, len(entries))

	// Filter out entries with no rows
	var active []MultiUpsertEntry
	for _, e := range entries {
		if len(e.Rows) > 0 {
			active = append(active, e)
		}
	}
	if len(active) == 0 {
		return results, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, eris.Wrap(err, "db: upsert multi: begin tx")
	}
	defer tx.Rollback(ctx)

	for _, e := range active {
		cfg := e.Config
		if len(cfg.Columns) == 0 {
			return nil, eris.Errorf("db: upsert multi: no columns specified for %s", cfg.Table)
		}
		if len(cfg.ConflictKeys) == 0 {
			return nil, eris.Errorf("db: upsert multi: no conflict keys specified for %s", cfg.Table)
		}

		updateCols := cfg.UpdateCols
		if updateCols == nil {
			conflictSet := make(map[string]bool, len(cfg.ConflictKeys))
			for _, k := range cfg.ConflictKeys {
				conflictSet[k] = true
			}
			for _, c := range cfg.Columns {
				if !conflictSet[c] {
					updateCols = append(updateCols, c)
				}
			}
		}

		tempTable := fmt.Sprintf("_tmp_upsert_%s", strings.ReplaceAll(cfg.Table, ".", "_"))

		createSQL := fmt.Sprintf(
			"CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP",
			pgx.Identifier{tempTable}.Sanitize(),
			sanitizeTable(cfg.Table),
		)
		if _, err := tx.Exec(ctx, createSQL); err != nil {
			return nil, eris.Wrapf(err, "db: upsert multi: create temp table for %s", cfg.Table)
		}

		copySource := pgx.CopyFromRows(e.Rows)
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{tempTable}, cfg.Columns, copySource); err != nil {
			return nil, eris.Wrapf(err, "db: upsert multi: COPY into temp table for %s", cfg.Table)
		}

		colList := quoteAndJoin(cfg.Columns)
		conflictList := quoteAndJoin(cfg.ConflictKeys)

		var setClauses []string
		for _, col := range updateCols {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", pgx.Identifier{col}.Sanitize(), pgx.Identifier{col}.Sanitize()))
		}

		upsertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
			sanitizeTable(cfg.Table),
			colList,
			colList,
			pgx.Identifier{tempTable}.Sanitize(),
			conflictList,
			strings.Join(setClauses, ", "),
		)

		tag, err := tx.Exec(ctx, upsertSQL)
		if err != nil {
			return nil, eris.Wrapf(err, "db: upsert multi: INSERT ON CONFLICT for %s", cfg.Table)
		}
		results[cfg.Table] = tag.RowsAffected()
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, eris.Wrap(err, "db: upsert multi: commit tx")
	}

	return results, nil
}

// sanitizeTable handles schema-qualified table names like "fed_data.cbp_data".
func sanitizeTable(table string) string {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return pgx.Identifier{parts[0], parts[1]}.Sanitize()
	}
	return pgx.Identifier{table}.Sanitize()
}

// quoteAndJoin quotes each column name and joins with commas.
func quoteAndJoin(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pgx.Identifier{c}.Sanitize()
	}
	return strings.Join(quoted, ", ")
}
