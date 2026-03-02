package geospatial

import (
	"context"
	"fmt"
	"strings"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
)

// USStateFIPS is the complete set of US state and territory FIPS codes.
var USStateFIPS = []string{
	"01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
	"13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
	"24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
	"34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
	"45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
	"56",                         // 50 states + DC
	"60", "66", "69", "72", "78", // territories: AS, GU, MP, PR, VI
}

// partitionableTable maps base table names to their partitioned variants.
var partitionableTable = map[string]string{
	"counties":      "counties_partitioned",
	"census_tracts": "census_tracts_partitioned",
	"places":        "places_partitioned",
}

// EnsureStatePartitions creates state-level partitions for a partitioned table.
// It creates one partition per FIPS code. Existing partitions are skipped.
func EnsureStatePartitions(ctx context.Context, pool db.Pool, baseTable string, fipsCodes []string) (int, error) {
	partTable, ok := partitionableTable[baseTable]
	if !ok {
		return 0, eris.Errorf("geo: table %q is not partitionable", baseTable)
	}

	log := zap.L().With(
		zap.String("component", "geo.partition"),
		zap.String("table", partTable),
	)

	created := 0
	for _, fips := range fipsCodes {
		if err := validateFIPS(fips); err != nil {
			return created, err
		}

		partName := fmt.Sprintf("geo.%s_%s", partTable, fips)
		sql := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF geo.%s FOR VALUES IN ('%s')`,
			partName, partTable, fips,
		)

		if _, err := pool.Exec(ctx, sql); err != nil {
			// Check for already-exists error and skip gracefully.
			if strings.Contains(err.Error(), "already exists") {
				continue
			}
			return created, eris.Wrapf(err, "geo: create partition %s", partName)
		}
		created++
		log.Debug("created partition", zap.String("partition", partName), zap.String("fips", fips))
	}

	log.Info("state partitions ensured",
		zap.Int("created", created),
		zap.Int("total_fips", len(fipsCodes)),
	)
	return created, nil
}

// EnsureAllStatePartitions creates partitions for all 50 states + DC + territories.
func EnsureAllStatePartitions(ctx context.Context, pool db.Pool, baseTable string) (int, error) {
	return EnsureStatePartitions(ctx, pool, baseTable, USStateFIPS)
}

// CreatePartitionIndexes creates GIST and B-tree indexes on each state partition.
// This provides partition-level indexes for faster spatial queries.
func CreatePartitionIndexes(ctx context.Context, pool db.Pool, baseTable string, fipsCodes []string) error {
	partTable, ok := partitionableTable[baseTable]
	if !ok {
		return eris.Errorf("geo: table %q is not partitionable", baseTable)
	}

	for _, fips := range fipsCodes {
		if err := validateFIPS(fips); err != nil {
			return err
		}

		partName := fmt.Sprintf("%s_%s", partTable, fips)

		// GIST index on geom.
		gistSQL := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_%s_geom ON geo.%s USING GIST (geom)`,
			partName, partName,
		)
		if _, err := pool.Exec(ctx, gistSQL); err != nil {
			return eris.Wrapf(err, "geo: create GIST index on %s", partName)
		}

		// B-tree index on geoid.
		btreeSQL := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_%s_geoid ON geo.%s (geoid)`,
			partName, partName,
		)
		if _, err := pool.Exec(ctx, btreeSQL); err != nil {
			return eris.Wrapf(err, "geo: create B-tree index on %s", partName)
		}
	}
	return nil
}

// ListPartitions returns the existing partition names for a partitioned table.
func ListPartitions(ctx context.Context, pool db.Pool, baseTable string) ([]string, error) {
	partTable, ok := partitionableTable[baseTable]
	if !ok {
		return nil, eris.Errorf("geo: table %q is not partitionable", baseTable)
	}

	sql := `
		SELECT child.relname
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		JOIN pg_namespace ns ON parent.relnamespace = ns.oid
		WHERE ns.nspname = 'geo' AND parent.relname = $1
		ORDER BY child.relname
	`

	rows, err := pool.Query(ctx, sql, partTable)
	if err != nil {
		return nil, eris.Wrap(err, "geo: list partitions")
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, eris.Wrap(err, "geo: scan partition name")
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// validateFIPS checks that a FIPS code is a 2-digit numeric string.
func validateFIPS(fips string) error {
	if len(fips) != 2 {
		return eris.Errorf("geo: invalid FIPS code %q: must be 2 digits", fips)
	}
	for _, c := range fips {
		if c < '0' || c > '9' {
			return eris.Errorf("geo: invalid FIPS code %q: must be numeric", fips)
		}
	}
	return nil
}
