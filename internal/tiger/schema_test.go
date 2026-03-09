package tiger

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeDerivedColumns_County(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "countyfp", "name"},
		Rows: [][]any{
			{"11", "001", "District of Columbia"},
			{"48", "201", "Harris"},
		},
	}

	out := ComputeDerivedColumns(result, "county_all")

	require.Len(t, out.Columns, 4)
	assert.Equal(t, "cntyidfp", out.Columns[3])
	assert.Equal(t, "11001", out.Rows[0][3])
	assert.Equal(t, "48201", out.Rows[1][3])
}

func TestComputeDerivedColumns_Cousub(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "countyfp", "cousubfp", "name"},
		Rows: [][]any{
			{"48", "201", "92800", "Houston"},
		},
	}

	out := ComputeDerivedColumns(result, "cousub")

	require.Len(t, out.Columns, 5)
	assert.Equal(t, "cosbidfp", out.Columns[4])
	assert.Equal(t, "4820192800", out.Rows[0][4])
}

func TestComputeDerivedColumns_Place(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "placefp", "name"},
		Rows: [][]any{
			{"48", "35000", "Houston"},
		},
	}

	out := ComputeDerivedColumns(result, "place")

	require.Len(t, out.Columns, 4)
	assert.Equal(t, "plcidfp", out.Columns[3])
	assert.Equal(t, "4835000", out.Rows[0][3])
}

func TestComputeDerivedColumns_NilSource(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "countyfp", "name"},
		Rows: [][]any{
			{nil, "001", "Unknown"},
		},
	}

	out := ComputeDerivedColumns(result, "county_all")

	require.Len(t, out.Columns, 4)
	assert.Nil(t, out.Rows[0][3], "derived column should be nil when source is nil")
}

func TestComputeDerivedColumns_NoMatch(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"tlid", "fullname"},
		Rows:    [][]any{{"123", "Main St"}},
	}

	out := ComputeDerivedColumns(result, "edges")
	assert.Equal(t, result, out, "should return original when no derived columns defined")
}

func TestComputeDerivedColumns_AlreadyPresent(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "countyfp", "cntyidfp"},
		Rows:    [][]any{{"48", "201", "48201"}},
	}

	out := ComputeDerivedColumns(result, "county_all")
	assert.Len(t, out.Columns, 3, "should not add column if already present")
}

func TestRenameColumns_ZCTA(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"zcta5ce20", "classfp20", "mtfcc20", "aland20"},
		Rows:    [][]any{{"10001", "B5", "G6350", "12345"}},
	}

	out := RenameColumns(result, "zcta5")

	assert.Equal(t, []string{"zcta5ce", "classfp", "mtfcc", "aland"}, out.Columns)
	assert.Equal(t, result.Rows, out.Rows, "rows should be unchanged")
}

func TestRenameColumns_NoMatch(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "name"},
		Rows:    [][]any{{"48", "Texas"}},
	}

	out := RenameColumns(result, "state_all")
	assert.Equal(t, result, out, "should return original when no renames defined")
}

func TestSetStateFIPS_AddsColumn(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"tlid", "fullname"},
		Rows: [][]any{
			{"123", "Main St"},
			{"456", "Oak Ave"},
		},
	}

	out := SetStateFIPS(result, "11")

	require.Len(t, out.Columns, 3)
	assert.Equal(t, "statefp", out.Columns[2])
	assert.Equal(t, "11", out.Rows[0][2])
	assert.Equal(t, "11", out.Rows[1][2])
}

func TestSetStateFIPS_FillsNulls(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"tlid", "statefp"},
		Rows: [][]any{
			{"123", nil},
			{"456", nil},
		},
	}

	out := SetStateFIPS(result, "48")

	assert.Len(t, out.Columns, 2, "should not add new column")
	assert.Equal(t, "48", out.Rows[0][1])
	assert.Equal(t, "48", out.Rows[1][1])
}

func TestSetStateFIPS_NoopWhenPopulated(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"tlid", "statefp"},
		Rows: [][]any{
			{"123", "11"},
		},
	}

	out := SetStateFIPS(result, "48")
	assert.Equal(t, "11", out.Rows[0][1], "should not overwrite existing value")
}

func TestSetStateFIPS_NoopForNational(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp"},
		Rows:    [][]any{{"11"}},
	}

	out := SetStateFIPS(result, "us")
	assert.Equal(t, result, out, "should skip for national (us) FIPS")

	out = SetStateFIPS(result, "")
	assert.Equal(t, result, out, "should skip for empty FIPS")
}

func TestFilterToTable(t *testing.T) {
	result := &ParseResult{
		Columns: []string{"statefp", "countyfp", "cntyidfp", "extra_col", "the_geom"},
		Rows: [][]any{
			{"48", "201", "48201", "extra", []byte("wkb")},
		},
	}

	validCols := map[string]bool{
		"statefp":  true,
		"countyfp": true,
		"cntyidfp": true,
		"the_geom": true,
	}

	filtered := FilterToTable(result, validCols)

	assert.Equal(t, []string{"statefp", "countyfp", "cntyidfp", "the_geom"}, filtered.Columns)
	require.Len(t, filtered.Rows, 1)
	assert.Equal(t, []any{"48", "201", "48201", []byte("wkb")}, filtered.Rows[0])
}

func TestPrepareTemplates(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First three alterations succeed.
	mock.ExpectExec("ALTER TABLE tiger.county ALTER COLUMN cntyidfp DROP NOT NULL").
		WillReturnResult(pgxmock.NewResult("ALTER", 0))
	mock.ExpectExec("ALTER TABLE tiger.cousub ALTER COLUMN cosbidfp DROP NOT NULL").
		WillReturnResult(pgxmock.NewResult("ALTER", 0))
	mock.ExpectExec("ALTER TABLE tiger.place ALTER COLUMN plcidfp DROP NOT NULL").
		WillReturnResult(pgxmock.NewResult("ALTER", 0))
	// Fourth alteration returns an error (e.g., constraint already dropped) — should be silently ignored.
	mock.ExpectExec("ALTER TABLE tiger.zcta5 ALTER COLUMN statefp DROP NOT NULL").
		WillReturnError(fmt.Errorf("column does not have a NOT NULL constraint"))
	// Fifth alteration succeeds.
	mock.ExpectExec("ALTER TABLE tiger.zcta5 ALTER COLUMN zcta5ce DROP NOT NULL").
		WillReturnResult(pgxmock.NewResult("ALTER", 0))

	err = PrepareTemplates(context.Background(), mock)
	require.NoError(t, err, "PrepareTemplates should never return an error")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPrepareTemplates_AllErrors(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// All five alterations return errors — function should still return nil.
	mock.ExpectExec("ALTER TABLE tiger.county ALTER COLUMN cntyidfp DROP NOT NULL").
		WillReturnError(fmt.Errorf("error 1"))
	mock.ExpectExec("ALTER TABLE tiger.cousub ALTER COLUMN cosbidfp DROP NOT NULL").
		WillReturnError(fmt.Errorf("error 2"))
	mock.ExpectExec("ALTER TABLE tiger.place ALTER COLUMN plcidfp DROP NOT NULL").
		WillReturnError(fmt.Errorf("error 3"))
	mock.ExpectExec("ALTER TABLE tiger.zcta5 ALTER COLUMN statefp DROP NOT NULL").
		WillReturnError(fmt.Errorf("error 4"))
	mock.ExpectExec("ALTER TABLE tiger.zcta5 ALTER COLUMN zcta5ce DROP NOT NULL").
		WillReturnError(fmt.Errorf("error 5"))

	err = PrepareTemplates(context.Background(), mock)
	require.NoError(t, err, "PrepareTemplates should return nil even when all alterations fail")
	require.NoError(t, mock.ExpectationsWereMet())
}

// mockSRIDNotExists mocks the SRID check query returning 0 (table not found in geometry_columns).
func mockSRIDNotExists(mock pgxmock.PgxPoolIface, schema, table string) {
	mock.ExpectQuery("SELECT COALESCE").
		WithArgs(schema, table).
		WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(0))
}

func TestCreateParentTables(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "STATE", Table: "state_all", TemplateTable: "state", National: true, GeomType: "MULTIPOLYGON"},
		{Name: "ZCTA520", Table: "zcta5", FileKey: "zcta520", National: true, GeomType: "MULTIPOLYGON"},
	}

	// --- STATE product (non-zcta5): SRID check + CREATE TABLE + inheritance DO block ---
	mockSRIDNotExists(mock, "tiger_data", "state_all")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."state_all" \(LIKE "tiger"."state" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))

	// --- ZCTA520 product (zcta5 special case): SRID check + CREATE TABLE + drop PK DO block + drop NOT NULL ---
	mockSRIDNotExists(mock, "tiger_data", "zcta5")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."zcta5" \(LIKE "tiger"."zcta5" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	// zcta5: drop PK constraint via DO block
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	// zcta5: drop NOT NULL on statefp
	mock.ExpectExec(`ALTER TABLE "tiger_data"."zcta5" ALTER COLUMN statefp DROP NOT NULL`).
		WillReturnResult(pgxmock.NewResult("ALTER", 0))

	// load_status tracking table
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS tiger_data.load_status").
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	err = CreateParentTables(context.Background(), mock, products)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateParentTables_CreateError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "STATE", Table: "state_all", TemplateTable: "state", National: true, GeomType: "MULTIPOLYGON"},
	}

	mockSRIDNotExists(mock, "tiger_data", "state_all")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."state_all"`).
		WillReturnError(fmt.Errorf("permission denied"))

	err = CreateParentTables(context.Background(), mock, products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create parent table")
	// No load_status expectation needed — function returns early on error.
}

func TestCreateStateTables(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		// National product — should be skipped.
		{Name: "STATE", Table: "state_all", National: true, GeomType: "MULTIPOLYGON"},
		// addr: per-county, no geom, has zip index.
		{Name: "ADDR", Table: "addr", PerCounty: true, GeomType: ""},
		// edges: per-county, has geom, has tlid index.
		{Name: "EDGES", Table: "edges", PerCounty: true, GeomType: "MULTILINESTRING"},
		// featnames: per-county, no geom, has tlid index.
		{Name: "FEATNAMES", Table: "featnames", PerCounty: true, GeomType: ""},
	}

	// --- ADDR (no geom, zip + tlid indexes) ---
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."fl_addr" \(LIKE "tiger_data"."addr" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	// addr-specific zip index
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_fl_addr_zip" ON "tiger_data"."fl_addr" \(zip\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	// addr-specific tlid index (for zip_lookup_all join)
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_fl_addr_tlid" ON "tiger_data"."fl_addr" \(tlid\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	// --- EDGES (has geom: SRID check + create + inherit + GIST + tlid) ---
	mockSRIDNotExists(mock, "tiger_data", "fl_edges")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."fl_edges" \(LIKE "tiger_data"."edges" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	// GIST spatial index
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_fl_edges_the_geom" ON "tiger_data"."fl_edges" USING GIST \(the_geom\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	// edges-specific tlid index
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_fl_edges_tlid" ON "tiger_data"."fl_edges" \(tlid\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	// --- FEATNAMES (no geom, tlid index) ---
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."fl_featnames" \(LIKE "tiger_data"."featnames" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	// featnames-specific tlid index
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_fl_featnames_tlid" ON "tiger_data"."fl_featnames" \(tlid\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	err = CreateStateTables(context.Background(), mock, "FL", products)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateStateTables_CreateError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "PLACE", Table: "place", PerState: true, GeomType: "MULTIPOLYGON"},
	}

	mockSRIDNotExists(mock, "tiger_data", "fl_place")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."fl_place"`).
		WillReturnError(fmt.Errorf("permission denied"))

	err = CreateStateTables(context.Background(), mock, "FL", products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create tiger_data.fl_place")
}

func TestCreateStateTables_InheritError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "PLACE", Table: "place", PerState: true, GeomType: "MULTIPOLYGON"},
	}

	mockSRIDNotExists(mock, "tiger_data", "fl_place")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."fl_place"`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnError(fmt.Errorf("inherit failed"))

	err = CreateStateTables(context.Background(), mock, "FL", products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inherit")
}

func TestCreateStateTables_GISTError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "EDGES", Table: "edges", PerCounty: true, GeomType: "MULTILINESTRING"},
	}

	mockSRIDNotExists(mock, "tiger_data", "fl_edges")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."fl_edges"`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_fl_edges_the_geom"`).
		WillReturnError(fmt.Errorf("gist index failed"))

	err = CreateStateTables(context.Background(), mock, "FL", products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GIST index")
}

func TestTableColumns(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"column_name"}).
		AddRow("statefp").
		AddRow("countyfp").
		AddRow("name").
		AddRow("the_geom")

	mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
		WithArgs("county_all").
		WillReturnRows(rows)

	cols, err := TableColumns(context.Background(), mock, "county_all")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{
		"statefp":  true,
		"countyfp": true,
		"name":     true,
		"the_geom": true,
	}, cols)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTableColumns_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
		WithArgs("bad_table").
		WillReturnError(fmt.Errorf("relation does not exist"))

	cols, err := TableColumns(context.Background(), mock, "bad_table")
	require.Error(t, err)
	assert.Nil(t, cols)
	assert.Contains(t, err.Error(), "query columns")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPopulateLookups(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Expect all lookup INSERT/UPDATE queries to succeed.
	mock.ExpectExec("INSERT INTO tiger.state_lookup").
		WillReturnResult(pgxmock.NewResult("INSERT", 51))
	mock.ExpectExec("INSERT INTO tiger.county_lookup").
		WillReturnResult(pgxmock.NewResult("INSERT", 3143))
	mock.ExpectExec("INSERT INTO tiger.place_lookup").
		WillReturnResult(pgxmock.NewResult("INSERT", 29000))
	mock.ExpectExec("INSERT INTO tiger.zip_lookup_all").
		WillReturnResult(pgxmock.NewResult("INSERT", 41000))
	mock.ExpectExec("INSERT INTO tiger.countysub_lookup").
		WillReturnResult(pgxmock.NewResult("INSERT", 35000))
	mock.ExpectExec("INSERT INTO tiger.zip_state").
		WillReturnResult(pgxmock.NewResult("INSERT", 38000))
	mock.ExpectExec("INSERT INTO tiger.zip_state_loc").
		WillReturnResult(pgxmock.NewResult("INSERT", 11000))
	mock.ExpectExec("INSERT INTO tiger.zip_lookup_base").
		WillReturnResult(pgxmock.NewResult("INSERT", 37000))
	mock.ExpectExec("INSERT INTO tiger.zip_lookup").
		WillReturnResult(pgxmock.NewResult("INSERT", 37000))
	mock.ExpectExec("UPDATE tiger.zcta5").
		WillReturnResult(pgxmock.NewResult("UPDATE", 33000))
	// ANALYZE for each critical table.
	mock.MatchExpectationsInOrder(false)
	for range 12 {
		mock.ExpectExec("ANALYZE").
			WillReturnResult(pgxmock.NewResult("ANALYZE", 0))
	}

	err = PopulateLookups(context.Background(), mock)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPopulateLookups_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// First lookup succeeds, second fails.
	mock.ExpectExec("INSERT INTO tiger.state_lookup").
		WillReturnResult(pgxmock.NewResult("INSERT", 51))
	mock.ExpectExec("INSERT INTO tiger.county_lookup").
		WillReturnError(fmt.Errorf("table does not exist"))

	err = PopulateLookups(context.Background(), mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "populate county_lookup")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateParentTables_ZCTA_PKError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "ZCTA520", Table: "zcta5", FileKey: "zcta520", National: true, GeomType: "MULTIPOLYGON"},
	}

	// SRID check (table doesn't exist yet).
	mockSRIDNotExists(mock, "tiger_data", "zcta5")
	// CREATE TABLE succeeds.
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."zcta5" \(LIKE "tiger"."zcta5" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	// Drop PK DO block fails — should be warned but not error.
	mock.ExpectExec("DO").
		WillReturnError(fmt.Errorf("constraint not found"))
	// Drop NOT NULL also fails — should be warned but not error.
	mock.ExpectExec(`ALTER TABLE "tiger_data"."zcta5" ALTER COLUMN statefp DROP NOT NULL`).
		WillReturnError(fmt.Errorf("column does not have NOT NULL"))

	// load_status tracking table
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS tiger_data.load_status").
		WillReturnResult(pgxmock.NewResult("CREATE", 0))

	err = CreateParentTables(context.Background(), mock, products)
	require.NoError(t, err, "ZCTA PK/NOT NULL errors should be non-fatal")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateParentTables_InheritError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "STATE", Table: "state_all", TemplateTable: "state", National: true, GeomType: "MULTIPOLYGON"},
	}

	mockSRIDNotExists(mock, "tiger_data", "state_all")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."state_all" \(LIKE "tiger"."state" INCLUDING ALL\)`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnError(fmt.Errorf("inherit failed"))

	err = CreateParentTables(context.Background(), mock, products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inherit")
	require.NoError(t, mock.ExpectationsWereMet())
	// No load_status expectation needed — function returns early on error.
}

func TestCreateStateTables_ZipIndexError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "ADDR", Table: "addr", PerCounty: true, GeomType: ""},
	}

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."tx_addr"`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_tx_addr_zip"`).
		WillReturnError(fmt.Errorf("index creation failed"))

	err = CreateStateTables(context.Background(), mock, "TX", products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zip index")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateStateTables_FeatNamesTlidError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "FEATNAMES", Table: "featnames", PerCounty: true, GeomType: ""},
	}

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."tx_featnames"`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_tx_featnames_tlid"`).
		WillReturnError(fmt.Errorf("tlid index failed"))

	err = CreateStateTables(context.Background(), mock, "TX", products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tlid index")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateStateTables_EdgesTlidError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	products := []Product{
		{Name: "EDGES", Table: "edges", PerCounty: true, GeomType: "MULTILINESTRING"},
	}

	mockSRIDNotExists(mock, "tiger_data", "tx_edges")
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "tiger_data"."tx_edges"`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectExec("DO").
		WillReturnResult(pgxmock.NewResult("DO", 0))
	// GIST index succeeds.
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_tx_edges_the_geom"`).
		WillReturnResult(pgxmock.NewResult("CREATE", 0))
	// tlid index fails.
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_tx_edges_tlid"`).
		WillReturnError(fmt.Errorf("tlid index failed"))

	err = CreateStateTables(context.Background(), mock, "TX", products)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tlid index")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTableColumns_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Return a row with a type that can't be scanned into a string.
	rows := pgxmock.NewRows([]string{"column_name"}).
		AddRow("statefp").
		RowError(0, fmt.Errorf("scan error"))

	mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
		WithArgs("test_table").
		WillReturnRows(rows)

	cols, err := TableColumns(context.Background(), mock, "test_table")
	require.Error(t, err)
	assert.Nil(t, cols)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRenameColumns_WithRenames(t *testing.T) {
	// Test that a ZCTA table with only some columns being renamed works correctly.
	result := &ParseResult{
		Columns: []string{"zcta5ce20", "statefp", "funcstat20"},
		Rows:    [][]any{{"10001", "36", "S"}},
	}

	out := RenameColumns(result, "zcta5")

	// zcta5ce20 -> zcta5ce, funcstat20 -> funcstat, statefp unchanged.
	assert.Equal(t, []string{"zcta5ce", "statefp", "funcstat"}, out.Columns)
	assert.Equal(t, result.Rows, out.Rows, "rows should be unchanged")
}

func TestComputeDerivedColumns_MissingSource(t *testing.T) {
	// When a source column for the derived column doesn't exist, skip it.
	result := &ParseResult{
		Columns: []string{"statefp", "name"}, // missing countyfp
		Rows:    [][]any{{"48", "Texas"}},
	}

	out := ComputeDerivedColumns(result, "county_all")
	// cntyidfp requires statefp+countyfp; countyfp is missing, so no derived column.
	assert.Equal(t, result, out)
}
