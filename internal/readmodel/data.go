package readmodel

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/db"
)

type postgresData struct {
	pool db.Pool
}

type tableColumnInfo struct {
	TableName  string
	ColumnName string
	DataType   string
}

var systemTables = map[string]bool{
	"sync_log":          true,
	"schema_migrations": true,
}

// TableExists implements DataReader.
func (p *postgresData) TableExists(ctx context.Context, table string) (bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = 'fed_data' AND table_name = $1
		)`,
		table,
	).Scan(&exists)
	if err != nil {
		return false, eris.Wrap(err, "readmodel: check table exists")
	}
	return exists, nil
}

// ColumnExists implements DataReader.
func (p *postgresData) ColumnExists(ctx context.Context, table, column string) (bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'fed_data' AND table_name = $1 AND column_name = $2
		)`,
		table, column,
	).Scan(&exists)
	if err != nil {
		return false, eris.Wrap(err, "readmodel: check column exists")
	}
	return exists, nil
}

// ListDataTables implements DataReader.
func (p *postgresData) ListDataTables(ctx context.Context) ([]TableMeta, error) {
	colRows, err := p.pool.Query(ctx, `
		SELECT table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'fed_data'
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: list data tables")
	}
	defer colRows.Close()

	type colInfo struct {
		name     string
		dataType string
	}

	tableColumns := make(map[string][]colInfo)
	var tableOrder []string
	seen := make(map[string]bool)
	for colRows.Next() {
		var col tableColumnInfo
		if err := colRows.Scan(&col.TableName, &col.ColumnName, &col.DataType); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan table column info")
		}
		if systemTables[col.TableName] {
			continue
		}
		if !seen[col.TableName] {
			seen[col.TableName] = true
			tableOrder = append(tableOrder, col.TableName)
		}
		tableColumns[col.TableName] = append(tableColumns[col.TableName], colInfo{
			name:     col.ColumnName,
			dataType: col.DataType,
		})
	}
	if err := colRows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate table columns")
	}

	rowCounts := make(map[string]int64)
	countRows, err := p.pool.Query(ctx, `
		SELECT relname, COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = 'fed_data'`)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: list table row counts")
	}
	defer countRows.Close()

	for countRows.Next() {
		var name string
		var count int64
		if err := countRows.Scan(&name, &count); err != nil {
			return nil, eris.Wrap(err, "readmodel: scan table row count")
		}
		rowCounts[name] = count
	}
	if err := countRows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate table row counts")
	}

	tables := make([]TableMeta, 0, len(tableOrder))
	for _, name := range tableOrder {
		cols := tableColumns[name]
		tableCols := make([]TableColumn, len(cols))
		for i, c := range cols {
			tableCols[i] = TableColumn{
				Key:      c.name,
				Label:    c.name,
				Type:     sqlTypeToFrontendType(c.dataType),
				Sortable: true,
			}
		}
		tables = append(tables, TableMeta{
			ID:                name,
			Name:              name,
			Category:          tableCategory(name),
			EstimatedRowCount: rowCounts[name],
			Columns:           tableCols,
		})
	}

	return tables, nil
}

// QueryDataTable implements DataReader.
func (p *postgresData) QueryDataTable(ctx context.Context, params DataQueryParams) (*DataQueryResult, error) {
	if ok, err := p.TableExists(ctx, params.Table); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrTableNotFound
	}

	if params.SortColumn != "" {
		ok, err := p.ColumnExists(ctx, params.Table, params.SortColumn)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrColumnNotFound
		}
	}
	if params.SearchColumn != "" {
		ok, err := p.ColumnExists(ctx, params.Table, params.SearchColumn)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrColumnNotFound
		}
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}

	orderClause := "ORDER BY 1"
	if params.SortColumn != "" {
		orderClause = fmt.Sprintf("ORDER BY %s %s", params.SortColumn, params.SortDirection)
	}

	var (
		whereClause string
		whereArgs   []any
	)
	if params.SearchColumn != "" && params.SearchValue != "" {
		whereClause = fmt.Sprintf("WHERE %s::text ILIKE $1", params.SearchColumn)
		whereArgs = []any{"%" + params.SearchValue + "%"}
	}

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM fed_data.%s %s`, params.Table, whereClause)
	var totalRows int64
	if err := p.pool.QueryRow(ctx, countQuery, whereArgs...).Scan(&totalRows); err != nil {
		return nil, eris.Wrap(err, "readmodel: count table rows")
	}

	var (
		query string
		args  []any
	)
	if whereClause != "" {
		query = fmt.Sprintf(`SELECT * FROM fed_data.%s %s %s LIMIT $2 OFFSET $3`, params.Table, whereClause, orderClause)
		args = append(whereArgs, limit, params.Offset)
	} else {
		query = fmt.Sprintf(`SELECT * FROM fed_data.%s %s LIMIT $1 OFFSET $2`, params.Table, orderClause)
		args = []any{limit, params.Offset}
	}

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: query data table")
	}
	defer rows.Close()

	colNames := fieldNames(rows.FieldDescriptions())
	results := make([]map[string]any, 0, limit)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, eris.Wrap(err, "readmodel: read row values")
		}

		row := make(map[string]any, len(colNames))
		for i, name := range colNames {
			row[name] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate table rows")
	}

	result := &DataQueryResult{
		Rows:      results,
		TotalRows: totalRows,
		Page:      params.Offset/limit + 1,
		PageSize:  limit,
	}
	if params.SortColumn != "" {
		result.Sort = &DataQuerySort{
			Column:    params.SortColumn,
			Direction: params.SortDirection,
		}
	}
	if params.SearchColumn != "" && params.SearchValue != "" {
		result.Filter = &DataQueryFilter{
			Column: params.SearchColumn,
			Value:  params.SearchValue,
		}
	}

	return result, nil
}

// GetDataRow implements DataReader.
func (p *postgresData) GetDataRow(ctx context.Context, table, id string) (map[string]any, error) {
	if ok, err := p.TableExists(ctx, table); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrTableNotFound
	}

	query := fmt.Sprintf(`SELECT * FROM fed_data.%s WHERE id::text = $1 LIMIT 1`, table)
	rows, err := p.pool.Query(ctx, query, id)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: query data row")
	}
	defer rows.Close()

	colNames := fieldNames(rows.FieldDescriptions())
	if !rows.Next() {
		return nil, ErrRowNotFound
	}

	values, err := rows.Values()
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: read data row values")
	}

	row := make(map[string]any, len(colNames))
	for i, name := range colNames {
		row[name] = values[i]
	}
	return row, nil
}

// GetDataFilterValues implements DataReader.
func (p *postgresData) GetDataFilterValues(ctx context.Context, table, column string, limit int) ([]any, error) {
	if ok, err := p.TableExists(ctx, table); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrTableNotFound
	}
	if ok, err := p.ColumnExists(ctx, table, column); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrColumnNotFound
	}

	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(
		`SELECT DISTINCT %s FROM fed_data.%s WHERE %s IS NOT NULL ORDER BY %s LIMIT $1`,
		column, table, column, column,
	)
	rows, err := p.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: query data filter values")
	}
	defer rows.Close()

	var values []any
	for rows.Next() {
		rowValues, err := rows.Values()
		if err != nil {
			return nil, eris.Wrap(err, "readmodel: read filter row")
		}
		if len(rowValues) > 0 {
			values = append(values, rowValues[0])
		}
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate filter values")
	}

	if values == nil {
		values = []any{}
	}
	return values, nil
}

// AggregateData implements DataReader.
func (p *postgresData) AggregateData(ctx context.Context, params DataAggregateParams) (*DataAggregateResult, error) {
	if ok, err := p.TableExists(ctx, params.Table); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrTableNotFound
	}
	if ok, err := p.ColumnExists(ctx, params.Table, params.GroupBy); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrColumnNotFound
	}
	if params.ValueField != "" {
		ok, err := p.ColumnExists(ctx, params.Table, params.ValueField)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrColumnNotFound
		}
	}

	aggregation := strings.ToLower(params.Aggregation)
	if aggregation == "" {
		aggregation = "count"
	}

	var aggExpr string
	if aggregation == "count" {
		aggExpr = "COUNT(*)"
		params.ValueField = ""
	} else {
		aggExpr = fmt.Sprintf("%s(%s)", strings.ToUpper(aggregation), params.ValueField)
	}

	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(
		`SELECT %s, %s AS value FROM fed_data.%s GROUP BY %s ORDER BY value DESC LIMIT $1`,
		params.GroupBy, aggExpr, params.Table, params.GroupBy,
	)
	rows, err := p.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, eris.Wrap(err, "readmodel: aggregate data")
	}
	defer rows.Close()

	result := &DataAggregateResult{
		Table:       params.Table,
		GroupBy:     params.GroupBy,
		Aggregation: aggregation,
		ValueField:  params.ValueField,
		Rows:        []DataAggregateRow{},
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, eris.Wrap(err, "readmodel: read aggregate row")
		}
		if len(values) >= 2 {
			result.Rows = append(result.Rows, DataAggregateRow{
				Key:   values[0],
				Value: values[1],
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "readmodel: iterate aggregate rows")
	}

	return result, nil
}

func fieldNames(fields []pgconn.FieldDescription) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

func sqlTypeToFrontendType(dt string) string {
	switch dt {
	case "integer", "bigint", "smallint", "numeric", "double precision", "real":
		return "number"
	case "money":
		return "currency"
	case "date", "timestamp", "timestamptz", "timestamp with time zone", "timestamp without time zone":
		return "date"
	default:
		return "text"
	}
}

func tableCategory(name string) string {
	prefixes := []struct {
		prefix   string
		category string
	}{
		{"cbp", "Census"},
		{"susb", "Census"},
		{"econ_census", "Census"},
		{"economic_census", "Census"},
		{"abs", "Census"},
		{"asm", "Census"},
		{"nes", "Census"},
		{"m3", "Census"},
		{"naics_codes", "Census"},
		{"sic_crosswalk", "Census"},
		{"fips_codes", "Census"},
		{"building_permits", "Census"},
		{"lehd", "Census"},
		{"qcew", "BLS"},
		{"oews", "BLS"},
		{"eci", "BLS"},
		{"cps", "BLS"},
		{"laus", "BLS"},
		{"adv", "SEC"},
		{"holdings", "SEC"},
		{"f13", "SEC"},
		{"edgar", "SEC"},
		{"form_d", "SEC"},
		{"xbrl", "SEC"},
		{"ia_", "SEC"},
		{"sec_enforcement", "SEC"},
		{"ncen", "SEC"},
		{"pe_", "SEC"},
		{"fpds", "Contracts"},
		{"usaspending", "Contracts"},
		{"ppp", "SBA"},
		{"sba", "SBA"},
		{"form_5500", "DOL"},
		{"eo_bmf", "IRS"},
		{"irs_soi", "IRS"},
		{"osha", "OSHA"},
		{"epa", "EPA"},
		{"fred", "FRED"},
		{"bea", "BEA"},
		{"fdic", "FDIC"},
		{"ncua", "NCUA"},
		{"brokercheck", "FINRA"},
		{"form_bd", "FINRA"},
		{"firm_scores", "System"},
		{"entity_xref", "System"},
		{"sync_log", "System"},
		{"schema_migrations", "System"},
		{"v_", "System"},
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix.prefix) {
			return prefix.category
		}
	}
	return "Other"
}
