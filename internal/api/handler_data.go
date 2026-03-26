package api

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/apicache"
	"github.com/sells-group/research-cli/internal/readmodel"
)

// validIdentifier matches safe SQL identifiers (lowercase letters, digits, underscores).
var validIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// allowedSortDirs is the set of valid sort directions.
var allowedSortDirs = map[string]bool{"asc": true, "desc": true}

// allowedAggFuncs is the set of valid aggregate functions.
var allowedAggFuncs = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
}

// validateTableNameHTTP validates a table name from the URL and writes an error response if invalid.
func (h *Handlers) validateTableNameHTTP(w http.ResponseWriter, r *http.Request, table string) bool {
	if !validIdentifier.MatchString(table) {
		WriteError(w, r, http.StatusBadRequest, "invalid_table", "invalid table name")
		return false
	}

	if !h.requireData(w, r) {
		return false
	}

	exists, err := h.readModel.Data.TableExists(r.Context(), table)
	if err != nil {
		zap.L().Error("validate table name failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to validate table")
		return false
	}
	if !exists {
		WriteError(w, r, http.StatusNotFound, "not_found", "table not found in fed_data schema")
		return false
	}
	return true
}

func (h *Handlers) validateColumnNameHTTP(w http.ResponseWriter, r *http.Request, table, column string) bool {
	if !validIdentifier.MatchString(column) {
		WriteError(w, r, http.StatusBadRequest, "invalid_column", "invalid column name")
		return false
	}

	exists, err := h.readModel.Data.ColumnExists(r.Context(), table, column)
	if err != nil {
		zap.L().Error("validate column name failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to validate column")
		return false
	}
	if !exists {
		WriteError(w, r, http.StatusBadRequest, "invalid_column", "column not found in fed_data table")
		return false
	}
	return true
}

// ListDataTables handles GET /data/tables.
func (h *Handlers) ListDataTables(w http.ResponseWriter, r *http.Request) {
	if cached, ok := h.cache.Get(apicache.KeyDataTables); ok {
		writeCachedJSON(w, cached)
		return
	}

	if !h.requireData(w, r) {
		return
	}

	tables, err := h.readModel.Data.ListDataTables(r.Context())
	if err != nil {
		zap.L().Error("list data tables failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to list tables")
		return
	}

	resp := map[string]any{"tables": tables}
	if err := h.cache.Set(apicache.KeyDataTables, resp, 5*time.Minute); err != nil {
		zap.L().Warn("cache data tables failed", zap.Error(err))
	}
	WriteJSON(w, http.StatusOK, resp)
}

// QueryDataTable handles GET /data/{table}.
func (h *Handlers) QueryDataTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")

	if !h.requireData(w, r) {
		return
	}

	if !h.validateTableNameHTTP(w, r, table) {
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	// Build query with optional sorting.
	sortCol := r.URL.Query().Get("sort")
	sortDir := strings.ToLower(r.URL.Query().Get("dir"))
	if !allowedSortDirs[sortDir] {
		sortDir = "asc"
	}
	if sortCol != "" && !h.validateColumnNameHTTP(w, r, table, sortCol) {
		return
	}

	// Optional search filter on a specific column.
	searchCol := r.URL.Query().Get("search_col")
	searchVal := r.URL.Query().Get("search_val")
	if searchCol != "" && !h.validateColumnNameHTTP(w, r, table, searchCol) {
		return
	}

	result, err := h.readModel.Data.QueryDataTable(r.Context(), readmodel.DataQueryParams{
		Table:         table,
		Limit:         limit,
		Offset:        offset,
		SortColumn:    sortCol,
		SortDirection: sortDir,
		SearchColumn:  searchCol,
		SearchValue:   searchVal,
	})
	if err != nil {
		zap.L().Error("query data table failed", zap.String("table", table), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to query table")
		return
	}

	WriteJSON(w, http.StatusOK, result)
}

// GetDataRow handles GET /data/{table}/{id}.
func (h *Handlers) GetDataRow(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	id := chi.URLParam(r, "id")

	if !h.requireData(w, r) {
		return
	}

	if !h.validateTableNameHTTP(w, r, table) {
		return
	}

	row, err := h.readModel.Data.GetDataRow(r.Context(), table, id)
	if err != nil {
		switch {
		case errors.Is(err, readmodel.ErrRowNotFound):
			WriteError(w, r, http.StatusNotFound, "not_found", "row not found")
		default:
			zap.L().Error("get data row failed", zap.String("table", table), zap.Error(err))
			WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get row")
		}
		return
	}

	WriteJSON(w, http.StatusOK, row)
}

// GetDataFilters handles GET /data/{table}/filters/{column}.
func (h *Handlers) GetDataFilters(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	column := chi.URLParam(r, "column")

	if !h.requireData(w, r) {
		return
	}

	if !h.validateTableNameHTTP(w, r, table) {
		return
	}
	if !h.validateColumnNameHTTP(w, r, table, column) {
		return
	}

	// Get distinct values with a reasonable limit.
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	values, err := h.readModel.Data.GetDataFilterValues(r.Context(), table, column, limit)
	if err != nil {
		zap.L().Error("get data filters failed", zap.String("table", table), zap.String("column", column), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get filter values")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"table":  table,
		"column": column,
		"values": values,
	})
}

// AggregateData handles GET /data/{table}/aggregate.
func (h *Handlers) AggregateData(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")

	if !h.requireData(w, r) {
		return
	}

	if !h.validateTableNameHTTP(w, r, table) {
		return
	}

	groupBy := r.URL.Query().Get("group_by")
	aggFunc := strings.ToLower(r.URL.Query().Get("func"))
	aggCol := r.URL.Query().Get("col")

	if groupBy == "" || !h.validateColumnNameHTTP(w, r, table, groupBy) {
		WriteError(w, r, http.StatusBadRequest, "invalid_group_by", "group_by must be a valid column name")
		return
	}

	if aggFunc == "" {
		aggFunc = "count"
	}
	if !allowedAggFuncs[aggFunc] {
		WriteError(w, r, http.StatusBadRequest, "invalid_func", "func must be one of: count, sum, avg, min, max")
		return
	}

	// For count, we don't need a column. For others, we do.
	if aggFunc != "count" {
		if aggCol == "" || !h.validateColumnNameHTTP(w, r, table, aggCol) {
			WriteError(w, r, http.StatusBadRequest, "invalid_col", "col is required for non-count aggregations and must be a valid column name")
			return
		}
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	result, err := h.readModel.Data.AggregateData(r.Context(), readmodel.DataAggregateParams{
		Table:       table,
		GroupBy:     groupBy,
		Aggregation: aggFunc,
		ValueField:  aggCol,
		Limit:       limit,
	})
	if err != nil {
		zap.L().Error("aggregate data failed", zap.String("table", table), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to aggregate data")
		return
	}

	WriteJSON(w, http.StatusOK, result)
}
