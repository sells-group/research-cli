package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/readmodel"
	"github.com/sells-group/research-cli/internal/store"
)

// ListCompanies handles GET /companies with pagination and search.
func (h *Handlers) ListCompanies(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.requireCompanies(w, r) {
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	search := r.URL.Query().Get("search")

	companies, total, err := h.readModel.Companies.ListCompanies(ctx, readmodel.CompaniesFilter{
		Search: search,
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		zap.L().Error("list companies failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to count companies")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"companies": companies,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
	})
}

// parseCompanyID extracts and validates the company ID from the URL path.
func parseCompanyID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_id", "company id must be an integer")
		return 0, false
	}
	return id, true
}

// GetCompanyHandler handles GET /companies/{id}.
func (h *Handlers) GetCompanyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}
	id, ok := parseCompanyID(w, r)
	if !ok {
		return
	}

	comp, err := h.readModel.Companies.GetCompany(ctx, id)
	if err != nil {
		zap.L().Error("get company failed", zap.Int64("id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get company")
		return
	}
	if comp == nil {
		WriteError(w, r, http.StatusNotFound, "not_found", "company not found")
		return
	}

	WriteJSON(w, http.StatusOK, comp)
}

// GetCompanyIdentifiers handles GET /companies/{id}/identifiers.
func (h *Handlers) GetCompanyIdentifiers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}
	id, ok := parseCompanyID(w, r)
	if !ok {
		return
	}

	identifiers, err := h.readModel.Companies.ListCompanyIdentifiers(ctx, id)
	if err != nil {
		zap.L().Error("get identifiers failed", zap.Int64("company_id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get identifiers")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"company_id":  id,
		"identifiers": identifiers,
	})
}

// GetCompanyAddresses handles GET /companies/{id}/addresses.
func (h *Handlers) GetCompanyAddresses(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}
	id, ok := parseCompanyID(w, r)
	if !ok {
		return
	}

	addresses, err := h.readModel.Companies.ListCompanyAddresses(ctx, id)
	if err != nil {
		zap.L().Error("get addresses failed", zap.Int64("company_id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get addresses")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"company_id": id,
		"addresses":  addresses,
	})
}

// GetCompanyMatches handles GET /companies/{id}/matches.
func (h *Handlers) GetCompanyMatches(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}
	id, ok := parseCompanyID(w, r)
	if !ok {
		return
	}

	matches, err := h.readModel.Companies.ListCompanyMatches(ctx, id)
	if err != nil {
		zap.L().Error("get matches failed", zap.Int64("company_id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get matches")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"company_id": id,
		"matches":    matches,
	})
}

// GetCompanyMSAs handles GET /companies/{id}/msas.
func (h *Handlers) GetCompanyMSAs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}
	id, ok := parseCompanyID(w, r)
	if !ok {
		return
	}

	msas, err := h.readModel.Companies.ListCompanyMSAs(ctx, id)
	if err != nil {
		zap.L().Error("get company MSAs failed", zap.Int64("company_id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get MSAs")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"company_id": id,
		"msas":       msas,
	})
}

// GetCompanyRuns handles GET /companies/{id}/runs.
func (h *Handlers) GetCompanyRuns(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}
	id, ok := parseCompanyID(w, r)
	if !ok {
		return
	}

	// Look up the company to get its URL for filtering runs.
	comp, err := h.readModel.Companies.GetCompany(ctx, id)
	if err != nil {
		zap.L().Error("get company for runs failed", zap.Int64("id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to get company")
		return
	}
	if comp == nil {
		WriteError(w, r, http.StatusNotFound, "not_found", "company not found")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}

	companyURL := companyRunURL(comp)
	runs, err := h.store.ListRuns(ctx, store.RunFilter{
		CompanyURL: companyURL,
		Limit:      limit,
	})
	if err != nil {
		zap.L().Error("list company runs failed", zap.Int64("company_id", id), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to list runs")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"company_id": id,
		"runs":       runs,
	})
}

// SearchCompanies handles GET /companies/search.
func (h *Handlers) SearchCompanies(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !h.requireCompanies(w, r) {
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		WriteError(w, r, http.StatusBadRequest, "missing_name", "name query parameter is required")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}

	companies, err := h.readModel.Companies.SearchCompanies(ctx, name, limit)
	if err != nil {
		zap.L().Error("search companies failed", zap.String("name", name), zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to search companies")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"companies": companies,
		"total":     len(companies),
	})
}

// geoJSONFeature represents a single GeoJSON feature.
type geoJSONFeature struct {
	Type       string         `json:"type"`
	Geometry   geoJSONPoint   `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

// geoJSONPoint is a GeoJSON point geometry.
type geoJSONPoint struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

// geoJSONCollection is a GeoJSON FeatureCollection.
type geoJSONCollection struct {
	Type     string           `json:"type"`
	Features []geoJSONFeature `json:"features"`
}

// CompaniesGeoJSON handles GET /companies/geojson.
func (h *Handlers) CompaniesGeoJSON(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if !h.requireCompanies(w, r) {
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 1000
	}

	points, err := h.readModel.Companies.ListCompanyGeoPoints(ctx, limit)
	if err != nil {
		zap.L().Error("geojson query failed", zap.Error(err))
		WriteError(w, r, http.StatusInternalServerError, "internal", "failed to query addresses")
		return
	}

	var features []geoJSONFeature
	for _, point := range points {
		props := map[string]any{
			"address_id": point.AddressID,
			"company_id": point.CompanyID,
			"name":       point.Name,
			"domain":     point.Domain,
			"street":     point.Street,
			"city":       point.City,
			"state":      point.State,
			"zip_code":   point.ZipCode,
		}
		if point.EnrichmentScore != nil {
			props["score"] = *point.EnrichmentScore
		}
		features = append(features, geoJSONFeature{
			Type: "Feature",
			Geometry: geoJSONPoint{
				Type:        "Point",
				Coordinates: []float64{point.Longitude, point.Latitude},
			},
			Properties: props,
		})
	}

	if features == nil {
		features = []geoJSONFeature{}
	}

	WriteJSON(w, http.StatusOK, geoJSONCollection{
		Type:     "FeatureCollection",
		Features: features,
	})
}

func companyRunURL(comp *company.CompanyRecord) string {
	candidates := []string{comp.Website, comp.Domain}
	for _, candidate := range candidates {
		if normalized := normalizeCompanyURL(candidate); normalized != "" {
			return normalized
		}
	}
	return ""
}

func normalizeCompanyURL(raw string) string {
	raw = strings.TrimSpace(strings.TrimRight(raw, "/"))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	return "https://" + raw
}
