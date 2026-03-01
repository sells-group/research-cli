// Package arcgis provides a client for querying Esri ArcGIS FeatureServer REST
// endpoints with automatic pagination. It is used by HIFLD, FEMA, EPA, and
// other government scrapers that publish data via ArcGIS Online.
package arcgis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/fetcher"
)

const defaultPageSize = 2000

// QueryConfig configures an ArcGIS FeatureServer query.
type QueryConfig struct {
	BaseURL   string   // e.g., ".../FeatureServer/0/query"
	Where     string   // SQL WHERE clause (default "1=1")
	OutFields []string // fields to return (default ["*"])
	PageSize  int      // records per request (default 2000)
}

// Feature represents a single ArcGIS feature with attributes and geometry.
type Feature struct {
	Attributes map[string]any `json:"attributes"`
	Geometry   *Geometry      `json:"geometry"`
}

// Geometry holds either point (X/Y) or polyline (Paths) data from ArcGIS.
type Geometry struct {
	X     *float64       `json:"x,omitempty"`
	Y     *float64       `json:"y,omitempty"`
	Paths [][][2]float64 `json:"paths,omitempty"`
}

// Centroid returns the latitude and longitude for this geometry.
// For points it returns Y (lat) and X (lon) directly.
// For polylines it returns the average of all vertex coordinates.
func (g *Geometry) Centroid() (lat, lon float64) {
	if g.X != nil && g.Y != nil {
		return *g.Y, *g.X
	}

	var sumLat, sumLon float64
	var count int
	for _, path := range g.Paths {
		for _, coord := range path {
			sumLon += coord[0]
			sumLat += coord[1]
			count++
		}
	}
	if count == 0 {
		return 0, 0
	}
	return sumLat / float64(count), sumLon / float64(count)
}

// BBox returns the bounding box [minLon, minLat, maxLon, maxLat] for polyline geometries.
// Returns nil for point geometries.
func (g *Geometry) BBox() []float64 {
	if len(g.Paths) == 0 {
		return nil
	}

	minLon, minLat := 180.0, 90.0
	maxLon, maxLat := -180.0, -90.0

	for _, path := range g.Paths {
		for _, coord := range path {
			if coord[0] < minLon {
				minLon = coord[0]
			}
			if coord[0] > maxLon {
				maxLon = coord[0]
			}
			if coord[1] < minLat {
				minLat = coord[1]
			}
			if coord[1] > maxLat {
				maxLat = coord[1]
			}
		}
	}

	return []float64{minLon, minLat, maxLon, maxLat}
}

// Response is the Esri JSON envelope returned by FeatureServer queries.
type Response struct {
	Features              []Feature `json:"features"`
	ExceededTransferLimit bool      `json:"exceededTransferLimit"`
}

// PageCallback is invoked for each page of features during QueryAll pagination.
type PageCallback func(features []Feature) error

// QueryAll pages through all features matching the query configuration, invoking
// the callback for each page. Pagination stops when ExceededTransferLimit is false
// or the callback returns an error.
func QueryAll(ctx context.Context, f fetcher.Fetcher, cfg QueryConfig, callback PageCallback) error {
	log := zap.L().With(zap.String("component", "arcgis"))

	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}

	where := cfg.Where
	if where == "" {
		where = "1=1"
	}

	outFields := "*"
	if len(cfg.OutFields) > 0 {
		for i, field := range cfg.OutFields {
			if i > 0 {
				outFields += ","
			} else {
				outFields = ""
			}
			outFields += field
		}
	}

	offset := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		u, err := buildURL(cfg.BaseURL, where, outFields, pageSize, offset)
		if err != nil {
			return eris.Wrap(err, "arcgis: build query URL")
		}

		log.Debug("fetching page", zap.String("url", u), zap.Int("offset", offset))

		body, err := f.Download(ctx, u)
		if err != nil {
			return eris.Wrapf(err, "arcgis: download page at offset %d", offset)
		}

		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return eris.Wrapf(err, "arcgis: read response at offset %d", offset)
		}

		var resp Response
		if err := json.Unmarshal(data, &resp); err != nil {
			return eris.Wrapf(err, "arcgis: decode response at offset %d", offset)
		}

		if len(resp.Features) > 0 {
			if err := callback(resp.Features); err != nil {
				return eris.Wrap(err, "arcgis: callback error")
			}
		}

		if !resp.ExceededTransferLimit || len(resp.Features) == 0 {
			break
		}

		offset += len(resp.Features)
	}

	return nil
}

// buildURL constructs the ArcGIS query URL with pagination parameters.
func buildURL(baseURL, where, outFields string, pageSize, offset int) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", eris.Wrapf(err, "parse base URL %q", baseURL)
	}

	q := u.Query()
	q.Set("where", where)
	q.Set("outFields", outFields)
	q.Set("returnGeometry", "true")
	q.Set("f", "json")
	q.Set("resultRecordCount", strconv.Itoa(pageSize))
	q.Set("resultOffset", strconv.Itoa(offset))
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// FormatURL is a convenience for building the full FeatureServer query URL from
// the ArcGIS services base and a layer name.
func FormatURL(layer string) string {
	return fmt.Sprintf(
		"https://services1.arcgis.com/Hp6G80Pky0om6HgA/ArcGIS/rest/services/%s/FeatureServer/0/query",
		layer,
	)
}
