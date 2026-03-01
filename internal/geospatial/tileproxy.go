package geospatial

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// TileProxy proxies basemap raster tiles from an upstream tile server (e.g., OSM, Stadia).
type TileProxy struct {
	baseURL string
	format  string
	client  *http.Client
	cache   *TileCache
}

// NewTileProxy creates a new basemap tile proxy.
func NewTileProxy(baseURL, format string, cache *TileCache) *TileProxy {
	return &TileProxy{
		baseURL: baseURL,
		format:  format,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache: cache,
	}
}

// Fetch retrieves a basemap tile from the upstream server or cache.
func (p *TileProxy) Fetch(ctx context.Context, z, x, y int) ([]byte, string, error) {
	layerKey := "basemap"

	// Check cache.
	if p.cache != nil {
		if cached := p.cache.Get(layerKey, z, x, y); cached != nil {
			return cached, p.contentType(), nil
		}
	}

	url := fmt.Sprintf("%s/%d/%d/%d.%s", p.baseURL, z, x, y, p.format)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", eris.Wrap(err, "geo: create basemap request")
	}
	req.Header.Set("User-Agent", "research-cli/1.0")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", eris.Wrap(err, "geo: fetch basemap tile")
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, "", eris.Errorf("geo: basemap upstream returned %d for %s", resp.StatusCode, url)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", eris.Wrap(err, "geo: read basemap tile body")
	}

	// Cache the result.
	if p.cache != nil {
		p.cache.Put(layerKey, z, x, y, data)
	}

	zap.L().Debug("geo: fetched basemap tile", zap.String("url", url), zap.Int("bytes", len(data)))
	return data, p.contentType(), nil
}

// contentType returns the MIME type for the basemap tile format.
func (p *TileProxy) contentType() string {
	switch p.format {
	case "png":
		return "image/png"
	case "jpg", "jpeg":
		return "image/jpeg"
	case "webp":
		return "image/webp"
	default:
		return "application/octet-stream"
	}
}

// ServeHTTP implements http.Handler for the tile proxy.
// Expected path format: /{z}/{x}/{y}.{format}
func (p *TileProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var z, x, y int
	var ext string
	if _, err := fmt.Sscanf(r.URL.Path, "/%d/%d/%d.%s", &z, &x, &y, &ext); err != nil {
		http.Error(w, "invalid tile path", http.StatusBadRequest)
		return
	}

	data, ct, err := p.Fetch(r.Context(), z, x, y)
	if err != nil {
		zap.L().Error("basemap tile fetch failed", zap.Error(err))
		http.Error(w, "upstream fetch failed", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write(data)
}
