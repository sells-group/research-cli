package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fedsync/dataset"
	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

// earthquakeBatchSize is the number of rows per BulkUpsert batch.
const earthquakeBatchSize = 5000

// earthquakeLookbackDays is how far back each monthly sync fetches.
const earthquakeLookbackDays = 60

// defaultMinMagnitude is the minimum earthquake magnitude to fetch (M2.5+ = "felt").
const defaultMinMagnitude = 2.5

// earthquakeCols are the columns written to geo.earthquakes.
var earthquakeCols = []string{
	"event_id", "magnitude", "mag_type", "place", "event_time",
	"depth_km", "status", "tsunami", "significance", "felt",
	"alert", "cdi", "mmi",
	"latitude", "longitude",
	"source", "source_id", "properties",
}

// earthquakeConflictKeys defines the unique constraint columns for upserts.
var earthquakeConflictKeys = []string{"source", "source_id"}

// usRegion defines a bounding box for earthquake queries.
type usRegion struct {
	Name   string
	MinLat float64
	MaxLat float64
	MinLon float64
	MaxLon float64
}

// usRegions covers the continental US, Alaska, and Hawaii.
var usRegions = []usRegion{
	{Name: "continental", MinLat: 24.5, MaxLat: 49.4, MinLon: -125.0, MaxLon: -66.0},
	{Name: "alaska", MinLat: 51.0, MaxLat: 72.0, MinLon: -180.0, MaxLon: -130.0},
	{Name: "hawaii", MinLat: 18.5, MaxLat: 22.5, MinLon: -161.0, MaxLon: -154.0},
}

// earthquakeResponse is the top-level USGS FDSN GeoJSON response.
type earthquakeResponse struct {
	Features []earthquakeFeature `json:"features"`
}

// earthquakeFeature is a single earthquake event in the GeoJSON FeatureCollection.
type earthquakeFeature struct {
	Properties earthquakeProps `json:"properties"`
	Geometry   earthquakeGeom  `json:"geometry"`
	ID         string          `json:"id"`
}

// earthquakeProps holds the USGS earthquake event properties.
type earthquakeProps struct {
	Mag     *float64 `json:"mag"`
	Place   string   `json:"place"`
	Time    int64    `json:"time"`    // epoch milliseconds
	Updated int64    `json:"updated"` // epoch milliseconds
	URL     string   `json:"url"`
	Detail  string   `json:"detail"`
	Status  string   `json:"status"`
	Tsunami int      `json:"tsunami"` // 0 or 1
	Sig     int      `json:"sig"`
	Net     string   `json:"net"`
	Code    string   `json:"code"`
	IDs     string   `json:"ids"`     // comma-separated associated event IDs
	Sources string   `json:"sources"` // comma-separated contributing networks
	Types   string   `json:"types"`   // comma-separated product types
	MagType string   `json:"magType"`
	Type    string   `json:"type"`
	Title   string   `json:"title"`
	Felt    *int     `json:"felt"`
	CDI     *float64 `json:"cdi"`
	MMI     *float64 `json:"mmi"`
	Alert   string   `json:"alert"`
	TZ      *int     `json:"tz"` // timezone offset in minutes
	Gap     *float64 `json:"gap"`
	RMS     *float64 `json:"rms"`
	NST     *int     `json:"nst"`
	Dmin    *float64 `json:"dmin"`
}

// earthquakeGeom is the GeoJSON Point geometry for an earthquake epicenter.
type earthquakeGeom struct {
	Coordinates []float64 `json:"coordinates"` // [lon, lat, depth_km]
}

// USGSEarthquakes scrapes earthquake data from the USGS FDSN Event Web Service.
type USGSEarthquakes struct {
	baseURL string // override for testing; empty uses default endpoint
}

// Name implements GeoScraper.
func (s *USGSEarthquakes) Name() string { return "usgs_earthquakes" }

// Table implements GeoScraper.
func (s *USGSEarthquakes) Table() string { return "geo.earthquakes" }

// Category implements GeoScraper.
func (s *USGSEarthquakes) Category() geoscraper.Category { return geoscraper.National }

// Cadence implements GeoScraper.
func (s *USGSEarthquakes) Cadence() geoscraper.Cadence { return geoscraper.Monthly }

// ShouldRun implements GeoScraper.
func (s *USGSEarthquakes) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return dataset.MonthlySchedule(now, lastSync)
}

// Sync implements GeoScraper.
func (s *USGSEarthquakes) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, _ string) (*geoscraper.SyncResult, error) {
	log := zap.L().With(zap.String("scraper", s.Name()))
	log.Info("starting earthquake sync")

	now := time.Now().UTC()
	start := now.AddDate(0, 0, -earthquakeLookbackDays)

	var totalRows int64
	var batch [][]any

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        s.Table(),
			Columns:      earthquakeCols,
			ConflictKeys: earthquakeConflictKeys,
		}, batch)
		if err != nil {
			return eris.Wrap(err, "earthquakes: upsert batch")
		}
		totalRows += n
		batch = batch[:0]
		return nil
	}

	for _, region := range usRegions {
		features, err := s.fetchRegion(ctx, f, start, now, region)
		if err != nil {
			return nil, eris.Wrapf(err, "earthquakes: fetch %s", region.Name)
		}

		log.Info("fetched earthquake region",
			zap.String("region", region.Name),
			zap.Int("features", len(features)))

		for _, feat := range features {
			row := featureToRow(feat)
			if row == nil {
				continue
			}
			batch = append(batch, row)

			if len(batch) >= earthquakeBatchSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := flush(); err != nil {
		return nil, err
	}

	log.Info("earthquake sync complete", zap.Int64("rows", totalRows))
	return &geoscraper.SyncResult{RowsSynced: totalRows}, nil
}

// fetchRegion downloads earthquake GeoJSON for a single bounding box region.
func (s *USGSEarthquakes) fetchRegion(ctx context.Context, f fetcher.Fetcher, start, end time.Time, region usRegion) ([]earthquakeFeature, error) {
	base := usgsURL(s.baseURL, earthquakeBaseURL)

	params := url.Values{
		"format":       {"geojson"},
		"starttime":    {start.Format("2006-01-02")},
		"endtime":      {end.Format("2006-01-02")},
		"minmagnitude": {fmt.Sprintf("%.1f", defaultMinMagnitude)},
		"minlatitude":  {fmt.Sprintf("%.1f", region.MinLat)},
		"maxlatitude":  {fmt.Sprintf("%.1f", region.MaxLat)},
		"minlongitude": {fmt.Sprintf("%.1f", region.MinLon)},
		"maxlongitude": {fmt.Sprintf("%.1f", region.MaxLon)},
	}

	reqURL := base + "?" + params.Encode()
	body, err := f.Download(ctx, reqURL)
	if err != nil {
		return nil, eris.Wrap(err, "download")
	}
	defer body.Close() //nolint:errcheck

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, eris.Wrap(err, "read body")
	}

	var resp earthquakeResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, eris.Wrap(err, "decode geojson")
	}

	return resp.Features, nil
}

// featureToRow converts a GeoJSON earthquake feature into a database row.
// Returns nil if the feature should be skipped (nil magnitude or missing coordinates).
func featureToRow(feat earthquakeFeature) []any {
	if feat.Properties.Mag == nil {
		return nil
	}
	if len(feat.Geometry.Coordinates) < 2 {
		return nil
	}

	lon := feat.Geometry.Coordinates[0]
	lat := feat.Geometry.Coordinates[1]
	var depthKM *float64
	if len(feat.Geometry.Coordinates) >= 3 {
		d := feat.Geometry.Coordinates[2]
		depthKM = &d
	}

	eventTime := time.UnixMilli(feat.Properties.Time).UTC()

	// Overflow properties for JSONB column.
	props := earthquakeOverflow(feat.Properties)

	return []any{
		feat.ID,                              // event_id
		*feat.Properties.Mag,                 // magnitude
		feat.Properties.MagType,              // mag_type
		feat.Properties.Place,                // place
		eventTime,                            // event_time
		depthKM,                              // depth_km
		feat.Properties.Status,               // status
		feat.Properties.Tsunami == 1,         // tsunami
		feat.Properties.Sig,                  // significance
		intPtrToAny(feat.Properties.Felt),    // felt
		nilIfEmpty(feat.Properties.Alert),    // alert
		float64PtrToAny(feat.Properties.CDI), // cdi
		float64PtrToAny(feat.Properties.MMI), // mmi
		lat,                                  // latitude
		lon,                                  // longitude
		usgsSource,                           // source
		feat.ID,                              // source_id
		props,                                // properties
	}
}

// earthquakeOverflow builds the JSONB properties from fields not stored in dedicated columns.
func earthquakeOverflow(p earthquakeProps) []byte {
	overflow := map[string]any{}
	if p.Gap != nil {
		overflow["gap"] = *p.Gap
	}
	if p.RMS != nil {
		overflow["rms"] = *p.RMS
	}
	if p.NST != nil {
		overflow["nst"] = *p.NST
	}
	if p.Dmin != nil {
		overflow["dmin"] = *p.Dmin
	}
	if p.Net != "" {
		overflow["net"] = p.Net
	}
	if p.Code != "" {
		overflow["code"] = p.Code
	}
	if p.IDs != "" {
		overflow["ids"] = p.IDs
	}
	if p.Sources != "" {
		overflow["sources"] = p.Sources
	}
	if p.Types != "" {
		overflow["types"] = p.Types
	}
	if p.URL != "" {
		overflow["url"] = p.URL
	}
	if p.Detail != "" {
		overflow["detail"] = p.Detail
	}
	if p.Updated != 0 {
		overflow["updated"] = p.Updated
	}
	if p.TZ != nil {
		overflow["tz"] = *p.TZ
	}
	if p.Type != "" {
		overflow["type"] = p.Type
	}
	if p.Title != "" {
		overflow["title"] = p.Title
	}
	data, err := json.Marshal(overflow)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// intPtrToAny converts *int to any, returning nil for SQL NULL.
func intPtrToAny(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

// float64PtrToAny converts *float64 to any, returning nil for SQL NULL.
func float64PtrToAny(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

// nilIfEmpty returns nil if the string is empty, otherwise the string itself.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
