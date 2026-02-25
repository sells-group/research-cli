package geocode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/rotisserie/eris"
)

const (
	censusOneLineURL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
	censusBatchURL   = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
	censusBenchmark  = "Public_AR_Current"
)

// censusOneLineResponse is the JSON response from the Census single-address API.
type censusOneLineResponse struct {
	Result struct {
		AddressMatches []censusAddressMatch `json:"addressMatches"`
	} `json:"result"`
}

type censusAddressMatch struct {
	Coordinates struct {
		X float64 `json:"x"` // longitude
		Y float64 `json:"y"` // latitude
	} `json:"coordinates"`
	TigerLine struct {
		Side    string `json:"side"`
		TigerID string `json:"tigerLineId"`
	} `json:"tigerLine"`
	MatchedAddress string `json:"matchedAddress"`
}

// geocodeCensus geocodes a single address using the Census one-line API.
func (g *geocoder) geocodeCensus(ctx context.Context, addr AddressInput) (*Result, error) {
	if err := g.limiter.Wait(ctx); err != nil {
		return nil, eris.Wrap(err, "geocode: census rate limit")
	}

	oneLine := formatOneLine(addr)
	params := url.Values{
		"address":   {oneLine},
		"benchmark": {censusBenchmark},
		"format":    {"json"},
	}

	reqURL := censusOneLineURL + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census build request")
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("geocode: census returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census read body")
	}

	var censusResp censusOneLineResponse
	if err := json.Unmarshal(body, &censusResp); err != nil {
		return nil, eris.Wrap(err, "geocode: census parse response")
	}

	if len(censusResp.Result.AddressMatches) == 0 {
		return &Result{Matched: false, Source: "census"}, nil
	}

	match := censusResp.Result.AddressMatches[0]
	return &Result{
		Latitude:  match.Coordinates.Y,
		Longitude: match.Coordinates.X,
		Source:    "census",
		Quality:   "rooftop", // Census one-line matches are exact
		Matched:   true,
	}, nil
}

// batchGeocodeCensus geocodes up to 10,000 addresses via the Census batch API.
func (g *geocoder) batchGeocodeCensus(ctx context.Context, addrs []AddressInput) ([]Result, error) {
	if err := g.limiter.Wait(ctx); err != nil {
		return nil, eris.Wrap(err, "geocode: census batch rate limit")
	}

	// Build CSV content: id,street,city,state,zip
	var csv strings.Builder
	idToIdx := make(map[string]int, len(addrs))
	for i, addr := range addrs {
		id := addr.ID
		if id == "" {
			id = fmt.Sprintf("%d", i)
		}
		idToIdx[id] = i
		fmt.Fprintf(&csv, "%s,%s,%s,%s,%s\n", id, addr.Street, addr.City, addr.State, addr.ZipCode)
	}

	// Build multipart form.
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if err := writer.WriteField("benchmark", censusBenchmark); err != nil {
		return nil, eris.Wrap(err, "geocode: census batch write benchmark")
	}

	part, err := writer.CreateFormFile("addressFile", "addresses.csv")
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census batch create form file")
	}
	if _, err := part.Write([]byte(csv.String())); err != nil {
		return nil, eris.Wrap(err, "geocode: census batch write csv")
	}
	if err := writer.Close(); err != nil {
		return nil, eris.Wrap(err, "geocode: census batch close writer")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, censusBatchURL, &buf)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census batch build request")
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census batch request")
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		return nil, eris.Errorf("geocode: census batch returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: census batch read body")
	}

	return parseCensusBatchResponse(string(body), idToIdx, len(addrs))
}

// parseCensusBatchResponse parses the Census batch CSV response.
// Format: "id","input address","match","exact/non_exact","matched address",lon/lat,tigerlineid,side
func parseCensusBatchResponse(body string, idToIdx map[string]int, total int) ([]Result, error) {
	results := make([]Result, total)

	lines := strings.Split(strings.TrimSpace(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := splitCSVLine(line)
		if len(fields) < 6 {
			continue
		}

		id := strings.Trim(fields[0], "\"")
		idx, ok := idToIdx[id]
		if !ok {
			continue
		}

		matchType := strings.Trim(fields[2], "\"")
		if !strings.EqualFold(matchType, "Match") {
			results[idx] = Result{Matched: false, Source: "census"}
			continue
		}

		exactness := strings.Trim(fields[3], "\"")
		quality := censusBatchQuality(exactness)

		// Parse coordinates: "lon,lat" or lon/lat in field 5.
		coords := strings.Trim(fields[5], "\"")
		lon, lat, parseErr := parseCensusCoords(coords)
		if parseErr != nil {
			results[idx] = Result{Matched: false, Source: "census"}
			continue
		}

		results[idx] = Result{
			Latitude:  lat,
			Longitude: lon,
			Source:    "census",
			Quality:   quality,
			Matched:   true,
		}
	}

	return results, nil
}

// censusBatchQuality maps Census batch match exactness to quality.
func censusBatchQuality(exactness string) string {
	switch strings.ToLower(strings.TrimSpace(exactness)) {
	case "exact":
		return "rooftop"
	case "non_exact":
		return "range"
	default:
		return "range"
	}
}

// parseCensusCoords parses "lon,lat" from Census batch response.
func parseCensusCoords(coords string) (lon, lat float64, err error) {
	parts := strings.SplitN(coords, ",", 2)
	if len(parts) != 2 {
		return 0, 0, eris.Errorf("geocode: invalid census coords %q", coords)
	}
	lon, err = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, 0, eris.Wrap(err, "geocode: parse census lon")
	}
	lat, err = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, eris.Wrap(err, "geocode: parse census lat")
	}
	return lon, lat, nil
}

// splitCSVLine splits a CSV line handling quoted fields.
func splitCSVLine(line string) []string {
	var fields []string
	var current strings.Builder
	inQuotes := false

	for _, ch := range line {
		switch {
		case ch == '"':
			inQuotes = !inQuotes
			current.WriteRune(ch)
		case ch == ',' && !inQuotes:
			fields = append(fields, current.String())
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}
	fields = append(fields, current.String())
	return fields
}

// formatOneLine formats an address as a single line for Census API.
func formatOneLine(addr AddressInput) string {
	parts := []string{addr.Street, addr.City, addr.State, addr.ZipCode}
	var nonEmpty []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, ", ")
}
