package geospatial

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounty_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	c := County{
		ID:         1,
		GEOID:      "48453",
		StateFIPS:  "48",
		CountyFIPS: "453",
		Name:       "Travis County",
		LSAD:       "06",
		Latitude:   30.3340,
		Longitude:  -97.7544,
		Source:     "tiger",
		SourceID:   "tl_2024_us_county",
		Properties: json.RawMessage(`{"population":1290188}`),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	data, err := json.Marshal(c)
	require.NoError(t, err)

	var got County
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, c.GEOID, got.GEOID)
	assert.Equal(t, c.Name, got.Name)
	assert.Equal(t, c.Latitude, got.Latitude)
	assert.JSONEq(t, `{"population":1290188}`, string(got.Properties))
}

func TestCounty_NilProperties(t *testing.T) {
	c := County{Name: "Test", Source: "tiger"}
	data, err := json.Marshal(c)
	require.NoError(t, err)

	var got County
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Nil(t, got.Properties)
}

func TestPlace_JSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	p := Place{
		ID:         1,
		GEOID:      "4805000",
		StateFIPS:  "48",
		PlaceFIPS:  "05000",
		Name:       "Austin",
		LSAD:       "25",
		ClassFIPS:  "C1",
		Latitude:   30.2672,
		Longitude:  -97.7431,
		Source:     "tiger",
		Properties: json.RawMessage(`{"type":"city"}`),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	data, err := json.Marshal(p)
	require.NoError(t, err)

	var got Place
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, p.GEOID, got.GEOID)
	assert.Equal(t, p.ClassFIPS, got.ClassFIPS)
}

func TestZCTA_JSONRoundTrip(t *testing.T) {
	z := ZCTA{
		ID:        1,
		ZCTA5:     "78701",
		StateFIPS: "48",
		ALand:     5000000,
		AWater:    100000,
		Latitude:  30.27,
		Longitude: -97.74,
		Source:    "tiger",
	}

	data, err := json.Marshal(z)
	require.NoError(t, err)

	var got ZCTA
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, z.ZCTA5, got.ZCTA5)
	assert.Equal(t, z.ALand, got.ALand)
	assert.Equal(t, z.AWater, got.AWater)
}

func TestCBSA_JSONRoundTrip(t *testing.T) {
	c := CBSA{
		ID:         1,
		CBSACode:   "12420",
		Name:       "Austin-Round Rock-Georgetown, TX",
		LSAD:       "M1",
		Latitude:   30.3,
		Longitude:  -97.7,
		Source:     "tiger",
		Properties: json.RawMessage(`{"metro_division":true}`),
	}

	data, err := json.Marshal(c)
	require.NoError(t, err)

	var got CBSA
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, c.CBSACode, got.CBSACode)
	assert.Equal(t, c.Name, got.Name)
}

func TestCensusTract_JSONRoundTrip(t *testing.T) {
	ct := CensusTract{
		ID:         1,
		GEOID:      "48453001700",
		StateFIPS:  "48",
		CountyFIPS: "453",
		TractCE:    "001700",
		Name:       "Census Tract 17",
		Latitude:   30.27,
		Longitude:  -97.74,
		Source:     "tiger",
	}

	data, err := json.Marshal(ct)
	require.NoError(t, err)

	var got CensusTract
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, ct.GEOID, got.GEOID)
	assert.Equal(t, ct.TractCE, got.TractCE)
}

func TestCongressionalDistrict_JSONRoundTrip(t *testing.T) {
	cd := CongressionalDistrict{
		ID:        1,
		GEOID:     "4825",
		StateFIPS: "48",
		District:  "25",
		Congress:  "118",
		Name:      "Congressional District 25",
		LSAD:      "C2",
		Latitude:  30.3,
		Longitude: -97.7,
		Source:    "tiger",
	}

	data, err := json.Marshal(cd)
	require.NoError(t, err)

	var got CongressionalDistrict
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, cd.District, got.District)
	assert.Equal(t, cd.Congress, got.Congress)
}

func TestPOI_JSONRoundTrip(t *testing.T) {
	p := POI{
		ID:          1,
		Name:        "Capitol Building",
		Category:    "government",
		Subcategory: "state_capitol",
		Address:     "1100 Congress Ave, Austin, TX",
		Latitude:    30.2747,
		Longitude:   -97.7404,
		Source:      "osm",
		Properties:  json.RawMessage(`{"amenity":"government"}`),
	}

	data, err := json.Marshal(p)
	require.NoError(t, err)

	var got POI
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, p.Name, got.Name)
	assert.Equal(t, p.Category, got.Category)
	assert.Equal(t, p.Subcategory, got.Subcategory)
}

func TestInfrastructure_JSONRoundTrip(t *testing.T) {
	infra := Infrastructure{
		ID:         1,
		Name:       "Decker Creek Power Station",
		Type:       "power_plant",
		FuelType:   "natural_gas",
		Capacity:   926.0,
		Latitude:   30.32,
		Longitude:  -97.62,
		Source:     "eia",
		Properties: json.RawMessage(`{"plant_code":"3497"}`),
	}

	data, err := json.Marshal(infra)
	require.NoError(t, err)

	var got Infrastructure
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, infra.Type, got.Type)
	assert.Equal(t, infra.FuelType, got.FuelType)
	assert.Equal(t, infra.Capacity, got.Capacity)
}

func TestEPASite_JSONRoundTrip(t *testing.T) {
	site := EPASite{
		ID:         1,
		Name:       "Test Facility",
		Program:    "RCRA",
		RegistryID: "TXD000001234",
		Status:     "active",
		Latitude:   30.27,
		Longitude:  -97.74,
		Source:     "epa",
		Properties: json.RawMessage(`{"handler_type":"LQG"}`),
	}

	data, err := json.Marshal(site)
	require.NoError(t, err)

	var got EPASite
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, site.RegistryID, got.RegistryID)
	assert.Equal(t, site.Program, got.Program)
}

func TestFloodZone_JSONRoundTrip(t *testing.T) {
	fz := FloodZone{
		ID:         1,
		ZoneCode:   "AE",
		FloodType:  "100-year",
		Source:     "fema",
		Properties: json.RawMessage(`{"panel":"48453C0235G"}`),
	}

	data, err := json.Marshal(fz)
	require.NoError(t, err)

	var got FloodZone
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, fz.ZoneCode, got.ZoneCode)
	assert.Equal(t, fz.FloodType, got.FloodType)
}

func TestFloodZone_NilProperties(t *testing.T) {
	fz := FloodZone{ZoneCode: "X", FloodType: "minimal", Source: "fema"}
	data, err := json.Marshal(fz)
	require.NoError(t, err)

	var got FloodZone
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Nil(t, got.Properties)
}

func TestDemographic_JSONRoundTrip(t *testing.T) {
	d := Demographic{
		ID:              1,
		GEOID:           "48453",
		GeoLevel:        "county",
		Year:            2022,
		TotalPopulation: 1290188,
		MedianIncome:    78947.0,
		MedianAge:       34.5,
		HousingUnits:    548000,
		Source:          "census",
		Properties:      json.RawMessage(`{"vintage":"2022"}`),
	}

	data, err := json.Marshal(d)
	require.NoError(t, err)

	var got Demographic
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, d.GEOID, got.GEOID)
	assert.Equal(t, d.GeoLevel, got.GeoLevel)
	assert.Equal(t, d.Year, got.Year)
	assert.Equal(t, d.TotalPopulation, got.TotalPopulation)
	assert.Equal(t, d.MedianIncome, got.MedianIncome)
}

func TestDemographic_ZeroValues(t *testing.T) {
	d := Demographic{
		GEOID:    "00000",
		GeoLevel: "county",
		Year:     2022,
		Source:   "census",
	}

	data, err := json.Marshal(d)
	require.NoError(t, err)

	var got Demographic
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, 0, got.TotalPopulation)
	assert.Equal(t, 0.0, got.MedianIncome)
	assert.Equal(t, 0.0, got.MedianAge)
	assert.Equal(t, 0, got.HousingUnits)
}
