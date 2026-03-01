package geospatial

import (
	"encoding/json"
	"time"
)

// County represents a county boundary from TIGER/Line data.
type County struct {
	ID         int             `json:"id"`
	GEOID      string          `json:"geoid"`
	StateFIPS  string          `json:"state_fips"`
	CountyFIPS string          `json:"county_fips"`
	Name       string          `json:"name"`
	LSAD       string          `json:"lsad,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// Place represents a city, town, or CDP boundary from TIGER/Line data.
type Place struct {
	ID         int             `json:"id"`
	GEOID      string          `json:"geoid"`
	StateFIPS  string          `json:"state_fips"`
	PlaceFIPS  string          `json:"place_fips"`
	Name       string          `json:"name"`
	LSAD       string          `json:"lsad,omitempty"`
	ClassFIPS  string          `json:"class_fips,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// ZCTA represents a ZIP Code Tabulation Area.
type ZCTA struct {
	ID         int             `json:"id"`
	ZCTA5      string          `json:"zcta5"`
	StateFIPS  string          `json:"state_fips,omitempty"`
	ALand      int64           `json:"aland"`
	AWater     int64           `json:"awater"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// CBSA represents a Core-Based Statistical Area (MSA/micropolitan).
type CBSA struct {
	ID         int             `json:"id"`
	CBSACode   string          `json:"cbsa_code"`
	Name       string          `json:"name"`
	LSAD       string          `json:"lsad,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// CensusTract represents a census tract boundary.
type CensusTract struct {
	ID         int             `json:"id"`
	GEOID      string          `json:"geoid"`
	StateFIPS  string          `json:"state_fips"`
	CountyFIPS string          `json:"county_fips"`
	TractCE    string          `json:"tract_ce"`
	Name       string          `json:"name,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// CongressionalDistrict represents a congressional district boundary.
type CongressionalDistrict struct {
	ID         int             `json:"id"`
	GEOID      string          `json:"geoid"`
	StateFIPS  string          `json:"state_fips"`
	District   string          `json:"district"`
	Congress   string          `json:"congress,omitempty"`
	Name       string          `json:"name,omitempty"`
	LSAD       string          `json:"lsad,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// POI represents a point of interest.
type POI struct {
	ID          int             `json:"id"`
	Name        string          `json:"name"`
	Category    string          `json:"category"`
	Subcategory string          `json:"subcategory,omitempty"`
	Address     string          `json:"address,omitempty"`
	Latitude    float64         `json:"latitude"`
	Longitude   float64         `json:"longitude"`
	Source      string          `json:"source"`
	SourceID    string          `json:"source_id,omitempty"`
	Properties  json.RawMessage `json:"properties,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// Infrastructure represents an infrastructure asset (power plant, substation, etc.).
type Infrastructure struct {
	ID         int             `json:"id"`
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	FuelType   string          `json:"fuel_type,omitempty"`
	Capacity   float64         `json:"capacity,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// EPASite represents an EPA-monitored environmental site.
type EPASite struct {
	ID         int             `json:"id"`
	Name       string          `json:"name"`
	Program    string          `json:"program"`
	RegistryID string          `json:"registry_id,omitempty"`
	Status     string          `json:"status,omitempty"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// FloodZone represents a FEMA flood zone area.
type FloodZone struct {
	ID         int             `json:"id"`
	ZoneCode   string          `json:"zone_code"`
	FloodType  string          `json:"flood_type"`
	Source     string          `json:"source"`
	SourceID   string          `json:"source_id,omitempty"`
	Properties json.RawMessage `json:"properties,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// Demographic represents Census/ACS demographic data for a geographic area.
type Demographic struct {
	ID              int             `json:"id"`
	GEOID           string          `json:"geoid"`
	GeoLevel        string          `json:"geo_level"`
	Year            int             `json:"year"`
	TotalPopulation int             `json:"total_population,omitempty"`
	MedianIncome    float64         `json:"median_income,omitempty"`
	MedianAge       float64         `json:"median_age,omitempty"`
	HousingUnits    int             `json:"housing_units,omitempty"`
	Source          string          `json:"source"`
	SourceID        string          `json:"source_id,omitempty"`
	Properties      json.RawMessage `json:"properties,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}
