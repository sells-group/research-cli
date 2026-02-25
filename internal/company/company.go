// Package company defines the golden record type for enriched company data.
package company

import (
	"time"
)

// CompanyRecord is the golden record for a company.
type CompanyRecord struct { //nolint:revive // stutters but widely used across codebase
	ID          int64  `json:"id" db:"id"`
	Name        string `json:"name" db:"name"`
	LegalName   string `json:"legal_name,omitempty" db:"legal_name"`
	Domain      string `json:"domain" db:"domain"`
	Website     string `json:"website,omitempty" db:"website"`
	Description string `json:"description,omitempty" db:"description"`

	// Classification
	NAICSCode     string `json:"naics_code,omitempty" db:"naics_code"`
	SICCode       string `json:"sic_code,omitempty" db:"sic_code"`
	BusinessModel string `json:"business_model,omitempty" db:"business_model"`
	YearFounded   int    `json:"year_founded,omitempty" db:"year_founded"`
	OwnershipType string `json:"ownership_type,omitempty" db:"ownership_type"`

	// Contact
	Phone string `json:"phone,omitempty" db:"phone"`
	Email string `json:"email,omitempty" db:"email"`

	// Size
	EmployeeCount     *int     `json:"employee_count,omitempty" db:"employee_count"`
	EmployeeEstimate  *int     `json:"employee_estimate,omitempty" db:"employee_estimate"`
	RevenueEstimate   *int64   `json:"revenue_estimate,omitempty" db:"revenue_estimate"`
	RevenueRange      string   `json:"revenue_range,omitempty" db:"revenue_range"`
	RevenueConfidence *float64 `json:"revenue_confidence,omitempty" db:"revenue_confidence"`

	// Primary address (denormalized)
	Street  string `json:"street,omitempty" db:"street"`
	City    string `json:"city,omitempty" db:"city"`
	State   string `json:"state,omitempty" db:"state"`
	ZipCode string `json:"zip_code,omitempty" db:"zip_code"`
	Country string `json:"country,omitempty" db:"country"`

	// Enrichment metadata
	EnrichmentScore *float64   `json:"enrichment_score,omitempty" db:"enrichment_score"`
	LastEnrichedAt  *time.Time `json:"last_enriched_at,omitempty" db:"last_enriched_at"`
	LastRunID       *int64     `json:"last_run_id,omitempty" db:"last_run_id"`

	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// Identifier links a company to an external system.
type Identifier struct {
	ID         int64     `json:"id" db:"id"`
	CompanyID  int64     `json:"company_id" db:"company_id"`
	System     string    `json:"system" db:"system"`
	Identifier string    `json:"identifier" db:"identifier"`
	Metadata   []byte    `json:"metadata,omitempty" db:"metadata"` // JSONB
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}

// Known identifier systems.
const (
	SystemSalesforce  = "salesforce"
	SystemCRD         = "crd"
	SystemCIK         = "cik"
	SystemEIN         = "ein"
	SystemDUNS        = "duns"
	SystemUEI         = "uei"
	SystemStateEntity = "state_entity"
	SystemNotion      = "notion"
	SystemPPPLoan     = "ppp_loan"
	SystemEPARegistry = "epa_registry"
	SystemLinkedIn    = "linkedin"
	SystemGrata       = "grata"
	SystemGooglePlace = "google_place"
)

// Address is a physical address for a company.
type Address struct {
	ID             int64      `json:"id" db:"id"`
	CompanyID      int64      `json:"company_id" db:"company_id"`
	AddressType    string     `json:"address_type" db:"address_type"`
	Street         string     `json:"street,omitempty" db:"street"`
	City           string     `json:"city,omitempty" db:"city"`
	State          string     `json:"state,omitempty" db:"state"`
	ZipCode        string     `json:"zip_code,omitempty" db:"zip_code"`
	Country        string     `json:"country,omitempty" db:"country"`
	Latitude       *float64   `json:"latitude,omitempty" db:"latitude"`
	Longitude      *float64   `json:"longitude,omitempty" db:"longitude"`
	Source         string     `json:"source,omitempty" db:"source"`
	Confidence     *float64   `json:"confidence,omitempty" db:"confidence"`
	IsPrimary      bool       `json:"is_primary" db:"is_primary"`
	GeocodeSource  string     `json:"geocode_source,omitempty" db:"geocode_source"`
	GeocodeQuality string     `json:"geocode_quality,omitempty" db:"geocode_quality"`
	GeocodedAt     *time.Time `json:"geocoded_at,omitempty" db:"geocoded_at"`
	CreatedAt      time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at" db:"updated_at"`
}

// Address types.
const (
	AddressMailing         = "mailing"
	AddressPrincipal       = "principal"
	AddressRegisteredAgent = "registered_agent"
	AddressBranch          = "branch"
)

// Contact is a person associated with a company.
type Contact struct {
	ID              int64     `json:"id" db:"id"`
	CompanyID       int64     `json:"company_id" db:"company_id"`
	FirstName       string    `json:"first_name,omitempty" db:"first_name"`
	LastName        string    `json:"last_name,omitempty" db:"last_name"`
	FullName        string    `json:"full_name,omitempty" db:"full_name"`
	Title           string    `json:"title,omitempty" db:"title"`
	RoleType        string    `json:"role_type,omitempty" db:"role_type"`
	Email           string    `json:"email,omitempty" db:"email"`
	Phone           string    `json:"phone,omitempty" db:"phone"`
	LinkedInURL     string    `json:"linkedin_url,omitempty" db:"linkedin_url"`
	OwnershipPct    *float64  `json:"ownership_pct,omitempty" db:"ownership_pct"`
	IsControlPerson bool      `json:"is_control_person" db:"is_control_person"`
	IsPrimary       bool      `json:"is_primary" db:"is_primary"`
	Source          string    `json:"source,omitempty" db:"source"`
	Confidence      *float64  `json:"confidence,omitempty" db:"confidence"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" db:"updated_at"`
}

// Role types.
const (
	RoleExecutive       = "executive"
	RoleOwner           = "owner"
	RoleOfficer         = "officer"
	RoleRegisteredAgent = "registered_agent"
	RoleAdviserRep      = "adviser_rep"
	RoleKeyPerson       = "key_person"
)

// License is a professional license or registration.
type License struct {
	ID            int64      `json:"id" db:"id"`
	CompanyID     int64      `json:"company_id" db:"company_id"`
	LicenseType   string     `json:"license_type" db:"license_type"`
	LicenseNumber string     `json:"license_number,omitempty" db:"license_number"`
	Authority     string     `json:"authority,omitempty" db:"authority"`
	State         string     `json:"state,omitempty" db:"state"`
	Status        string     `json:"status,omitempty" db:"status"`
	IssuedDate    *time.Time `json:"issued_date,omitempty" db:"issued_date"`
	ExpiryDate    *time.Time `json:"expiry_date,omitempty" db:"expiry_date"`
	Source        string     `json:"source,omitempty" db:"source"`
	RawText       string     `json:"raw_text,omitempty" db:"raw_text"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
}

// Source holds raw data from a specific provider for a company.
type Source struct {
	ID              int64      `json:"id" db:"id"`
	CompanyID       int64      `json:"company_id" db:"company_id"`
	SourceName      string     `json:"source" db:"source"`
	SourceID        string     `json:"source_id,omitempty" db:"source_id"`
	RawData         []byte     `json:"raw_data,omitempty" db:"raw_data"`
	ExtractedFields []byte     `json:"extracted_fields,omitempty" db:"extracted_fields"`
	DataAsOf        *time.Time `json:"data_as_of,omitempty" db:"data_as_of"`
	FetchedAt       time.Time  `json:"fetched_at" db:"fetched_at"`
	RunID           *int64     `json:"run_id,omitempty" db:"run_id"`
	CreatedAt       time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at" db:"updated_at"`
}

// Financial is a time-series metric for a company.
type Financial struct {
	ID         int64     `json:"id" db:"id"`
	CompanyID  int64     `json:"company_id" db:"company_id"`
	PeriodType string    `json:"period_type" db:"period_type"`
	PeriodDate time.Time `json:"period_date" db:"period_date"`
	Metric     string    `json:"metric" db:"metric"`
	Value      float64   `json:"value" db:"value"`
	SourceName string    `json:"source,omitempty" db:"source"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// Tag is a categorical label for a company.
type Tag struct {
	CompanyID int64     `json:"company_id" db:"company_id"`
	TagType   string    `json:"tag_type" db:"tag_type"`
	TagValue  string    `json:"tag_value" db:"tag_value"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// Tag types.
const (
	TagService            = "service"
	TagIndustry           = "industry"
	TagCustomerType       = "customer_type"
	TagDifferentiator     = "differentiator"
	TagInvestmentStrategy = "investment_strategy"
)

// AddressMSA links an address to a CBSA metropolitan area with precomputed distances.
type AddressMSA struct {
	ID             int64     `json:"id" db:"id"`
	AddressID      int64     `json:"address_id" db:"address_id"`
	CBSACode       string    `json:"cbsa_code" db:"cbsa_code"`
	MSAName        string    `json:"msa_name,omitempty"`
	IsWithin       bool      `json:"is_within" db:"is_within"`
	DistanceKM     float64   `json:"distance_km" db:"distance_km"`
	CentroidKM     float64   `json:"centroid_km" db:"centroid_km"`
	EdgeKM         float64   `json:"edge_km" db:"edge_km"`
	Classification string    `json:"classification" db:"classification"`
	ComputedAt     time.Time `json:"computed_at" db:"computed_at"`
}

// Match links a company to a fed_data entity.
type Match struct {
	ID            int64     `json:"id" db:"id"`
	CompanyID     int64     `json:"company_id" db:"company_id"`
	MatchedSource string    `json:"matched_source" db:"matched_source"`
	MatchedKey    string    `json:"matched_key" db:"matched_key"`
	MatchType     string    `json:"match_type" db:"match_type"`
	Confidence    *float64  `json:"confidence,omitempty" db:"confidence"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}
