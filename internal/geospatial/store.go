package geospatial

import "context"

// Store defines CRUD operations for geospatial entities in the geo.* schema.
type Store interface {
	// UpsertCounty inserts or updates a county by GEOID.
	UpsertCounty(ctx context.Context, c *County) error

	// GetCounty retrieves a county by GEOID.
	GetCounty(ctx context.Context, geoid string) (*County, error)

	// ListCountiesByState returns all counties for a given state FIPS code.
	ListCountiesByState(ctx context.Context, stateFIPS string) ([]County, error)

	// BulkUpsertCounties upserts multiple counties in a single transaction.
	BulkUpsertCounties(ctx context.Context, counties []County) (int64, error)

	// UpsertPlace inserts or updates a place by GEOID.
	UpsertPlace(ctx context.Context, p *Place) error

	// UpsertCBSA inserts or updates a CBSA by code.
	UpsertCBSA(ctx context.Context, c *CBSA) error

	// GetCBSA retrieves a CBSA by code.
	GetCBSA(ctx context.Context, cbsaCode string) (*CBSA, error)

	// UpsertPOI inserts or updates a point of interest.
	UpsertPOI(ctx context.Context, p *POI) error

	// GetPOI retrieves a POI by ID.
	GetPOI(ctx context.Context, id int) (*POI, error)

	// ListPOIByCategory returns POIs matching a category with pagination.
	// Returns items, total count, and error.
	ListPOIByCategory(ctx context.Context, category string, limit, offset int) ([]POI, int, error)

	// BulkUpsertPOI upserts multiple POIs in a single transaction.
	BulkUpsertPOI(ctx context.Context, pois []POI) (int64, error)

	// UpsertInfrastructure inserts or updates an infrastructure asset.
	UpsertInfrastructure(ctx context.Context, infra *Infrastructure) error

	// BulkUpsertInfrastructure upserts multiple infrastructure assets.
	BulkUpsertInfrastructure(ctx context.Context, infras []Infrastructure) (int64, error)

	// UpsertEPASite inserts or updates an EPA site by registry ID.
	UpsertEPASite(ctx context.Context, site *EPASite) error

	// UpsertFloodZone inserts or updates a flood zone.
	UpsertFloodZone(ctx context.Context, fz *FloodZone) error

	// UpsertDemographic inserts or updates a demographic record.
	UpsertDemographic(ctx context.Context, d *Demographic) error

	// GetDemographic retrieves a demographic record by GEOID, geo level, and year.
	GetDemographic(ctx context.Context, geoid, geoLevel string, year int) (*Demographic, error)
}
