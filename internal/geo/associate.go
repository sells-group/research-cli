package geo

import (
	"context"
	"fmt"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/db"
)

// MSARelation describes the spatial relationship between a point and an MSA.
type MSARelation struct {
	CBSACode       string
	MSAName        string
	IsWithin       bool
	DistanceKM     float64
	CentroidKM     float64
	EdgeKM         float64
	Classification string
}

// AssociatorOption configures an Associator.
type AssociatorOption func(*Associator)

// WithGeoSchema configures the Associator to query geo.cbsa instead of public.cbsa_areas.
func WithGeoSchema() AssociatorOption {
	return func(a *Associator) {
		a.useGeoSchema = true
	}
}

// Associator finds and persists MSA associations for geocoded addresses.
type Associator struct {
	pool         db.Pool
	companyStore company.CompanyStore
	useGeoSchema bool
}

// NewAssociator creates a new Associator.
func NewAssociator(pool db.Pool, cs company.CompanyStore, opts ...AssociatorOption) *Associator {
	a := &Associator{pool: pool, companyStore: cs}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// AssociateAddress finds the top N nearest MSAs for a geocoded address and
// persists the associations via the company store.
func (a *Associator) AssociateAddress(ctx context.Context, addressID int64, lat, lon float64, topN int) ([]MSARelation, error) {
	if topN <= 0 {
		topN = 3
	}

	// PostGIS query: find top N nearest CBSAs with distances.
	cbsaTable := "public.cbsa_areas"
	if a.useGeoSchema {
		cbsaTable = "geo.cbsa"
	}

	query := fmt.Sprintf(`
		SELECT
			cb.cbsa_code,
			cb.name,
			ST_Contains(cb.geom, pt) AS is_within,
			CASE WHEN ST_Contains(cb.geom, pt) THEN 0
				 ELSE ST_Distance(cb.geom::geography, pt::geography) / 1000
			END AS distance_km,
			ST_Distance(ST_Centroid(cb.geom)::geography, pt::geography) / 1000 AS centroid_km,
			ST_Distance(ST_Boundary(cb.geom)::geography, pt::geography) / 1000 AS edge_km
		FROM %s cb,
			 ST_SetSRID(ST_MakePoint($1, $2), 4326) AS pt
		WHERE cb.lsad IN ('M1', 'M2')
		ORDER BY cb.geom <-> pt
		LIMIT $3`, cbsaTable)

	rows, err := a.pool.Query(ctx, query, lon, lat, topN)
	if err != nil {
		return nil, eris.Wrap(err, "geo: associate address query")
	}
	defer rows.Close()

	var relations []MSARelation
	for rows.Next() {
		var r MSARelation
		if err := rows.Scan(&r.CBSACode, &r.MSAName, &r.IsWithin,
			&r.DistanceKM, &r.CentroidKM, &r.EdgeKM); err != nil {
			return nil, eris.Wrap(err, "geo: scan MSA relation")
		}
		r.Classification = Classify(r.IsWithin, r.CentroidKM, r.EdgeKM)
		relations = append(relations, r)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "geo: iterate MSA rows")
	}

	// Persist associations via the company store.
	for _, r := range relations {
		am := &company.AddressMSA{
			AddressID:      addressID,
			CBSACode:       r.CBSACode,
			MSAName:        r.MSAName,
			IsWithin:       r.IsWithin,
			DistanceKM:     r.DistanceKM,
			CentroidKM:     r.CentroidKM,
			EdgeKM:         r.EdgeKM,
			Classification: r.Classification,
			ComputedAt:     time.Now(),
		}
		if err := a.companyStore.UpsertAddressMSA(ctx, am); err != nil {
			zap.L().Warn("geo: failed to upsert address MSA",
				zap.Int64("address_id", addressID),
				zap.String("cbsa_code", r.CBSACode),
				zap.Error(err),
			)
		}
	}

	return relations, nil
}
