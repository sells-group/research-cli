package pipeline

import (
	"context"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/geocode"
)

// SetGeocoder sets the geocoding client for Phase 7D.
func (p *Pipeline) SetGeocoder(gc geocode.Client) {
	p.geocoder = gc
}

// SetGeoAssociator sets the MSA associator for Phase 7D.
func (p *Pipeline) SetGeoAssociator(ga *geo.Associator) {
	p.geoAssoc = ga
}

// Phase7DGeocode geocodes company addresses and associates with MSAs.
func (p *Pipeline) Phase7DGeocode(ctx context.Context, companyModel model.Company, _ string) (*model.PhaseResult, error) {
	log := zap.L().With(
		zap.String("company", companyModel.Name),
		zap.String("phase", "7d_geocode"),
	)

	if p.geocoder == nil {
		return &model.PhaseResult{
			Status:   model.PhaseStatusSkipped,
			Metadata: map[string]any{"reason": "geocoder_not_configured"},
		}, nil
	}

	// Find the company record by domain.
	companyStore, ok := p.companyStore()
	if !ok {
		return &model.PhaseResult{
			Status:   model.PhaseStatusSkipped,
			Metadata: map[string]any{"reason": "company_store_not_available"},
		}, nil
	}

	// Get company addresses.
	domain := extractDomain(companyModel.URL)
	cr, lookupErr := companyStore.GetCompanyByDomain(ctx, domain)
	if lookupErr != nil {
		log.Debug("geocode: company lookup failed, skipping", zap.String("domain", domain), zap.Error(lookupErr))
		return &model.PhaseResult{
			Status:   model.PhaseStatusSkipped,
			Metadata: map[string]any{"reason": "company_not_found"},
		}, nil //nolint:nilerr // lookup failure is a skip, not a pipeline error
	}
	if cr == nil {
		log.Debug("geocode: company not found by domain, skipping", zap.String("domain", domain))
		return &model.PhaseResult{
			Status:   model.PhaseStatusSkipped,
			Metadata: map[string]any{"reason": "company_not_found"},
		}, nil
	}

	addrs, err := companyStore.GetAddresses(ctx, cr.ID)
	if err != nil {
		return nil, eris.Wrap(err, "geocode: get addresses")
	}

	// If no addresses exist, create one from the company model's denormalized fields.
	if len(addrs) == 0 {
		if companyModel.Street != "" && companyModel.City != "" && companyModel.State != "" {
			newAddr := &company.Address{
				CompanyID:   cr.ID,
				AddressType: company.AddressPrincipal,
				Street:      companyModel.Street,
				City:        companyModel.City,
				State:       companyModel.State,
				ZipCode:     companyModel.ZipCode,
				Country:     "US",
				IsPrimary:   true,
			}
			if upsertErr := companyStore.UpsertAddress(ctx, newAddr); upsertErr != nil {
				return nil, eris.Wrap(upsertErr, "geocode: create address from model")
			}
			addrs = []company.Address{*newAddr}
		} else if cr.Street != "" && cr.City != "" && cr.State != "" {
			newAddr := &company.Address{
				CompanyID:   cr.ID,
				AddressType: company.AddressPrincipal,
				Street:      cr.Street,
				City:        cr.City,
				State:       cr.State,
				ZipCode:     cr.ZipCode,
				Country:     "US",
				IsPrimary:   true,
			}
			if upsertErr := companyStore.UpsertAddress(ctx, newAddr); upsertErr != nil {
				return nil, eris.Wrap(upsertErr, "geocode: create address from record")
			}
			addrs = []company.Address{*newAddr}
		}
	}

	if len(addrs) == 0 {
		return &model.PhaseResult{
			Status:   model.PhaseStatusSkipped,
			Metadata: map[string]any{"reason": "no_addresses"},
		}, nil
	}

	// Geocode addresses that don't already have coordinates.
	var geocodedCount int
	var msaAssociations int
	classifications := make(map[string]int)

	for i := range addrs {
		addr := &addrs[i]

		// Skip already geocoded addresses.
		if addr.Latitude != nil && addr.Longitude != nil {
			continue
		}

		// Skip addresses without enough data.
		if addr.Street == "" || addr.City == "" || addr.State == "" {
			continue
		}

		result, geocodeErr := p.geocoder.Geocode(ctx, geocode.AddressInput{
			Street:  addr.Street,
			City:    addr.City,
			State:   addr.State,
			ZipCode: addr.ZipCode,
		})
		if geocodeErr != nil {
			log.Warn("geocode: failed to geocode address",
				zap.Int64("address_id", addr.ID),
				zap.Error(geocodeErr),
			)
			continue
		}
		if !result.Matched {
			continue
		}

		// Update address with geocode results.
		if updateErr := companyStore.UpdateAddressGeocode(ctx, addr.ID, result.Latitude, result.Longitude, result.Source, result.Quality, result.CountyFIPS); updateErr != nil {
			log.Warn("geocode: failed to update address",
				zap.Int64("address_id", addr.ID),
				zap.Error(updateErr),
			)
			continue
		}
		geocodedCount++

		// Store for MSA association below.
		lat := result.Latitude
		lon := result.Longitude
		addr.Latitude = &lat
		addr.Longitude = &lon
	}

	// Associate geocoded addresses with MSAs.
	if p.geoAssoc != nil {
		topN := p.cfg.Geo.TopMSAs
		if topN <= 0 {
			topN = 3
		}

		for _, addr := range addrs {
			if addr.Latitude == nil || addr.Longitude == nil {
				continue
			}

			relations, assocErr := p.geoAssoc.AssociateAddress(ctx, addr.ID, *addr.Latitude, *addr.Longitude, topN)
			if assocErr != nil {
				log.Warn("geocode: MSA association failed",
					zap.Int64("address_id", addr.ID),
					zap.Error(assocErr),
				)
				continue
			}

			msaAssociations += len(relations)
			for _, r := range relations {
				classifications[r.Classification]++
			}
		}
	}

	log.Info("geocode phase complete",
		zap.Int("geocoded", geocodedCount),
		zap.Int("msa_associations", msaAssociations),
	)

	return &model.PhaseResult{
		Metadata: map[string]any{
			"geocoded_count":    geocodedCount,
			"msa_associations":  msaAssociations,
			"classifications":   classifications,
			"addresses_checked": len(addrs),
		},
	}, nil
}

// collectGeoData reads the primary address and MSA data for a company,
// returning a GeoData struct for Salesforce field injection in Phase 9.
func (p *Pipeline) collectGeoData(ctx context.Context, companyModel model.Company) *model.GeoData {
	cs, ok := p.companyStore()
	if !ok {
		return nil
	}

	domain := extractDomain(companyModel.URL)
	cr, err := cs.GetCompanyByDomain(ctx, domain)
	if err != nil || cr == nil {
		return nil
	}

	addrs, err := cs.GetAddresses(ctx, cr.ID)
	if err != nil || len(addrs) == 0 {
		return nil
	}

	// Find the primary geocoded address.
	var primary *company.Address
	for i := range addrs {
		if addrs[i].Latitude != nil && addrs[i].Longitude != nil {
			if addrs[i].IsPrimary || primary == nil {
				primary = &addrs[i]
			}
		}
	}
	if primary == nil {
		return nil
	}

	gd := &model.GeoData{
		Latitude:   *primary.Latitude,
		Longitude:  *primary.Longitude,
		CountyFIPS: primary.CountyFIPS,
	}

	// Get MSA associations (ordered by centroid distance).
	msas, err := cs.GetCompanyMSAs(ctx, cr.ID)
	if err == nil && len(msas) > 0 {
		best := msas[0]
		gd.MSAName = best.MSAName
		gd.CBSACode = best.CBSACode
		gd.Classification = best.Classification
		gd.CentroidKM = best.CentroidKM
		gd.EdgeKM = best.EdgeKM
	}

	return gd
}

// companyStore returns the company.CompanyStore if the store supports it.
func (p *Pipeline) companyStore() (company.CompanyStore, bool) {
	type companyStoreProvider interface {
		CompanyStore() company.CompanyStore
	}
	if csp, ok := p.store.(companyStoreProvider); ok {
		return csp.CompanyStore(), true
	}
	return nil, false
}
