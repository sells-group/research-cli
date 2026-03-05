// Package geo provides Temporal workflows and activities for geo backfill operations.
package geo

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rotisserie/eris"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/config"
	igeo "github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/pkg/geocode"
)

// Activities holds dependencies for geo backfill Temporal activities.
type Activities struct {
	pool    *pgxpool.Pool
	cs      *company.PostgresStore
	gc      geocode.Client
	assoc   *igeo.Associator
	cfg     *config.Config
	topMSAs int
}

// NewActivities creates geo backfill Activities.
func NewActivities(pool *pgxpool.Pool, cs *company.PostgresStore, gc geocode.Client, assoc *igeo.Associator, cfg *config.Config) *Activities {
	topMSAs := cfg.Geo.TopMSAs
	if topMSAs <= 0 {
		topMSAs = 3
	}
	return &Activities{
		pool:    pool,
		cs:      cs,
		gc:      gc,
		assoc:   assoc,
		cfg:     cfg,
		topMSAs: topMSAs,
	}
}

// UnlinkedRecord represents a single record that needs geo backfill processing.
type UnlinkedRecord struct {
	// Common fields
	Key  string `json:"key"`  // identifier value (CRD, EIN, cert)
	Name string `json:"name"` // entity name

	// Address fields
	Street1 string `json:"street1,omitempty"`
	Street2 string `json:"street2,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Zip     string `json:"zip,omitempty"`
	Website string `json:"website,omitempty"`

	// FDIC-specific fields
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
}

// QueryUnlinkedParams is the input for QueryUnlinkedRecords.
type QueryUnlinkedParams struct {
	Source string `json:"source"` // "adv", "5500", "990", "fdic", "sba", "address"
	Limit  int    `json:"limit"`
}

// QueryUnlinkedResult is the output of QueryUnlinkedRecords.
type QueryUnlinkedResult struct {
	Records []UnlinkedRecord `json:"records"`
}

// QueryUnlinkedRecords finds records that haven't been linked to companies yet.
func (a *Activities) QueryUnlinkedRecords(ctx context.Context, params QueryUnlinkedParams) (*QueryUnlinkedResult, error) {
	var records []UnlinkedRecord
	var err error

	switch params.Source {
	case "adv":
		records, err = a.queryUnlinkedADV(ctx, params.Limit)
	case "5500":
		records, err = a.queryUnlinked5500(ctx, params.Limit)
	case "990":
		records, err = a.queryUnlinked990(ctx, params.Limit)
	case "fdic":
		records, err = a.queryUnlinkedFDIC(ctx, params.Limit)
	case "sba":
		records, err = a.queryUnlinkedSBA(ctx, params.Limit)
	case "address":
		records, err = a.queryUngeocodedAddresses(ctx, params.Limit)
	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unknown source: %s", params.Source),
			"UnknownSource", nil)
	}
	if err != nil {
		return nil, eris.Wrapf(err, "query unlinked %s records", params.Source)
	}
	return &QueryUnlinkedResult{Records: records}, nil
}

func (a *Activities) queryUnlinkedADV(ctx context.Context, limit int) ([]UnlinkedRecord, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT af.crd_number::text, af.firm_name,
		       af.street1, af.street2, af.city, af.state, af.zip, af.website
		FROM fed_data.adv_firms af
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = af.crd_number::text
			AND cm.matched_source = 'adv_firms'
		WHERE cm.id IS NULL
		ORDER BY af.aum DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked ADV firms")
	}
	defer rows.Close()

	var records []UnlinkedRecord
	for rows.Next() {
		var r UnlinkedRecord
		var s1, s2, city, state, zip, website *string
		if err := rows.Scan(&r.Key, &r.Name, &s1, &s2, &city, &state, &zip, &website); err != nil {
			return nil, eris.Wrap(err, "scan ADV firm")
		}
		if s1 != nil {
			r.Street1 = *s1
		}
		if s2 != nil {
			r.Street2 = *s2
		}
		if city != nil {
			r.City = *city
		}
		if state != nil {
			r.State = *state
		}
		if zip != nil {
			r.Zip = *zip
		}
		if website != nil {
			r.Website = *website
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (a *Activities) queryUnlinked5500(ctx context.Context, limit int) ([]UnlinkedRecord, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT DISTINCT ON (ein) ein, sponsor_name,
		       sponsor_street1, sponsor_street2, sponsor_city, sponsor_state, sponsor_zip
		FROM (
			SELECT ein, sponsor_name, sponsor_street1, sponsor_street2,
			       sponsor_city, sponsor_state, sponsor_zip
			FROM fed_data.form_5500
			UNION ALL
			SELECT ein, sponsor_name, sponsor_street1, sponsor_street2,
			       sponsor_city, sponsor_state, sponsor_zip
			FROM fed_data.form_5500_sf
		) combined
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = combined.ein
			AND cm.matched_source = 'form_5500'
		WHERE cm.id IS NULL AND combined.ein IS NOT NULL AND combined.ein != ''
		ORDER BY ein, sponsor_name
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked 5500 sponsors")
	}
	defer rows.Close()

	var records []UnlinkedRecord
	for rows.Next() {
		var r UnlinkedRecord
		var s1, s2, city, state, zip *string
		if err := rows.Scan(&r.Key, &r.Name, &s1, &s2, &city, &state, &zip); err != nil {
			return nil, eris.Wrap(err, "scan 5500 sponsor")
		}
		if s1 != nil {
			r.Street1 = *s1
		}
		if s2 != nil {
			r.Street2 = *s2
		}
		if city != nil {
			r.City = *city
		}
		if state != nil {
			r.State = *state
		}
		if zip != nil {
			r.Zip = *zip
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (a *Activities) queryUnlinked990(ctx context.Context, limit int) ([]UnlinkedRecord, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT ein, name, street, city, state, zip
		FROM fed_data.eo_bmf
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = eo_bmf.ein
			AND cm.matched_source = 'eo_bmf'
		WHERE cm.id IS NULL AND eo_bmf.status = 1
		ORDER BY eo_bmf.asset_amt DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked 990 orgs")
	}
	defer rows.Close()

	var records []UnlinkedRecord
	for rows.Next() {
		var r UnlinkedRecord
		var street, city, state, zip *string
		if err := rows.Scan(&r.Key, &r.Name, &street, &city, &state, &zip); err != nil {
			return nil, eris.Wrap(err, "scan 990 org")
		}
		if street != nil {
			r.Street1 = *street
		}
		if city != nil {
			r.City = *city
		}
		if state != nil {
			r.State = *state
		}
		if zip != nil {
			r.Zip = *zip
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (a *Activities) queryUnlinkedFDIC(ctx context.Context, limit int) ([]UnlinkedRecord, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT fi.cert::text, fi.name,
		       fi.address, fi.city, fi.stalp, fi.zip, fi.webaddr,
		       fi.latitude, fi.longitude
		FROM fed_data.fdic_institutions fi
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = fi.cert::text
			AND cm.matched_source = 'fdic_institutions'
		WHERE cm.id IS NULL AND fi.active = 1
		ORDER BY fi.asset DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked FDIC institutions")
	}
	defer rows.Close()

	var records []UnlinkedRecord
	for rows.Next() {
		var r UnlinkedRecord
		var addr, city, state, zip, website *string
		var lat, lng *float64
		if err := rows.Scan(&r.Key, &r.Name, &addr, &city, &state, &zip, &website, &lat, &lng); err != nil {
			return nil, eris.Wrap(err, "scan FDIC institution")
		}
		if addr != nil {
			r.Street1 = *addr
		}
		if city != nil {
			r.City = *city
		}
		if state != nil {
			r.State = *state
		}
		if zip != nil {
			r.Zip = *zip
		}
		if website != nil {
			r.Website = *website
		}
		r.Latitude = lat
		r.Longitude = lng
		records = append(records, r)
	}
	return records, rows.Err()
}

// ProcessBatchParams is the input for ProcessGeoBackfillBatch.
type ProcessBatchParams struct {
	Source  string           `json:"source"`
	Records []UnlinkedRecord `json:"records"`
	SkipMSA bool             `json:"skip_msa"`
}

// ProcessBatchResult is the output of ProcessGeoBackfillBatch.
type ProcessBatchResult struct {
	Created  int `json:"created"`
	Geocoded int `json:"geocoded"`
	Linked   int `json:"linked"`
	MSAs     int `json:"msas"`
	Branches int `json:"branches"`
	Failed   int `json:"failed"`
}

// ProcessGeoBackfillBatch processes a batch of unlinked records: creates stub companies,
// upserts addresses, geocodes, associates MSAs, and links via company_matches.
func (a *Activities) ProcessGeoBackfillBatch(ctx context.Context, params ProcessBatchParams) (*ProcessBatchResult, error) {
	log := zap.L().With(zap.String("source", params.Source))
	result := &ProcessBatchResult{}

	for i, rec := range params.Records {
		// Heartbeat every 10 records.
		if i > 0 && i%10 == 0 {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("processed %d/%d", i, len(params.Records)))
		}

		if err := a.processRecord(ctx, params.Source, rec, params.SkipMSA, result); err != nil {
			log.Warn("failed to process record",
				zap.String("key", rec.Key),
				zap.Error(err),
			)
			result.Failed++
		}
	}

	return result, nil
}

func (a *Activities) processRecord(ctx context.Context, source string, rec UnlinkedRecord, skipMSA bool, result *ProcessBatchResult) error {
	// Determine match source and type from the backfill source.
	// Address source is special — only geocodes existing addresses, no company creation.
	if source == "address" {
		return a.processAddressRecord(ctx, rec, skipMSA, result)
	}

	var matchSource, matchType, addrSource, identSystem string
	switch source {
	case "adv":
		matchSource = "adv_firms"
		matchType = "direct_crd"
		addrSource = "adv_firms"
		identSystem = company.SystemCRD
	case "5500":
		matchSource = "form_5500"
		matchType = "direct_ein"
		addrSource = "form_5500"
		identSystem = company.SystemEIN
	case "990":
		matchSource = "eo_bmf"
		matchType = "direct_ein"
		addrSource = "eo_bmf"
		identSystem = company.SystemEIN
	case "fdic":
		matchSource = "fdic_institutions"
		matchType = "direct_fdic_cert"
		addrSource = "fdic_institutions"
		identSystem = company.SystemFDIC
	case "sba":
		matchSource = "sba_loans"
		matchType = "direct_sba_loan"
		addrSource = "sba_loans"
		identSystem = company.SystemSBALoan
	}

	// 1. Create stub company.
	cr := &company.CompanyRecord{
		Name:    rec.Name,
		Domain:  rec.Website,
		Website: rec.Website,
		City:    rec.City,
		State:   rec.State,
		Country: "US",
	}
	if err := a.cs.CreateCompany(ctx, cr); err != nil {
		return eris.Wrap(err, "create stub company")
	}
	result.Created++

	// 2. Upsert identifier (for FDIC and 990/5500 EIN).
	if identSystem != "" && source != "adv" {
		if err := a.cs.UpsertIdentifier(ctx, &company.Identifier{
			CompanyID:  cr.ID,
			System:     identSystem,
			Identifier: rec.Key,
		}); err != nil {
			zap.L().Warn("failed to upsert identifier", zap.String("key", rec.Key), zap.Error(err))
		}
	}

	// 3. Create address.
	street := strings.TrimSpace(rec.Street1 + " " + rec.Street2)
	conf := 1.0
	addr := &company.Address{
		CompanyID:   cr.ID,
		AddressType: company.AddressPrincipal,
		Street:      street,
		City:        rec.City,
		State:       rec.State,
		ZipCode:     rec.Zip,
		Country:     "US",
		IsPrimary:   true,
		Source:      addrSource,
		Confidence:  &conf,
	}
	if err := a.cs.UpsertAddress(ctx, addr); err != nil {
		return eris.Wrap(err, "upsert address")
	}

	// 4. Geocode.
	geocoded := false
	var lat, lng float64
	if source == "fdic" && rec.Latitude != nil && rec.Longitude != nil && *rec.Latitude != 0 && *rec.Longitude != 0 {
		// Use FDIC-provided coordinates.
		lat, lng = *rec.Latitude, *rec.Longitude
		if err := a.cs.UpdateAddressGeocode(ctx, addr.ID, lat, lng, "fdic", "provider", ""); err != nil {
			zap.L().Warn("failed to update geocode from FDIC", zap.String("key", rec.Key), zap.Error(err))
		} else {
			geocoded = true
		}
	} else {
		gcResult, err := a.gc.Geocode(ctx, geocode.AddressInput{
			ID:      fmt.Sprintf("%d", addr.ID),
			Street:  street,
			City:    rec.City,
			State:   rec.State,
			ZipCode: rec.Zip,
		})
		if err != nil {
			zap.L().Debug("geocode failed", zap.String("key", rec.Key), zap.Error(err))
		} else if gcResult.Matched {
			lat, lng = gcResult.Latitude, gcResult.Longitude
			if err := a.cs.UpdateAddressGeocode(ctx, addr.ID, lat, lng, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); err != nil {
				zap.L().Warn("failed to update geocode", zap.String("key", rec.Key), zap.Error(err))
			} else {
				geocoded = true
			}
		}
	}

	if geocoded {
		result.Geocoded++
		// 5. Associate with MSAs.
		if !skipMSA && a.assoc != nil {
			relations, err := a.assoc.AssociateAddress(ctx, addr.ID, lat, lng, a.topMSAs)
			if err != nil {
				zap.L().Warn("MSA association failed", zap.String("key", rec.Key), zap.Error(err))
			} else {
				result.MSAs += len(relations)
			}
		}
	}

	// 6. FDIC branch processing.
	if source == "fdic" {
		bc, bmc, err := a.backfillFDICBranches(ctx, rec.Key, cr.ID, skipMSA)
		if err != nil {
			zap.L().Warn("branch backfill failed", zap.String("key", rec.Key), zap.Error(err))
		} else {
			result.Branches += bc
			result.MSAs += bmc
		}
	}

	// 7. Link via company_matches.
	matchConf := 1.0
	match := &company.Match{
		CompanyID:     cr.ID,
		MatchedSource: matchSource,
		MatchedKey:    rec.Key,
		MatchType:     matchType,
		Confidence:    &matchConf,
	}
	if err := a.cs.UpsertMatch(ctx, match); err != nil {
		return eris.Wrap(err, "upsert match")
	}
	result.Linked++
	return nil
}

func (a *Activities) queryUnlinkedSBA(ctx context.Context, limit int) ([]UnlinkedRecord, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT DISTINCT ON (s.l2locid)
		       s.l2locid::text, s.borrname,
		       s.borrstreet, s.borrcity, s.borrstate, s.borrzip
		FROM fed_data.sba_loans s
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = s.l2locid::text
			AND cm.matched_source = 'sba_loans'
		WHERE cm.id IS NULL
		  AND s.borrname IS NOT NULL AND s.borrname != ''
		ORDER BY s.l2locid, s.approvalfiscalyear DESC NULLS LAST, s.grossapproval DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked SBA borrowers")
	}
	defer rows.Close()

	var records []UnlinkedRecord
	for rows.Next() {
		var r UnlinkedRecord
		var street, city, state, zip *string
		if err := rows.Scan(&r.Key, &r.Name, &street, &city, &state, &zip); err != nil {
			return nil, eris.Wrap(err, "scan SBA borrower")
		}
		if street != nil {
			r.Street1 = *street
		}
		if city != nil {
			r.City = *city
		}
		if state != nil {
			r.State = *state
		}
		if zip != nil {
			r.Zip = *zip
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (a *Activities) queryUngeocodedAddresses(ctx context.Context, limit int) ([]UnlinkedRecord, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT ca.id::text, COALESCE(c.name, ''),
		       ca.street, ca.city, ca.state, ca.zip_code
		FROM public.company_addresses ca
		JOIN public.companies c ON c.id = ca.company_id
		WHERE ca.latitude IS NULL
		ORDER BY ca.id
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query ungeocoded addresses")
	}
	defer rows.Close()

	var records []UnlinkedRecord
	for rows.Next() {
		var r UnlinkedRecord
		var street, city, state, zip *string
		if err := rows.Scan(&r.Key, &r.Name, &street, &city, &state, &zip); err != nil {
			return nil, eris.Wrap(err, "scan ungeocoded address")
		}
		if street != nil {
			r.Street1 = *street
		}
		if city != nil {
			r.City = *city
		}
		if state != nil {
			r.State = *state
		}
		if zip != nil {
			r.Zip = *zip
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

func (a *Activities) processAddressRecord(ctx context.Context, rec UnlinkedRecord, skipMSA bool, result *ProcessBatchResult) error {
	var addrID int64
	if _, err := fmt.Sscanf(rec.Key, "%d", &addrID); err != nil {
		return eris.Wrapf(err, "parse address ID %q", rec.Key)
	}

	street := strings.TrimSpace(rec.Street1 + " " + rec.Street2)
	gcResult, err := a.gc.Geocode(ctx, geocode.AddressInput{
		ID:      rec.Key,
		Street:  street,
		City:    rec.City,
		State:   rec.State,
		ZipCode: rec.Zip,
	})
	if err != nil {
		return eris.Wrap(err, "geocode address")
	}
	if !gcResult.Matched {
		return nil
	}

	if err := a.cs.UpdateAddressGeocode(ctx, addrID, gcResult.Latitude, gcResult.Longitude, gcResult.Source, gcResult.Quality, gcResult.CountyFIPS); err != nil {
		return eris.Wrap(err, "update address geocode")
	}
	result.Geocoded++

	if !skipMSA && a.assoc != nil {
		relations, err := a.assoc.AssociateAddress(ctx, addrID, gcResult.Latitude, gcResult.Longitude, a.topMSAs)
		if err != nil {
			zap.L().Warn("MSA association failed", zap.String("address_id", rec.Key), zap.Error(err))
		} else {
			result.MSAs += len(relations)
		}
	}

	return nil
}

func (a *Activities) backfillFDICBranches(ctx context.Context, certStr string, companyID int64, skipMSA bool) (int, int, error) {
	rows, err := a.pool.Query(ctx, `
		SELECT uni_num, off_name, address, city, stalp, zip, county,
		       latitude, longitude, main_off, stcnty
		FROM fed_data.fdic_branches WHERE cert::text = $1`, certStr)
	if err != nil {
		return 0, 0, eris.Wrapf(err, "query branches for cert %s", certStr)
	}
	defer rows.Close()

	var branchCount, msaCount int
	for rows.Next() {
		var uniNum, mainOff *int
		var offName, addr, city, state, zip, county, stcnty *string
		var lat, lng *float64
		if err := rows.Scan(&uniNum, &offName, &addr, &city, &state, &zip, &county, &lat, &lng, &mainOff, &stcnty); err != nil {
			return branchCount, msaCount, eris.Wrap(err, "scan branch")
		}

		branchAddr := &company.Address{
			CompanyID:   companyID,
			AddressType: company.AddressBranch,
			IsPrimary:   false,
			Source:      "fdic_branches",
			Country:     "US",
		}
		if addr != nil {
			branchAddr.Street = strings.TrimSpace(*addr)
		}
		if city != nil {
			branchAddr.City = *city
		}
		if state != nil {
			branchAddr.State = *state
		}
		if zip != nil {
			branchAddr.ZipCode = *zip
		}
		if stcnty != nil {
			branchAddr.CountyFIPS = *stcnty
		}

		if err := a.cs.UpsertAddress(ctx, branchAddr); err != nil {
			zap.L().Debug("failed to create branch address", zap.String("cert", certStr), zap.Error(err))
			continue
		}
		branchCount++

		if lat != nil && lng != nil && *lat != 0 && *lng != 0 {
			if err := a.cs.UpdateAddressGeocode(ctx, branchAddr.ID, *lat, *lng, "fdic", "provider", ""); err != nil {
				zap.L().Debug("failed to update branch geocode", zap.String("cert", certStr), zap.Error(err))
			} else if !skipMSA && a.assoc != nil {
				relations, err := a.assoc.AssociateAddress(ctx, branchAddr.ID, *lat, *lng, a.topMSAs)
				if err != nil {
					zap.L().Debug("branch MSA association failed", zap.String("cert", certStr), zap.Error(err))
				} else {
					msaCount += len(relations)
				}
			}
		}
	}
	return branchCount, msaCount, rows.Err()
}
