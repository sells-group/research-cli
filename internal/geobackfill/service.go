// Package geobackfill provides reusable geo entity backfill services for CLI and Temporal flows.
package geobackfill

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rotisserie/eris"

	"github.com/sells-group/research-cli/internal/company"
	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/db"
	igeo "github.com/sells-group/research-cli/internal/geo"
	"github.com/sells-group/research-cli/pkg/geocode"
)

// Source identifies a geo entity backfill source.
type Source string

const (
	// SourceADV backfills SEC ADV firms.
	SourceADV Source = "adv"
	// Source5500 backfills Form 5500 sponsors.
	Source5500 Source = "5500"
	// Source990 backfills IRS EO BMF organizations.
	Source990 Source = "990"
	// SourceFDIC backfills FDIC institutions and branches.
	SourceFDIC Source = "fdic"
	// SourceNCUA backfills NCUA credit unions.
	SourceNCUA Source = "ncua"
	// SourceSBA backfills SBA loan borrowers.
	SourceSBA Source = "sba"
	// SourceUSAspending backfills USAspending recipients.
	SourceUSAspending Source = "usaspending"
)

// Record represents a single unlinked entity that needs company/geo backfill.
type Record struct {
	Key       string   `json:"key"`
	Name      string   `json:"name"`
	Street1   string   `json:"street1,omitempty"`
	Street2   string   `json:"street2,omitempty"`
	City      string   `json:"city,omitempty"`
	State     string   `json:"state,omitempty"`
	Zip       string   `json:"zip,omitempty"`
	Website   string   `json:"website,omitempty"`
	Country   string   `json:"country,omitempty"`
	DUNS      string   `json:"duns,omitempty"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
}

// RunOptions configures a direct service run.
type RunOptions struct {
	Source       Source `json:"source"`
	Limit        int    `json:"limit"`
	BatchSize    int    `json:"batch_size"`
	SkipMSA      bool   `json:"skip_msa"`
	SkipBranches bool   `json:"skip_branches"`
}

// Result summarizes a geo entity backfill run.
type Result struct {
	TotalRecords int `json:"total_records"`
	Created      int `json:"created"`
	Geocoded     int `json:"geocoded"`
	Linked       int `json:"linked"`
	MSAs         int `json:"msas"`
	Branches     int `json:"branches"`
	Failed       int `json:"failed"`
}

type msaAssociator interface {
	AssociateAddress(ctx context.Context, addressID int64, lat, lon float64, topN int) ([]igeo.MSARelation, error)
}

// Service runs entity backfill logic independently of CLI or Temporal wrappers.
type Service struct {
	pool    db.Pool
	store   company.CompanyStore
	geocode geocode.Client
	assoc   msaAssociator
	topMSAs int
}

// NewService creates a geo entity backfill service.
func NewService(pool db.Pool, store company.CompanyStore, geocoder geocode.Client, assoc msaAssociator, cfg *config.Config) *Service {
	topMSAs := 3
	if cfg != nil && cfg.Geo.TopMSAs > 0 {
		topMSAs = cfg.Geo.TopMSAs
	}

	return &Service{
		pool:    pool,
		store:   store,
		geocode: geocoder,
		assoc:   assoc,
		topMSAs: topMSAs,
	}
}

// QueryUnlinkedRecords fetches unlinked source records.
func (s *Service) QueryUnlinkedRecords(ctx context.Context, source Source, limit int) ([]Record, error) {
	switch source {
	case SourceADV:
		return s.queryADV(ctx, limit)
	case Source5500:
		return s.query5500(ctx, limit)
	case Source990:
		return s.query990(ctx, limit)
	case SourceFDIC:
		return s.queryFDIC(ctx, limit)
	case SourceNCUA:
		return s.queryNCUA(ctx, limit)
	case SourceSBA:
		return s.querySBA(ctx, limit)
	case SourceUSAspending:
		return s.queryUSAspending(ctx, limit)
	default:
		return nil, eris.Errorf("geobackfill: unknown source %s", source)
	}
}

// Run queries and processes records in batches.
func (s *Service) Run(ctx context.Context, opts RunOptions) (*Result, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}

	records, err := s.QueryUnlinkedRecords(ctx, opts.Source, opts.Limit)
	if err != nil {
		return nil, err
	}

	result := &Result{TotalRecords: len(records)}
	for start := 0; start < len(records); start += opts.BatchSize {
		end := start + opts.BatchSize
		if end > len(records) {
			end = len(records)
		}

		batchResult, batchErr := s.ProcessBatch(ctx, opts.Source, records[start:end], opts.SkipMSA, opts.SkipBranches)
		if batchErr != nil {
			result.Failed += end - start
			continue
		}

		result.Created += batchResult.Created
		result.Geocoded += batchResult.Geocoded
		result.Linked += batchResult.Linked
		result.MSAs += batchResult.MSAs
		result.Branches += batchResult.Branches
		result.Failed += batchResult.Failed
	}

	return result, nil
}

// ProcessBatch processes a slice of records for one source.
func (s *Service) ProcessBatch(ctx context.Context, source Source, records []Record, skipMSA bool, skipBranches bool) (*Result, error) {
	result := &Result{}
	for _, rec := range records {
		if err := s.processRecord(ctx, source, rec, skipMSA, skipBranches, result); err != nil {
			result.Failed++
		}
	}
	return result, nil
}

func (s *Service) processRecord(ctx context.Context, source Source, rec Record, skipMSA bool, skipBranches bool, result *Result) error {
	matchSource, matchType, addrSource, identSystem, addressType := sourceMetadata(source)

	companyRecord := &company.CompanyRecord{
		Name:    rec.Name,
		Domain:  rec.Website,
		Website: rec.Website,
		City:    rec.City,
		State:   rec.State,
		Country: countryOrUS(rec.Country),
	}
	if err := s.store.CreateCompany(ctx, companyRecord); err != nil {
		return eris.Wrap(err, "create stub company")
	}
	result.Created++

	for _, identifier := range identifiersForSource(source, rec, identSystem) {
		if err := s.store.UpsertIdentifier(ctx, identifierForCompany(companyRecord.ID, identifier.system, identifier.value)); err != nil {
			continue
		}
	}

	street := strings.TrimSpace(rec.Street1 + " " + rec.Street2)
	confidence := 1.0
	address := &company.Address{
		CompanyID:   companyRecord.ID,
		AddressType: addressType,
		Street:      street,
		City:        rec.City,
		State:       rec.State,
		ZipCode:     rec.Zip,
		Country:     countryOrUS(rec.Country),
		IsPrimary:   true,
		Source:      addrSource,
		Confidence:  &confidence,
	}
	if err := s.store.UpsertAddress(ctx, address); err != nil {
		return eris.Wrap(err, "upsert address")
	}

	geocoded, msaCount, err := s.geocodeAndAssociate(ctx, source, rec, address, skipMSA)
	if err != nil {
		return err
	}
	if geocoded {
		result.Geocoded++
		result.MSAs += msaCount
	}

	if source == SourceFDIC && !skipBranches {
		branchCount, branchMSAs, branchErr := s.backfillFDICBranches(ctx, rec.Key, companyRecord.ID, skipMSA)
		if branchErr == nil {
			result.Branches += branchCount
			result.MSAs += branchMSAs
		}
	}

	matchConfidence := 1.0
	if err := s.store.UpsertMatch(ctx, &company.Match{
		CompanyID:     companyRecord.ID,
		MatchedSource: matchSource,
		MatchedKey:    rec.Key,
		MatchType:     matchType,
		Confidence:    &matchConfidence,
	}); err != nil {
		return eris.Wrap(err, "upsert match")
	}
	result.Linked++
	return nil
}

func (s *Service) geocodeAndAssociate(ctx context.Context, source Source, rec Record, address *company.Address, skipMSA bool) (bool, int, error) {
	if source == SourceFDIC && rec.Latitude != nil && rec.Longitude != nil && *rec.Latitude != 0 && *rec.Longitude != 0 {
		if err := s.store.UpdateAddressGeocode(ctx, address.ID, *rec.Latitude, *rec.Longitude, "fdic", "provider", ""); err != nil {
			return false, 0, eris.Wrap(err, "update FDIC address geocode")
		}
		msaCount, _ := s.associate(ctx, address.ID, *rec.Latitude, *rec.Longitude, skipMSA)
		return true, msaCount, nil
	}

	if s.geocode == nil {
		return false, 0, nil
	}

	result, err := s.geocode.Geocode(ctx, geocode.AddressInput{
		ID:      fmt.Sprintf("%d", address.ID),
		Street:  address.Street,
		City:    address.City,
		State:   address.State,
		ZipCode: address.ZipCode,
	})
	if err != nil {
		return false, 0, eris.Wrap(err, "geocode address")
	}
	if result == nil || !result.Matched {
		return false, 0, nil
	}

	if err := s.store.UpdateAddressGeocode(ctx, address.ID, result.Latitude, result.Longitude, result.Source, result.Quality, result.CountyFIPS); err != nil {
		return false, 0, eris.Wrap(err, "update geocoded address")
	}

	msaCount, _ := s.associate(ctx, address.ID, result.Latitude, result.Longitude, skipMSA)
	return true, msaCount, nil
}

func (s *Service) associate(ctx context.Context, addressID int64, lat, lon float64, skipMSA bool) (int, error) {
	if skipMSA || s.assoc == nil {
		return 0, nil
	}

	relations, err := s.assoc.AssociateAddress(ctx, addressID, lat, lon, s.topMSAs)
	if err != nil {
		return 0, err
	}
	return len(relations), nil
}

func (s *Service) backfillFDICBranches(ctx context.Context, cert string, companyID int64, skipMSA bool) (int, int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT uni_num, off_name, address, city, stalp, zip, county,
		       latitude, longitude, main_off, stcnty
		FROM fed_data.fdic_branches WHERE cert::text = $1`, cert)
	if err != nil {
		return 0, 0, eris.Wrapf(err, "query branches for cert %s", cert)
	}
	defer rows.Close()

	var branchCount int
	var msaCount int
	for rows.Next() {
		var uniNum, mainOff *int
		var offName, street, city, state, zip, county, stcnty *string
		var lat, lon *float64
		if err := rows.Scan(&uniNum, &offName, &street, &city, &state, &zip, &county, &lat, &lon, &mainOff, &stcnty); err != nil {
			return branchCount, msaCount, eris.Wrap(err, "scan branch")
		}

		addr := &company.Address{
			CompanyID:   companyID,
			AddressType: company.AddressBranch,
			Street:      derefString(street),
			City:        derefString(city),
			State:       derefString(state),
			ZipCode:     derefString(zip),
			Country:     "US",
			IsPrimary:   false,
			Source:      "fdic_branches",
			CountyFIPS:  derefString(stcnty),
		}
		if err := s.store.UpsertAddress(ctx, addr); err != nil {
			continue
		}
		branchCount++

		if lat != nil && lon != nil && *lat != 0 && *lon != 0 {
			if err := s.store.UpdateAddressGeocode(ctx, addr.ID, *lat, *lon, "fdic", "provider", ""); err == nil {
				associated, _ := s.associate(ctx, addr.ID, *lat, *lon, skipMSA)
				msaCount += associated
			}
		}
	}

	return branchCount, msaCount, rows.Err()
}

func (s *Service) queryADV(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
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
	return scanOptionalAddressRecords(rows, true)
}

func (s *Service) query5500(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
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
	return scanOptionalAddressRecords(rows, false)
}

func (s *Service) query990(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
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
	return scanOptionalAddressRecords(rows, false)
}

func (s *Service) queryFDIC(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
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

	var records []Record
	for rows.Next() {
		var rec Record
		var street, city, state, zip, website *string
		var lat, lon *float64
		if err := rows.Scan(&rec.Key, &rec.Name, &street, &city, &state, &zip, &website, &lat, &lon); err != nil {
			return nil, eris.Wrap(err, "scan FDIC institution")
		}
		rec.Street1 = derefString(street)
		rec.City = derefString(city)
		rec.State = derefString(state)
		rec.Zip = derefString(zip)
		rec.Website = derefString(website)
		rec.Latitude = lat
		rec.Longitude = lon
		records = append(records, rec)
	}
	return records, rows.Err()
}

func (s *Service) queryNCUA(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (n.cu_number) n.cu_number::text, n.cu_name,
		       n.street, n.city, n.state, n.zip_code
		FROM fed_data.ncua_call_reports n
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = n.cu_number::text
			AND cm.matched_source = 'ncua_call_reports'
		WHERE cm.id IS NULL
		ORDER BY n.cu_number, n.cycle_date DESC, n.total_assets DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked NCUA credit unions")
	}
	defer rows.Close()
	return scanOptionalAddressRecords(rows, false)
}

func (s *Service) querySBA(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (s.l2locid)
		       s.l2locid::text, s.borrname, s.borrstreet, NULL, s.borrcity, s.borrstate, s.borrzip
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
	return scanOptionalAddressRecords(rows, false)
}

func (s *Service) queryUSAspending(ctx context.Context, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `
		WITH recipients AS (
			SELECT DISTINCT ON (recipient_uei)
				recipient_uei AS uei, recipient_duns AS duns,
				recipient_name AS name,
				recipient_address_line_1 AS street,
				recipient_city AS city, recipient_state AS state,
				recipient_zip AS zip, recipient_country AS country,
				SUM(total_obligated_amount) OVER (PARTITION BY recipient_uei) AS total_obligated
			FROM fed_data.usaspending_awards
			WHERE recipient_uei IS NOT NULL AND recipient_uei != ''
				AND recipient_country = 'USA'
			ORDER BY recipient_uei, award_latest_action_date DESC
		)
		SELECT r.uei, r.name, r.street, NULL, r.city, r.state, r.zip, r.country, r.duns
		FROM recipients r
		LEFT JOIN public.company_matches cm
			ON cm.matched_key = r.uei AND cm.matched_source = 'usaspending_awards'
		WHERE cm.id IS NULL
		ORDER BY r.total_obligated DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, eris.Wrap(err, "query unlinked USAspending recipients")
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var rec Record
		var street1, street2, city, state, zip, country, duns *string
		if err := rows.Scan(&rec.Key, &rec.Name, &street1, &street2, &city, &state, &zip, &country, &duns); err != nil {
			return nil, eris.Wrap(err, "scan usaspending recipient")
		}
		rec.Street1 = derefString(street1)
		rec.Street2 = derefString(street2)
		rec.City = derefString(city)
		rec.State = derefString(state)
		rec.Zip = derefString(zip)
		rec.Country = derefString(country)
		rec.DUNS = derefString(duns)
		records = append(records, rec)
	}
	return records, rows.Err()
}

func scanOptionalAddressRecords(rows pgx.Rows, includeWebsite bool) ([]Record, error) {
	var records []Record
	for rows.Next() {
		var rec Record
		var street1, street2, city, state, zip, website *string
		if includeWebsite {
			if err := rows.Scan(&rec.Key, &rec.Name, &street1, &street2, &city, &state, &zip, &website); err != nil {
				return nil, eris.Wrap(err, "scan record")
			}
			rec.Website = derefString(website)
		} else {
			if err := rows.Scan(&rec.Key, &rec.Name, &street1, &street2, &city, &state, &zip); err != nil {
				return nil, eris.Wrap(err, "scan record")
			}
		}
		rec.Street1 = derefString(street1)
		rec.Street2 = derefString(street2)
		rec.City = derefString(city)
		rec.State = derefString(state)
		rec.Zip = derefString(zip)
		records = append(records, rec)
	}
	return records, rows.Err()
}

type identifierValue struct {
	system string
	value  string
}

func identifiersForSource(source Source, rec Record, defaultSystem string) []identifierValue {
	var out []identifierValue
	if defaultSystem != "" && rec.Key != "" {
		out = append(out, identifierValue{system: defaultSystem, value: rec.Key})
	}
	if source == SourceUSAspending && rec.DUNS != "" {
		out = append(out, identifierValue{system: company.SystemDUNS, value: rec.DUNS})
	}
	return out
}

func identifierForCompany(companyID int64, system, value string) *company.Identifier {
	return &company.Identifier{
		CompanyID:  companyID,
		System:     system,
		Identifier: value,
	}
}

func sourceMetadata(source Source) (matchSource, matchType, addrSource, identSystem, addressType string) {
	switch source {
	case SourceADV:
		return "adv_firms", "direct_crd", "adv_firms", company.SystemCRD, company.AddressPrincipal
	case Source5500:
		return "form_5500", "direct_ein", "form_5500", company.SystemEIN, company.AddressMailing
	case Source990:
		return "eo_bmf", "direct_ein", "eo_bmf", company.SystemEIN, company.AddressMailing
	case SourceFDIC:
		return "fdic_institutions", "direct_fdic_cert", "fdic_institutions", company.SystemFDIC, company.AddressPrincipal
	case SourceNCUA:
		return "ncua_call_reports", "direct_ncua_charter", "ncua_call_reports", company.SystemNCUA, company.AddressPrincipal
	case SourceSBA:
		return "sba_loans", "direct_sba_loan", "sba_loans", company.SystemSBALoan, company.AddressPrincipal
	case SourceUSAspending:
		return "usaspending_awards", "direct_uei", "usaspending_awards", company.SystemUEI, company.AddressMailing
	default:
		return "", "", "", "", company.AddressPrincipal
	}
}

func countryOrUS(country string) string {
	if strings.TrimSpace(country) == "" {
		return "US"
	}
	return country
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
