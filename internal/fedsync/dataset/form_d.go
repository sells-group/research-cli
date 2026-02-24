package dataset

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rotisserie/eris"
	"github.com/sells-group/research-cli/internal/db"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/config"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	formDSearchURL = "https://efts.sec.gov/LATEST/search-index"
	formDBatchSize = 2000
)

// FormD implements the SEC Form D dataset.
// Searches for new Form D filings via EDGAR EFTS API, downloads XML, and parses offering data.
type FormD struct {
	cfg *config.Config
}

func (d *FormD) Name() string     { return "form_d" }
func (d *FormD) Table() string    { return "fed_data.form_d" }
func (d *FormD) Phase() Phase     { return Phase1B }
func (d *FormD) Cadence() Cadence { return Daily }

func (d *FormD) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return DailySchedule(now, lastSync)
}

// formDSearchResult is the response from the EDGAR EFTS search for Form D filings.
type formDSearchResult struct {
	Hits struct {
		Total eftsTotal `json:"total"`
		Hits  []struct {
			Source struct {
				CIK             string `json:"entity_cik"`
				EntityName      string `json:"entity_name"`
				FormType        string `json:"form_type"`
				FilingDate      string `json:"file_date"`
				AccessionNumber string `json:"accession_no"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// formDXML represents the parsed Form D XML document.
type formDXML struct {
	XMLName         xml.Name      `xml:"edgarSubmission"`
	AccessionNumber string        `xml:"headerData>accessionNumber"`
	PrimaryIssuer   formDIssuer   `xml:"formData>issuerList>issuer"`
	OfferingData    formDOffering `xml:"formData>offeringData"`
}

type formDIssuer struct {
	CIK        string `xml:"issuerCIK"`
	EntityName string `xml:"issuerName"`
	EntityType string `xml:"issuerEntityType"`
	YearOfInc  string `xml:"issuerYearOfInc"`
	StateOfInc string `xml:"issuerStateOrCountryOfInc"`
}

type formDOffering struct {
	IndustryGroup string `xml:"industryGroup>industryGroupType"`
	RevenueRange  string `xml:"issuerSize>revenueRange"`
	TotalOffering int64  `xml:"offeringSalesAmounts>totalOfferingAmount"`
	TotalSold     int64  `xml:"offeringSalesAmounts>totalAmountSold"`
}

func (d *FormD) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "form_d"))

	// Search for Form D filings from the last 2 days to handle weekends.
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -2).Format("2006-01-02")
	endDate := now.Format("2006-01-02")

	searchURL := fmt.Sprintf(
		"%s?q=*&dateRange=custom&startdt=%s&enddt=%s&forms=D&from=0&size=200",
		formDSearchURL, startDate, endDate,
	)

	log.Info("searching for Form D filings",
		zap.String("start_date", startDate),
		zap.String("end_date", endDate),
	)

	body, err := f.Download(ctx, searchURL)
	if err != nil {
		return nil, eris.Wrap(err, "form_d: search EFTS")
	}

	result, err := fetcher.DecodeJSONObject[formDSearchResult](body)
	_ = body.Close()
	if err != nil {
		return nil, eris.Wrap(err, "form_d: decode search results")
	}

	log.Info("found Form D filings", zap.Int("total", result.Hits.Total.Value))

	columns := []string{"accession_number", "cik", "entity_name", "entity_type", "year_of_inc", "state_of_inc", "industry_group", "revenue_range", "total_offering", "total_sold", "filing_date"}
	conflictKeys := []string{"accession_number"}

	var batch [][]any
	var totalRows int64

	for _, hit := range result.Hits.Hits {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		src := hit.Source
		cik := strings.TrimLeft(src.CIK, "0")
		accession := src.AccessionNumber
		accessionClean := strings.ReplaceAll(accession, "-", "")

		// Download Form D XML.
		xmlURL := fmt.Sprintf(
			"https://www.sec.gov/Archives/edgar/data/%s/%s/primary_doc.xml",
			cik, accessionClean,
		)

		xmlPath := filepath.Join(tempDir, fmt.Sprintf("form_d_%s.xml", accessionClean))
		if _, dlErr := f.DownloadToFile(ctx, xmlURL, xmlPath); dlErr != nil {
			// Fall back to search metadata if XML download fails.
			filingDate := parseDate(src.FilingDate)
			row := []any{accession, cik, src.EntityName, "", "", "", "", "", int64(0), int64(0), filingDate}
			batch = append(batch, row)
			continue
		}

		xmlFile, err := os.Open(xmlPath)
		if err != nil {
			_ = os.Remove(xmlPath)
			continue
		}

		row, err := d.parseFormDXML(xmlFile, accession, cik, src.FilingDate)
		_ = xmlFile.Close()
		_ = os.Remove(xmlPath)

		if err != nil {
			log.Warn("form_d: parse XML failed", zap.String("accession", accession), zap.Error(err))
			filingDate := parseDate(src.FilingDate)
			row = []any{accession, cik, src.EntityName, "", "", "", "", "", int64(0), int64(0), filingDate}
		}

		batch = append(batch, row)

		if len(batch) >= formDBatchSize {
			n, upsertErr := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table: "fed_data.form_d", Columns: columns, ConflictKeys: conflictKeys,
			}, batch)
			if upsertErr != nil {
				return nil, eris.Wrap(upsertErr, "form_d: upsert")
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table: "fed_data.form_d", Columns: columns, ConflictKeys: conflictKeys,
		}, batch)
		if err != nil {
			return nil, eris.Wrap(err, "form_d: upsert final")
		}
		totalRows += n
	}

	log.Info("form_d sync complete", zap.Int64("rows", totalRows))

	return &SyncResult{
		RowsSynced: totalRows,
		Metadata: map[string]any{
			"filings_found": result.Hits.Total.Value,
		},
	}, nil
}

func (d *FormD) parseFormDXML(r io.Reader, accession, cik, filingDateStr string) ([]any, error) {
	var doc formDXML
	if err := xml.NewDecoder(r).Decode(&doc); err != nil {
		return nil, eris.Wrap(err, "form_d: decode XML")
	}

	issuer := doc.PrimaryIssuer
	offering := doc.OfferingData
	filingDate := parseDate(filingDateStr)

	stateOfInc := strings.TrimSpace(issuer.StateOfInc)
	if len(stateOfInc) > 2 {
		stateOfInc = stateOfInc[:2]
	}

	row := []any{
		accession,
		cik,
		strings.TrimSpace(issuer.EntityName),
		strings.TrimSpace(issuer.EntityType),
		strings.TrimSpace(issuer.YearOfInc),
		stateOfInc,
		strings.TrimSpace(offering.IndustryGroup),
		strings.TrimSpace(offering.RevenueRange),
		offering.TotalOffering,
		offering.TotalSold,
		filingDate,
	}

	return row, nil
}
