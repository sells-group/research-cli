package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFPDS_Metadata(t *testing.T) {
	ds := &FPDS{}
	assert.Equal(t, "fpds", ds.Name())
	assert.Equal(t, "fed_data.fpds_contracts", ds.Table())
	assert.Equal(t, Phase1, ds.Phase())
	assert.Equal(t, Daily, ds.Cadence())
}

func TestFPDS_ShouldRun(t *testing.T) {
	ds := &FPDS{}

	// Never synced -> should run
	now := time.Date(2024, time.June, 15, 12, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, nil))

	// Synced yesterday -> should run
	yesterday := time.Date(2024, time.June, 14, 10, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &yesterday))

	// Synced today -> should not run
	today := time.Date(2024, time.June, 15, 8, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &today))

	// Synced 3 days ago -> should run
	threeDaysAgo := time.Date(2024, time.June, 12, 0, 0, 0, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &threeDaysAgo))

	// Synced at midnight today -> should not run
	midnight := time.Date(2024, time.June, 15, 0, 0, 0, 0, time.UTC)
	assert.False(t, ds.ShouldRun(now, &midnight))

	// Synced just before midnight yesterday -> should run
	beforeMidnight := time.Date(2024, time.June, 14, 23, 59, 59, 0, time.UTC)
	assert.True(t, ds.ShouldRun(now, &beforeMidnight))
}

func TestFPDS_ParseResponse(t *testing.T) {
	ds := &FPDS{}

	// Valid response with one opportunity
	data := []byte(`{
		"totalRecords": 1,
		"opportunitiesData": [{
			"noticeId": "CONTRACT-001",
			"solicitationNumber": "PIID-001",
			"fullParentPathName": "Department of Treasury",
			"fullParentPathCode": "2000",
			"title": "Financial advisory services",
			"naicsCode": "523110",
			"classificationCode": "R408",
			"postedDate": "2024-06-01",
			"award": {
				"amount": 150000.50,
				"date": "2024-06-10",
				"awardee": {
					"name": "Acme Financial LLC",
					"ueiSAM": "ABC123456789",
					"duns": "1234567890123",
					"location": {
						"city": "New York",
						"state": "NY",
						"zip": "10001"
					}
				}
			}
		}]
	}`)

	rows, hasMore, err := ds.parseResponse(data)
	assert.NoError(t, err)
	assert.False(t, hasMore)
	assert.Len(t, rows, 1)

	row := rows[0]
	assert.Equal(t, "CONTRACT-001", row[0])       // contract_id
	assert.Equal(t, "PIID-001", row[1])           // piid
	assert.Equal(t, "2000", row[2])               // agency_id
	assert.Equal(t, "Acme Financial LLC", row[4]) // vendor_name
	assert.Equal(t, "ABC123456789", row[6])       // vendor_uei
	assert.Equal(t, "NY", row[8])                 // vendor_state
	assert.Equal(t, "523110", row[10])            // naics
	assert.Equal(t, "R408", row[11])              // psc
	assert.Equal(t, 150000.50, row[13])           // dollars_obligated

	// Empty response
	emptyData := []byte(`{"totalRecords": 0, "opportunitiesData": []}`)
	rows, hasMore, err = ds.parseResponse(emptyData)
	assert.NoError(t, err)
	assert.False(t, hasMore)
	assert.Empty(t, rows)
}

func TestFPDS_ParseResponse_HasMore(t *testing.T) {
	ds := &FPDS{}

	// Build response with exactly fpdsPageSize items to trigger hasMore
	opps := `[`
	for i := 0; i < fpdsPageSize; i++ {
		if i > 0 {
			opps += ","
		}
		opps += `{"noticeId":"` + "ID-" + string(rune('A'+i%26)) + `","title":"test","naicsCode":"52"}`
	}
	opps += `]`

	data := []byte(`{"totalRecords": 200, "opportunitiesData": ` + opps + `}`)
	_, hasMore, err := ds.parseResponse(data)
	assert.NoError(t, err)
	assert.True(t, hasMore)
}
