package tiger

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadURL_National(t *testing.T) {
	p, ok := ProductByName("STATE")
	require.True(t, ok)

	url := DownloadURL(p, 2024, "")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip", url)
}

func TestDownloadURL_PerState(t *testing.T) {
	p, ok := ProductByName("EDGES")
	require.True(t, ok)

	url := DownloadURL(p, 2024, "12")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/EDGES/tl_2024_12_edges.zip", url)
}

func TestProductByName_Found(t *testing.T) {
	p, ok := ProductByName("ADDR")
	assert.True(t, ok)
	assert.Equal(t, "addr", p.Table)
	assert.False(t, p.National)
}

func TestProductByName_NotFound(t *testing.T) {
	_, ok := ProductByName("NONEXISTENT")
	assert.False(t, ok)
}

func TestFIPSCodes(t *testing.T) {
	// Spot-check a few states.
	assert.Equal(t, "12", FIPSCodes["FL"])
	assert.Equal(t, "06", FIPSCodes["CA"])
	assert.Equal(t, "36", FIPSCodes["NY"])
	assert.Equal(t, "48", FIPSCodes["TX"])
	assert.Equal(t, "11", FIPSCodes["DC"])
}

func TestAbbrFromFIPS(t *testing.T) {
	abbr, ok := AbbrFromFIPS("12")
	assert.True(t, ok)
	assert.Equal(t, "FL", abbr)

	_, ok = AbbrFromFIPS("99")
	assert.False(t, ok)
}

func TestAllStateFIPS(t *testing.T) {
	fips := AllStateFIPS()
	assert.True(t, len(fips) > 50) // 50 states + DC + territories
	// Should be sorted.
	for i := 1; i < len(fips); i++ {
		assert.True(t, fips[i-1] <= fips[i], "FIPS codes should be sorted")
	}
}

func TestAllStateAbbrs(t *testing.T) {
	abbrs := AllStateAbbrs()
	assert.True(t, len(abbrs) > 50)
	// Should be sorted.
	for i := 1; i < len(abbrs); i++ {
		assert.True(t, abbrs[i-1] <= abbrs[i], "abbreviations should be sorted")
	}
}

func TestNationalProducts(t *testing.T) {
	natl := NationalProducts()
	for _, p := range natl {
		assert.True(t, p.National, "product %s should be national", p.Name)
	}
	assert.True(t, len(natl) >= 3)
}

func TestPerStateProducts(t *testing.T) {
	perState := PerStateProducts()
	for _, p := range perState {
		assert.False(t, p.National, "product %s should be per-state", p.Name)
	}
	assert.True(t, len(perState) >= 6)
}

func TestProducts_HaveColumns(t *testing.T) {
	for _, p := range Products {
		assert.True(t, len(p.Columns) > 0, "product %s should have columns", p.Name)
	}
}

func TestPerCountyProducts(t *testing.T) {
	perCounty := PerCountyProducts()
	require.True(t, len(perCounty) >= 4, "should have at least EDGES, FACES, ADDR, FEATNAMES")

	for _, p := range perCounty {
		assert.True(t, p.PerCounty, "product %s should be per-county", p.Name)
	}

	// Verify known per-county products are included.
	names := make(map[string]bool)
	for _, p := range perCounty {
		names[p.Name] = true
	}
	assert.True(t, names["EDGES"], "EDGES should be per-county")
	assert.True(t, names["FACES"], "FACES should be per-county")
	assert.True(t, names["ADDR"], "ADDR should be per-county")
	assert.True(t, names["FEATNAMES"], "FEATNAMES should be per-county")
}

func TestProduct_Template(t *testing.T) {
	// Product with explicit TemplateTable.
	p := Product{Name: "STATE", Table: "state_all", TemplateTable: "state"}
	assert.Equal(t, "state", p.Template())

	// Product without TemplateTable — should fall back to Table.
	p2 := Product{Name: "PLACE", Table: "place"}
	assert.Equal(t, "place", p2.Template())

	// Verify known products with explicit templates.
	state, ok := ProductByName("STATE")
	require.True(t, ok)
	assert.Equal(t, "state", state.Template())
	assert.NotEqual(t, state.Table, state.Template(), "STATE should have different Table and Template")

	county, ok := ProductByName("COUNTY")
	require.True(t, ok)
	assert.Equal(t, "county", county.Template())
	assert.NotEqual(t, county.Table, county.Template(), "COUNTY should have different Table and Template")

	// EDGES has no TemplateTable set — Template() should return Table.
	edges, ok := ProductByName("EDGES")
	require.True(t, ok)
	assert.Equal(t, "edges", edges.Template())
}

func TestDownloadURL_ZCTA(t *testing.T) {
	p, ok := ProductByName("ZCTA520")
	require.True(t, ok)
	assert.Equal(t, "zcta520", p.FileKey, "ZCTA520 should use custom FileKey")

	url := DownloadURL(p, 2024, "")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip", url)
}

func TestDownloadURL_PerCounty(t *testing.T) {
	p, ok := ProductByName("EDGES")
	require.True(t, ok)

	url := DownloadURL(p, 2024, "12086")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/EDGES/tl_2024_12086_edges.zip", url)
}

func TestDownloadURL_FallbackFileKey(t *testing.T) {
	// A product with empty FileKey should use Table as the file key.
	p := Product{Name: "PLACE", Table: "place", PerState: true}
	url := DownloadURL(p, 2024, "48")
	assert.Equal(t, "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/tl_2024_48_place.zip", url)
}

func TestCountyFIPSForState(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"fips"}).
		AddRow("48201").
		AddRow("48113").
		AddRow("48029")

	mock.ExpectQuery("SELECT statefp").
		WithArgs("48").
		WillReturnRows(rows)

	codes, err := CountyFIPSForState(context.Background(), mock, "48")
	require.NoError(t, err)
	assert.Equal(t, []string{"48201", "48113", "48029"}, codes)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCountyFIPSForState_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery("SELECT statefp").
		WithArgs("99").
		WillReturnError(assert.AnError)

	codes, err := CountyFIPSForState(context.Background(), mock, "99")
	require.Error(t, err)
	assert.Nil(t, codes)
	assert.Contains(t, err.Error(), "query county FIPS")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCountyFIPSForState_Empty(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"fips"})

	mock.ExpectQuery("SELECT statefp").
		WithArgs("11").
		WillReturnRows(rows)

	codes, err := CountyFIPSForState(context.Background(), mock, "11")
	require.NoError(t, err)
	assert.Empty(t, codes)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCountyFIPSForState_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"fips"}).
		AddRow("48201").
		RowError(0, assert.AnError)

	mock.ExpectQuery("SELECT statefp").
		WithArgs("48").
		WillReturnRows(rows)

	codes, err := CountyFIPSForState(context.Background(), mock, "48")
	require.Error(t, err)
	assert.Nil(t, codes)
	require.NoError(t, mock.ExpectationsWereMet())
}
