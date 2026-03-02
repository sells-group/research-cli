// Package tiger downloads Census TIGER/Line shapefiles and bulk-loads them
// into PostGIS tiger_data.* tables for geocoding.
package tiger

import (
	"fmt"
	"sort"
)

// Product describes a TIGER/Line shapefile product.
type Product struct {
	Name     string   // e.g., "EDGES"
	Table    string   // target table without state prefix, e.g., "edges"
	National bool     // true = single national file, false = per-state
	Columns  []string // DB columns (without geom)
	GeomType string   // "POINT", "LINESTRING", "POLYGON", "MULTIPOLYGON"
}

// Products lists all TIGER/Line products required for geocoding.
var Products = []Product{
	{
		Name:     "STATE",
		Table:    "state_all",
		National: true,
		Columns: []string{
			"region", "division", "statefp", "statens", "geoid", "stusps",
			"name", "lsad", "mtfcc", "funcstat", "aland", "awater",
			"intptlat", "intptlon",
		},
		GeomType: "MULTIPOLYGON",
	},
	{
		Name:     "COUNTY",
		Table:    "county_all",
		National: true,
		Columns: []string{
			"statefp", "countyfp", "countyns", "geoid", "name", "namelsad",
			"lsad", "classfp", "mtfcc", "csafp", "cbsafp", "metdivfp",
			"funcstat", "aland", "awater", "intptlat", "intptlon",
		},
		GeomType: "MULTIPOLYGON",
	},
	{
		Name:     "PLACE",
		Table:    "place",
		National: true,
		Columns: []string{
			"statefp", "placefp", "placens", "geoid", "name", "namelsad",
			"lsad", "classfp", "pcicbsa", "pcinecta", "mtfcc", "funcstat",
			"aland", "awater", "intptlat", "intptlon",
		},
		GeomType: "MULTIPOLYGON",
	},
	{
		Name:     "COUSUB",
		Table:    "cousub",
		National: true,
		Columns: []string{
			"statefp", "countyfp", "cousubfp", "cousubns", "geoid", "name",
			"namelsad", "lsad", "classfp", "mtfcc", "cnectafp", "nectafp",
			"nctadvfp", "funcstat", "aland", "awater", "intptlat", "intptlon",
		},
		GeomType: "MULTIPOLYGON",
	},
	{
		Name:     "ZCTA520",
		Table:    "zcta5",
		National: true,
		Columns: []string{
			"zcta5ce20", "geoid20", "classfp20", "mtfcc20", "funcstat20",
			"aland20", "awater20", "intptlat20", "intptlon20",
		},
		GeomType: "MULTIPOLYGON",
	},
	{
		Name:     "EDGES",
		Table:    "edges",
		National: false,
		Columns: []string{
			"tlid", "statefp", "countyfp", "fullname", "smtyp", "mtfcc",
			"lwflag", "offsetl", "offsetr", "tfidl", "tfidr", "zipl", "zipr",
		},
		GeomType: "MULTILINESTRING",
	},
	{
		Name:     "FACES",
		Table:    "faces",
		National: false,
		Columns: []string{
			"tfid", "statefp00", "countyfp00", "tractce00", "blkgrpce00",
			"blockce00", "cousubfp00", "submcdfp00", "conctyfp00", "placefp00",
			"aiession00", "comptyp00", "cpi00", "statefp", "countyfp",
			"tractce", "blkgrpce", "blockce", "cousubfp", "submcdfp",
			"conctyfp", "placefp", "aiession", "comptyp", "cpi", "lwflag",
		},
		GeomType: "MULTIPOLYGON",
	},
	{
		Name:     "ADDR",
		Table:    "addr",
		National: false,
		Columns: []string{
			"tlid", "fromhn", "tohn", "side", "zip", "plus4", "fromtyp",
			"totyp", "fromarmid", "toarmid", "aodo", "statefp",
		},
		GeomType: "",
	},
	{
		Name:     "FEATNAMES",
		Table:    "featnames",
		National: false,
		Columns: []string{
			"tlid", "fullname", "name", "predirabrv", "pretypabrv",
			"prequalabr", "sufdirabrv", "suftypabrv", "sufqualabr", "predir",
			"pretyp", "prequal", "sufdir", "suftyp", "sufqual", "linearid",
			"mtfcc", "paflag", "statefp",
		},
		GeomType: "",
	},
}

// FIPSCodes maps state abbreviation to 2-digit FIPS code for all 50 states + DC.
var FIPSCodes = map[string]string{
	"AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
	"CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
	"GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
	"IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
	"MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
	"MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
	"NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
	"OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
	"SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
	"VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
	"WY": "56",
}

// abbrByFIPS is a reverse lookup from FIPS code to state abbreviation.
var abbrByFIPS map[string]string

func init() {
	abbrByFIPS = make(map[string]string, len(FIPSCodes))
	for abbr, fips := range FIPSCodes {
		abbrByFIPS[fips] = abbr
	}
}

// AbbrFromFIPS returns the state abbreviation for a FIPS code.
func AbbrFromFIPS(fips string) (string, bool) {
	abbr, ok := abbrByFIPS[fips]
	return abbr, ok
}

// AllStateFIPS returns a sorted list of all state FIPS codes.
func AllStateFIPS() []string {
	codes := make([]string, 0, len(FIPSCodes))
	for _, fips := range FIPSCodes {
		codes = append(codes, fips)
	}
	sort.Strings(codes)
	return codes
}

// ProductByName looks up a product by its name (case-sensitive).
func ProductByName(name string) (Product, bool) {
	for _, p := range Products {
		if p.Name == name {
			return p, true
		}
	}
	return Product{}, false
}

// DownloadURL builds the Census Bureau download URL for a TIGER/Line shapefile.
// National products use tl_{year}_us_{table}.zip; per-state use tl_{year}_{fips}_{table}.zip.
func DownloadURL(product Product, year int, stateFIPS string) string {
	if product.National {
		return fmt.Sprintf(
			"https://www2.census.gov/geo/tiger/TIGER%d/%s/tl_%d_us_%s.zip",
			year, product.Name, year, product.Table,
		)
	}
	return fmt.Sprintf(
		"https://www2.census.gov/geo/tiger/TIGER%d/%s/tl_%d_%s_%s.zip",
		year, product.Name, year, stateFIPS, product.Table,
	)
}

// NationalProducts returns products with National=true.
func NationalProducts() []Product {
	var out []Product
	for _, p := range Products {
		if p.National {
			out = append(out, p)
		}
	}
	return out
}

// AllStateAbbrs returns a sorted list of state abbreviations (50 states + DC).
func AllStateAbbrs() []string {
	abbrs := make([]string, 0, len(FIPSCodes))
	for abbr := range FIPSCodes {
		abbrs = append(abbrs, abbr)
	}
	sort.Strings(abbrs)
	return abbrs
}

// PerStateProducts returns products with National=false.
func PerStateProducts() []Product {
	var out []Product
	for _, p := range Products {
		if !p.National {
			out = append(out, p)
		}
	}
	return out
}
