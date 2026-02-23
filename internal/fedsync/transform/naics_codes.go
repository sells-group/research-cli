package transform

// NAICSTitles maps NAICS codes to their official titles.
// Includes 2-digit sector titles and common 6-digit codes.
// Source: Census Bureau 2022 NAICS
var NAICSTitles = map[string]string{
	// 2-digit sector titles
	"11": "Agriculture, Forestry, Fishing and Hunting",
	"21": "Mining, Quarrying, and Oil and Gas Extraction",
	"22": "Utilities",
	"23": "Construction",
	"31": "Manufacturing",
	"32": "Manufacturing",
	"33": "Manufacturing",
	"42": "Wholesale Trade",
	"44": "Retail Trade",
	"45": "Retail Trade",
	"48": "Transportation and Warehousing",
	"49": "Transportation and Warehousing",
	"51": "Information",
	"52": "Finance and Insurance",
	"53": "Real Estate and Rental and Leasing",
	"54": "Professional, Scientific, and Technical Services",
	"55": "Management of Companies and Enterprises",
	"56": "Administrative and Support and Waste Management",
	"61": "Educational Services",
	"62": "Health Care and Social Assistance",
	"71": "Arts, Entertainment, and Recreation",
	"72": "Accommodation and Food Services",
	"81": "Other Services (except Public Administration)",
	"92": "Public Administration",
	// Common 6-digit codes encountered in enrichment
	"221114": "Solar Electric Power Generation",
	"236115": "New Single-Family Housing Construction (except For-Sale Builders)",
	"236116": "New Multifamily Housing Construction (except For-Sale Builders)",
	"236118": "Residential Remodelers",
	"236210": "Industrial Building Construction",
	"236220": "Commercial and Institutional Building Construction",
	"238110": "Poured Concrete Foundation and Structure Contractors",
	"238140": "Masonry Contractors",
	"238160": "Roofing Contractors",
	"238210": "Electrical Contractors and Other Wiring Installation Contractors",
	"238220": "Plumbing, Heating, and Air-Conditioning Contractors",
	"238290": "Other Building Equipment Contractors",
	"238310": "Drywall and Insulation Contractors",
	"238320": "Painting and Wall Covering Contractors",
	"238910": "Site Preparation Contractors",
	"238990": "All Other Specialty Trade Contractors",
	"332312": "Fabricated Structural Metal Manufacturing",
	"423610": "Electrical Apparatus and Equipment Merchant Wholesalers",
	"423720": "Plumbing and Heating Equipment Merchant Wholesalers",
	"423730": "Warm Air Heating and Air-Conditioning Equipment Merchant Wholesalers",
	"423840": "Industrial Supplies Merchant Wholesalers",
	"441310": "Automotive Parts and Accessories Retailers",
	"444110": "Home Centers",
	"444180": "Other Building Material Dealers",
	"511210": "Software Publishers",
	"518210": "Computing Infrastructure Providers",
	"522110": "Commercial Banking",
	"523910": "Miscellaneous Intermediation",
	"524210": "Insurance Agencies and Brokerages",
	"531210": "Offices of Real Estate Agents and Brokers",
	"541330": "Engineering Services",
	"541511": "Custom Computer Programming Services",
	"541512": "Computer Systems Design Services",
	"541611": "Administrative Management Consulting Services",
	"541613": "Marketing Consulting Services",
	"541711": "Research and Development in Biotechnology",
	"561110": "Office Administrative Services",
	"561311": "Employment Placement Agencies",
	"561320": "Temporary Help Services",
	"561720": "Janitorial Services",
	"621111": "Offices of Physicians (except Mental Health Specialists)",
	"621210": "Offices of Dentists",
	"722511": "Full-Service Restaurants",
	"722513": "Limited-Service Restaurants",
	"811111": "General Automotive Repair",
	"812111": "Barber Shops",
	"812112": "Beauty Salons",
}

// NAICSTitle returns the official title for a NAICS code.
// Falls back to the 2-digit sector title if the full code isn't found.
func NAICSTitle(code string) string {
	if t, ok := NAICSTitles[code]; ok {
		return t
	}
	// Fall back to 2-digit sector.
	if len(code) >= 2 {
		if t, ok := NAICSTitles[code[:2]]; ok {
			return t
		}
	}
	return ""
}
