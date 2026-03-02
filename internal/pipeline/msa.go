package pipeline

import "strings"

// LookupMSA returns the Metropolitan Statistical Area name for a given city and
// state abbreviation. Returns empty string if no MSA match is found.
func LookupMSA(city, state string) string {
	c := strings.ToLower(strings.TrimSpace(city))
	s := strings.ToUpper(strings.TrimSpace(state))
	if c == "" || s == "" {
		return ""
	}

	key := c + "," + strings.ToLower(s)
	if msa, ok := msaIndex[key]; ok {
		return msa
	}
	return ""
}

// msaIndex maps "city,state_abbr" (lowercase) to MSA names for major US metros.
var msaIndex = map[string]string{
	// New York–Newark–Jersey City
	"new york,ny":        "New York-Newark-Jersey City, NY-NJ-PA",
	"newark,nj":          "New York-Newark-Jersey City, NY-NJ-PA",
	"jersey city,nj":     "New York-Newark-Jersey City, NY-NJ-PA",
	"yonkers,ny":         "New York-Newark-Jersey City, NY-NJ-PA",
	"brooklyn,ny":        "New York-Newark-Jersey City, NY-NJ-PA",
	"queens,ny":          "New York-Newark-Jersey City, NY-NJ-PA",
	"bronx,ny":           "New York-Newark-Jersey City, NY-NJ-PA",
	"staten island,ny":   "New York-Newark-Jersey City, NY-NJ-PA",
	"white plains,ny":    "New York-Newark-Jersey City, NY-NJ-PA",
	"stamford,ct":        "New York-Newark-Jersey City, NY-NJ-PA",
	"paterson,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"elizabeth,nj":       "New York-Newark-Jersey City, NY-NJ-PA",
	"morganville,nj":     "New York-Newark-Jersey City, NY-NJ-PA",
	"freehold,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"marlboro,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"holmdel,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"middletown,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"red bank,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"shrewsbury,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"tinton falls,nj":    "New York-Newark-Jersey City, NY-NJ-PA",
	"hazlet,nj":          "New York-Newark-Jersey City, NY-NJ-PA",
	"matawan,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"old bridge,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"woodbridge,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"edison,nj":          "New York-Newark-Jersey City, NY-NJ-PA",
	"new brunswick,nj":   "New York-Newark-Jersey City, NY-NJ-PA",
	"princeton,nj":       "New York-Newark-Jersey City, NY-NJ-PA",
	"hoboken,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"morristown,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"parsippany,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"wayne,nj":           "New York-Newark-Jersey City, NY-NJ-PA",
	"clifton,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"toms river,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"hackensack,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"fort lee,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"paramus,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"montclair,nj":       "New York-Newark-Jersey City, NY-NJ-PA",
	"new rochelle,ny":    "New York-Newark-Jersey City, NY-NJ-PA",
	"mount kisco,ny":     "New York-Newark-Jersey City, NY-NJ-PA",
	"tarrytown,ny":       "New York-Newark-Jersey City, NY-NJ-PA",
	"greenwich,ct":       "New York-Newark-Jersey City, NY-NJ-PA",
	"darien,ct":          "New York-Newark-Jersey City, NY-NJ-PA",
	"westport,ct":        "New York-Newark-Jersey City, NY-NJ-PA",
	"garden city,ny":     "New York-Newark-Jersey City, NY-NJ-PA",
	"great neck,ny":      "New York-Newark-Jersey City, NY-NJ-PA",
	"manhasset,ny":       "New York-Newark-Jersey City, NY-NJ-PA",
	"hempstead,ny":       "New York-Newark-Jersey City, NY-NJ-PA",
	"huntington,ny":      "New York-Newark-Jersey City, NY-NJ-PA",
	"smithtown,ny":       "New York-Newark-Jersey City, NY-NJ-PA",
	"melville,ny":        "New York-Newark-Jersey City, NY-NJ-PA",
	"piscataway,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"somerville,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"bridgewater,nj":     "New York-Newark-Jersey City, NY-NJ-PA",
	"union,nj":           "New York-Newark-Jersey City, NY-NJ-PA",
	"cranford,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"westfield,nj":       "New York-Newark-Jersey City, NY-NJ-PA",
	"summit,nj":          "New York-Newark-Jersey City, NY-NJ-PA",
	"chatham,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"short hills,nj":     "New York-Newark-Jersey City, NY-NJ-PA",
	"livingston,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"florham park,nj":    "New York-Newark-Jersey City, NY-NJ-PA",
	"secaucus,nj":        "New York-Newark-Jersey City, NY-NJ-PA",
	"weehawken,nj":       "New York-Newark-Jersey City, NY-NJ-PA",
	"north bergen,nj":    "New York-Newark-Jersey City, NY-NJ-PA",
	"west new york,nj":   "New York-Newark-Jersey City, NY-NJ-PA",
	"bayonne,nj":         "New York-Newark-Jersey City, NY-NJ-PA",
	"sayreville,nj":      "New York-Newark-Jersey City, NY-NJ-PA",
	"east brunswick,nj":  "New York-Newark-Jersey City, NY-NJ-PA",
	"south brunswick,nj": "New York-Newark-Jersey City, NY-NJ-PA",
	"plainsboro,nj":      "New York-Newark-Jersey City, NY-NJ-PA",

	// Los Angeles–Long Beach–Anaheim
	"los angeles,ca":      "Los Angeles-Long Beach-Anaheim, CA",
	"long beach,ca":       "Los Angeles-Long Beach-Anaheim, CA",
	"anaheim,ca":          "Los Angeles-Long Beach-Anaheim, CA",
	"santa ana,ca":        "Los Angeles-Long Beach-Anaheim, CA",
	"irvine,ca":           "Los Angeles-Long Beach-Anaheim, CA",
	"glendale,ca":         "Los Angeles-Long Beach-Anaheim, CA",
	"huntington beach,ca": "Los Angeles-Long Beach-Anaheim, CA",
	"santa clarita,ca":    "Los Angeles-Long Beach-Anaheim, CA",
	"torrance,ca":         "Los Angeles-Long Beach-Anaheim, CA",
	"pasadena,ca":         "Los Angeles-Long Beach-Anaheim, CA",
	"orange,ca":           "Los Angeles-Long Beach-Anaheim, CA",
	"fullerton,ca":        "Los Angeles-Long Beach-Anaheim, CA",
	"costa mesa,ca":       "Los Angeles-Long Beach-Anaheim, CA",
	"burbank,ca":          "Los Angeles-Long Beach-Anaheim, CA",

	// Chicago–Naperville–Elgin
	"chicago,il":           "Chicago-Naperville-Elgin, IL-IN-WI",
	"naperville,il":        "Chicago-Naperville-Elgin, IL-IN-WI",
	"elgin,il":             "Chicago-Naperville-Elgin, IL-IN-WI",
	"aurora,il":            "Chicago-Naperville-Elgin, IL-IN-WI",
	"joliet,il":            "Chicago-Naperville-Elgin, IL-IN-WI",
	"schaumburg,il":        "Chicago-Naperville-Elgin, IL-IN-WI",
	"evanston,il":          "Chicago-Naperville-Elgin, IL-IN-WI",
	"gary,in":              "Chicago-Naperville-Elgin, IL-IN-WI",
	"oak brook,il":         "Chicago-Naperville-Elgin, IL-IN-WI",
	"downers grove,il":     "Chicago-Naperville-Elgin, IL-IN-WI",
	"wheaton,il":           "Chicago-Naperville-Elgin, IL-IN-WI",
	"oak park,il":          "Chicago-Naperville-Elgin, IL-IN-WI",
	"des plaines,il":       "Chicago-Naperville-Elgin, IL-IN-WI",
	"palatine,il":          "Chicago-Naperville-Elgin, IL-IN-WI",
	"arlington heights,il": "Chicago-Naperville-Elgin, IL-IN-WI",
	"skokie,il":            "Chicago-Naperville-Elgin, IL-IN-WI",
	"orland park,il":       "Chicago-Naperville-Elgin, IL-IN-WI",
	"tinley park,il":       "Chicago-Naperville-Elgin, IL-IN-WI",
	"bolingbrook,il":       "Chicago-Naperville-Elgin, IL-IN-WI",
	"hoffman estates,il":   "Chicago-Naperville-Elgin, IL-IN-WI",

	// Dallas–Fort Worth–Arlington
	"dallas,tx":       "Dallas-Fort Worth-Arlington, TX",
	"fort worth,tx":   "Dallas-Fort Worth-Arlington, TX",
	"arlington,tx":    "Dallas-Fort Worth-Arlington, TX",
	"plano,tx":        "Dallas-Fort Worth-Arlington, TX",
	"irving,tx":       "Dallas-Fort Worth-Arlington, TX",
	"frisco,tx":       "Dallas-Fort Worth-Arlington, TX",
	"mckinney,tx":     "Dallas-Fort Worth-Arlington, TX",
	"denton,tx":       "Dallas-Fort Worth-Arlington, TX",
	"richardson,tx":   "Dallas-Fort Worth-Arlington, TX",
	"allen,tx":        "Dallas-Fort Worth-Arlington, TX",
	"flower mound,tx": "Dallas-Fort Worth-Arlington, TX",
	"southlake,tx":    "Dallas-Fort Worth-Arlington, TX",
	"grapevine,tx":    "Dallas-Fort Worth-Arlington, TX",
	"lewisville,tx":   "Dallas-Fort Worth-Arlington, TX",
	"carrollton,tx":   "Dallas-Fort Worth-Arlington, TX",
	"garland,tx":      "Dallas-Fort Worth-Arlington, TX",
	"mesquite,tx":     "Dallas-Fort Worth-Arlington, TX",
	"rockwall,tx":     "Dallas-Fort Worth-Arlington, TX",
	"prosper,tx":      "Dallas-Fort Worth-Arlington, TX",
	"celina,tx":       "Dallas-Fort Worth-Arlington, TX",
	"coppell,tx":      "Dallas-Fort Worth-Arlington, TX",
	"colleyville,tx":  "Dallas-Fort Worth-Arlington, TX",

	// Houston–The Woodlands–Sugar Land
	"houston,tx":       "Houston-The Woodlands-Sugar Land, TX",
	"the woodlands,tx": "Houston-The Woodlands-Sugar Land, TX",
	"sugar land,tx":    "Houston-The Woodlands-Sugar Land, TX",
	"pasadena,tx":      "Houston-The Woodlands-Sugar Land, TX",
	"pearland,tx":      "Houston-The Woodlands-Sugar Land, TX",
	"league city,tx":   "Houston-The Woodlands-Sugar Land, TX",
	"baytown,tx":       "Houston-The Woodlands-Sugar Land, TX",
	"katy,tx":          "Houston-The Woodlands-Sugar Land, TX",
	"cypress,tx":       "Houston-The Woodlands-Sugar Land, TX",
	"spring,tx":        "Houston-The Woodlands-Sugar Land, TX",
	"humble,tx":        "Houston-The Woodlands-Sugar Land, TX",
	"missouri city,tx": "Houston-The Woodlands-Sugar Land, TX",
	"friendswood,tx":   "Houston-The Woodlands-Sugar Land, TX",
	"richmond,tx":      "Houston-The Woodlands-Sugar Land, TX",
	"conroe,tx":        "Houston-The Woodlands-Sugar Land, TX",
	"tomball,tx":       "Houston-The Woodlands-Sugar Land, TX",

	// Washington–Arlington–Alexandria
	"washington,dc":     "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"arlington,va":      "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"alexandria,va":     "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"fairfax,va":        "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"reston,va":         "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"bethesda,md":       "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"silver spring,md":  "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"rockville,md":      "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"mclean,va":         "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"tysons,va":         "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"herndon,va":        "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"vienna,va":         "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"manassas,va":       "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"leesburg,va":       "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"ashburn,va":        "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"chantilly,va":      "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"gainesville,va":    "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"woodbridge,va":     "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"fredericksburg,va": "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"gaithersburg,md":   "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"germantown,md":     "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"bowie,md":          "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"laurel,md":         "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"college park,md":   "Washington-Arlington-Alexandria, DC-VA-MD-WV",
	"annapolis,md":      "Washington-Arlington-Alexandria, DC-VA-MD-WV",

	// Philadelphia–Camden–Wilmington
	"philadelphia,pa":    "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"camden,nj":          "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"wilmington,de":      "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"cherry hill,nj":     "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"king of prussia,pa": "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"conshohocken,pa":    "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"wayne,pa":           "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"media,pa":           "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"doylestown,pa":      "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"norristown,pa":      "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"west chester,pa":    "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"radnor,pa":          "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"lansdale,pa":        "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"moorestown,nj":      "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"marlton,nj":         "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"mount laurel,nj":    "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
	"voorhees,nj":        "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",

	// Miami–Fort Lauderdale–Pompano Beach
	"miami,fl":              "Miami-Fort Lauderdale-Pompano Beach, FL",
	"fort lauderdale,fl":    "Miami-Fort Lauderdale-Pompano Beach, FL",
	"pompano beach,fl":      "Miami-Fort Lauderdale-Pompano Beach, FL",
	"hialeah,fl":            "Miami-Fort Lauderdale-Pompano Beach, FL",
	"hollywood,fl":          "Miami-Fort Lauderdale-Pompano Beach, FL",
	"coral springs,fl":      "Miami-Fort Lauderdale-Pompano Beach, FL",
	"boca raton,fl":         "Miami-Fort Lauderdale-Pompano Beach, FL",
	"west palm beach,fl":    "Miami-Fort Lauderdale-Pompano Beach, FL",
	"doral,fl":              "Miami-Fort Lauderdale-Pompano Beach, FL",
	"coral gables,fl":       "Miami-Fort Lauderdale-Pompano Beach, FL",
	"aventura,fl":           "Miami-Fort Lauderdale-Pompano Beach, FL",
	"coconut grove,fl":      "Miami-Fort Lauderdale-Pompano Beach, FL",
	"key biscayne,fl":       "Miami-Fort Lauderdale-Pompano Beach, FL",
	"miami beach,fl":        "Miami-Fort Lauderdale-Pompano Beach, FL",
	"sunny isles beach,fl":  "Miami-Fort Lauderdale-Pompano Beach, FL",
	"plantation,fl":         "Miami-Fort Lauderdale-Pompano Beach, FL",
	"weston,fl":             "Miami-Fort Lauderdale-Pompano Beach, FL",
	"davie,fl":              "Miami-Fort Lauderdale-Pompano Beach, FL",
	"pembroke pines,fl":     "Miami-Fort Lauderdale-Pompano Beach, FL",
	"miramar,fl":            "Miami-Fort Lauderdale-Pompano Beach, FL",
	"delray beach,fl":       "Miami-Fort Lauderdale-Pompano Beach, FL",
	"boynton beach,fl":      "Miami-Fort Lauderdale-Pompano Beach, FL",
	"jupiter,fl":            "Miami-Fort Lauderdale-Pompano Beach, FL",
	"palm beach gardens,fl": "Miami-Fort Lauderdale-Pompano Beach, FL",

	// Atlanta–Sandy Springs–Alpharetta
	"atlanta,ga":           "Atlanta-Sandy Springs-Alpharetta, GA",
	"sandy springs,ga":     "Atlanta-Sandy Springs-Alpharetta, GA",
	"alpharetta,ga":        "Atlanta-Sandy Springs-Alpharetta, GA",
	"roswell,ga":           "Atlanta-Sandy Springs-Alpharetta, GA",
	"marietta,ga":          "Atlanta-Sandy Springs-Alpharetta, GA",
	"johns creek,ga":       "Atlanta-Sandy Springs-Alpharetta, GA",
	"decatur,ga":           "Atlanta-Sandy Springs-Alpharetta, GA",
	"kennesaw,ga":          "Atlanta-Sandy Springs-Alpharetta, GA",
	"duluth,ga":            "Atlanta-Sandy Springs-Alpharetta, GA",
	"lawrenceville,ga":     "Atlanta-Sandy Springs-Alpharetta, GA",
	"suwanee,ga":           "Atlanta-Sandy Springs-Alpharetta, GA",
	"peachtree city,ga":    "Atlanta-Sandy Springs-Alpharetta, GA",
	"smyrna,ga":            "Atlanta-Sandy Springs-Alpharetta, GA",
	"dunwoody,ga":          "Atlanta-Sandy Springs-Alpharetta, GA",
	"brookhaven,ga":        "Atlanta-Sandy Springs-Alpharetta, GA",
	"cumming,ga":           "Atlanta-Sandy Springs-Alpharetta, GA",
	"woodstock,ga":         "Atlanta-Sandy Springs-Alpharetta, GA",
	"norcross,ga":          "Atlanta-Sandy Springs-Alpharetta, GA",
	"peachtree corners,ga": "Atlanta-Sandy Springs-Alpharetta, GA",

	// Boston–Cambridge–Newton
	"boston,ma":     "Boston-Cambridge-Newton, MA-NH",
	"cambridge,ma":  "Boston-Cambridge-Newton, MA-NH",
	"newton,ma":     "Boston-Cambridge-Newton, MA-NH",
	"quincy,ma":     "Boston-Cambridge-Newton, MA-NH",
	"somerville,ma": "Boston-Cambridge-Newton, MA-NH",
	"waltham,ma":    "Boston-Cambridge-Newton, MA-NH",
	"nashua,nh":     "Boston-Cambridge-Newton, MA-NH",
	"brookline,ma":  "Boston-Cambridge-Newton, MA-NH",
	"framingham,ma": "Boston-Cambridge-Newton, MA-NH",
	"needham,ma":    "Boston-Cambridge-Newton, MA-NH",
	"wellesley,ma":  "Boston-Cambridge-Newton, MA-NH",
	"lexington,ma":  "Boston-Cambridge-Newton, MA-NH",
	"burlington,ma": "Boston-Cambridge-Newton, MA-NH",
	"woburn,ma":     "Boston-Cambridge-Newton, MA-NH",
	"braintree,ma":  "Boston-Cambridge-Newton, MA-NH",
	"plymouth,ma":   "Boston-Cambridge-Newton, MA-NH",
	"hingham,ma":    "Boston-Cambridge-Newton, MA-NH",
	"norwood,ma":    "Boston-Cambridge-Newton, MA-NH",
	"dedham,ma":     "Boston-Cambridge-Newton, MA-NH",
	"manchester,nh": "Boston-Cambridge-Newton, MA-NH",

	// Phoenix–Mesa–Chandler
	"phoenix,az":    "Phoenix-Mesa-Chandler, AZ",
	"mesa,az":       "Phoenix-Mesa-Chandler, AZ",
	"chandler,az":   "Phoenix-Mesa-Chandler, AZ",
	"scottsdale,az": "Phoenix-Mesa-Chandler, AZ",
	"tempe,az":      "Phoenix-Mesa-Chandler, AZ",
	"gilbert,az":    "Phoenix-Mesa-Chandler, AZ",
	"glendale,az":   "Phoenix-Mesa-Chandler, AZ",
	"peoria,az":     "Phoenix-Mesa-Chandler, AZ",

	// San Francisco–Oakland–Berkeley
	"san francisco,ca": "San Francisco-Oakland-Berkeley, CA",
	"oakland,ca":       "San Francisco-Oakland-Berkeley, CA",
	"berkeley,ca":      "San Francisco-Oakland-Berkeley, CA",
	"fremont,ca":       "San Francisco-Oakland-Berkeley, CA",
	"hayward,ca":       "San Francisco-Oakland-Berkeley, CA",
	"san mateo,ca":     "San Francisco-Oakland-Berkeley, CA",
	"daly city,ca":     "San Francisco-Oakland-Berkeley, CA",
	"richmond,ca":      "San Francisco-Oakland-Berkeley, CA",
	"san leandro,ca":   "San Francisco-Oakland-Berkeley, CA",
	"walnut creek,ca":  "San Francisco-Oakland-Berkeley, CA",
	"concord,ca":       "San Francisco-Oakland-Berkeley, CA",
	"pleasanton,ca":    "San Francisco-Oakland-Berkeley, CA",
	"livermore,ca":     "San Francisco-Oakland-Berkeley, CA",
	"dublin,ca":        "San Francisco-Oakland-Berkeley, CA",
	"san ramon,ca":     "San Francisco-Oakland-Berkeley, CA",
	"alameda,ca":       "San Francisco-Oakland-Berkeley, CA",
	"redwood city,ca":  "San Francisco-Oakland-Berkeley, CA",
	"foster city,ca":   "San Francisco-Oakland-Berkeley, CA",
	"burlingame,ca":    "San Francisco-Oakland-Berkeley, CA",
	"san rafael,ca":    "San Francisco-Oakland-Berkeley, CA",
	"mill valley,ca":   "San Francisco-Oakland-Berkeley, CA",
	"sausalito,ca":     "San Francisco-Oakland-Berkeley, CA",
	"danville,ca":      "San Francisco-Oakland-Berkeley, CA",
	"lafayette,ca":     "San Francisco-Oakland-Berkeley, CA",
	"orinda,ca":        "San Francisco-Oakland-Berkeley, CA",

	// Riverside–San Bernardino–Ontario
	"riverside,ca":        "Riverside-San Bernardino-Ontario, CA",
	"san bernardino,ca":   "Riverside-San Bernardino-Ontario, CA",
	"ontario,ca":          "Riverside-San Bernardino-Ontario, CA",
	"rancho cucamonga,ca": "Riverside-San Bernardino-Ontario, CA",
	"fontana,ca":          "Riverside-San Bernardino-Ontario, CA",
	"moreno valley,ca":    "Riverside-San Bernardino-Ontario, CA",
	"corona,ca":           "Riverside-San Bernardino-Ontario, CA",
	"temecula,ca":         "Riverside-San Bernardino-Ontario, CA",

	// Detroit–Warren–Dearborn
	"detroit,mi":          "Detroit-Warren-Dearborn, MI",
	"warren,mi":           "Detroit-Warren-Dearborn, MI",
	"dearborn,mi":         "Detroit-Warren-Dearborn, MI",
	"sterling heights,mi": "Detroit-Warren-Dearborn, MI",
	"troy,mi":             "Detroit-Warren-Dearborn, MI",
	"livonia,mi":          "Detroit-Warren-Dearborn, MI",
	"ann arbor,mi":        "Detroit-Warren-Dearborn, MI",

	// Seattle–Tacoma–Bellevue
	"seattle,wa":       "Seattle-Tacoma-Bellevue, WA",
	"tacoma,wa":        "Seattle-Tacoma-Bellevue, WA",
	"bellevue,wa":      "Seattle-Tacoma-Bellevue, WA",
	"kent,wa":          "Seattle-Tacoma-Bellevue, WA",
	"renton,wa":        "Seattle-Tacoma-Bellevue, WA",
	"redmond,wa":       "Seattle-Tacoma-Bellevue, WA",
	"kirkland,wa":      "Seattle-Tacoma-Bellevue, WA",
	"issaquah,wa":      "Seattle-Tacoma-Bellevue, WA",
	"sammamish,wa":     "Seattle-Tacoma-Bellevue, WA",
	"bothell,wa":       "Seattle-Tacoma-Bellevue, WA",
	"woodinville,wa":   "Seattle-Tacoma-Bellevue, WA",
	"mercer island,wa": "Seattle-Tacoma-Bellevue, WA",
	"lynnwood,wa":      "Seattle-Tacoma-Bellevue, WA",
	"everett,wa":       "Seattle-Tacoma-Bellevue, WA",
	"federal way,wa":   "Seattle-Tacoma-Bellevue, WA",
	"burien,wa":        "Seattle-Tacoma-Bellevue, WA",

	// Minneapolis–St. Paul–Bloomington
	"minneapolis,mn":  "Minneapolis-St. Paul-Bloomington, MN-WI",
	"st. paul,mn":     "Minneapolis-St. Paul-Bloomington, MN-WI",
	"saint paul,mn":   "Minneapolis-St. Paul-Bloomington, MN-WI",
	"bloomington,mn":  "Minneapolis-St. Paul-Bloomington, MN-WI",
	"plymouth,mn":     "Minneapolis-St. Paul-Bloomington, MN-WI",
	"eden prairie,mn": "Minneapolis-St. Paul-Bloomington, MN-WI",

	// San Diego–Chula Vista–Carlsbad
	"san diego,ca":   "San Diego-Chula Vista-Carlsbad, CA",
	"chula vista,ca": "San Diego-Chula Vista-Carlsbad, CA",
	"carlsbad,ca":    "San Diego-Chula Vista-Carlsbad, CA",
	"oceanside,ca":   "San Diego-Chula Vista-Carlsbad, CA",
	"escondido,ca":   "San Diego-Chula Vista-Carlsbad, CA",
	"el cajon,ca":    "San Diego-Chula Vista-Carlsbad, CA",

	// Tampa–St. Petersburg–Clearwater
	"tampa,fl":            "Tampa-St. Petersburg-Clearwater, FL",
	"st. petersburg,fl":   "Tampa-St. Petersburg-Clearwater, FL",
	"saint petersburg,fl": "Tampa-St. Petersburg-Clearwater, FL",
	"clearwater,fl":       "Tampa-St. Petersburg-Clearwater, FL",
	"brandon,fl":          "Tampa-St. Petersburg-Clearwater, FL",
	"lakeland,fl":         "Tampa-St. Petersburg-Clearwater, FL",

	// Denver–Aurora–Lakewood
	"denver,co":            "Denver-Aurora-Lakewood, CO",
	"aurora,co":            "Denver-Aurora-Lakewood, CO",
	"lakewood,co":          "Denver-Aurora-Lakewood, CO",
	"thornton,co":          "Denver-Aurora-Lakewood, CO",
	"arvada,co":            "Denver-Aurora-Lakewood, CO",
	"centennial,co":        "Denver-Aurora-Lakewood, CO",
	"boulder,co":           "Denver-Aurora-Lakewood, CO",
	"westminster,co":       "Denver-Aurora-Lakewood, CO",
	"broomfield,co":        "Denver-Aurora-Lakewood, CO",
	"littleton,co":         "Denver-Aurora-Lakewood, CO",
	"highlands ranch,co":   "Denver-Aurora-Lakewood, CO",
	"parker,co":            "Denver-Aurora-Lakewood, CO",
	"castle rock,co":       "Denver-Aurora-Lakewood, CO",
	"lone tree,co":         "Denver-Aurora-Lakewood, CO",
	"greenwood village,co": "Denver-Aurora-Lakewood, CO",
	"englewood,co":         "Denver-Aurora-Lakewood, CO",
	"golden,co":            "Denver-Aurora-Lakewood, CO",
	"louisville,co":        "Denver-Aurora-Lakewood, CO",
	"longmont,co":          "Denver-Aurora-Lakewood, CO",
	"erie,co":              "Denver-Aurora-Lakewood, CO",

	// St. Louis
	"st. louis,mo":   "St. Louis, MO-IL",
	"saint louis,mo": "St. Louis, MO-IL",

	// Baltimore–Columbia–Towson
	"baltimore,md": "Baltimore-Columbia-Towson, MD",
	"columbia,md":  "Baltimore-Columbia-Towson, MD",
	"towson,md":    "Baltimore-Columbia-Towson, MD",

	// Orlando–Kissimmee–Sanford
	"orlando,fl":   "Orlando-Kissimmee-Sanford, FL",
	"kissimmee,fl": "Orlando-Kissimmee-Sanford, FL",
	"sanford,fl":   "Orlando-Kissimmee-Sanford, FL",

	// Charlotte–Concord–Gastonia
	"charlotte,nc": "Charlotte-Concord-Gastonia, NC-SC",
	"concord,nc":   "Charlotte-Concord-Gastonia, NC-SC",
	"gastonia,nc":  "Charlotte-Concord-Gastonia, NC-SC",

	// San Antonio–New Braunfels
	"san antonio,tx":   "San Antonio-New Braunfels, TX",
	"new braunfels,tx": "San Antonio-New Braunfels, TX",

	// Portland–Vancouver–Hillsboro
	"portland,or":  "Portland-Vancouver-Hillsboro, OR-WA",
	"vancouver,wa": "Portland-Vancouver-Hillsboro, OR-WA",
	"hillsboro,or": "Portland-Vancouver-Hillsboro, OR-WA",
	"beaverton,or": "Portland-Vancouver-Hillsboro, OR-WA",

	// Sacramento–Roseville–Folsom
	"sacramento,ca": "Sacramento-Roseville-Folsom, CA",
	"roseville,ca":  "Sacramento-Roseville-Folsom, CA",
	"folsom,ca":     "Sacramento-Roseville-Folsom, CA",
	"elk grove,ca":  "Sacramento-Roseville-Folsom, CA",

	// Pittsburgh
	"pittsburgh,pa": "Pittsburgh, PA",

	// Austin–Round Rock–Georgetown
	"austin,tx":     "Austin-Round Rock-Georgetown, TX",
	"round rock,tx": "Austin-Round Rock-Georgetown, TX",
	"georgetown,tx": "Austin-Round Rock-Georgetown, TX",
	"cedar park,tx": "Austin-Round Rock-Georgetown, TX",

	// Las Vegas–Henderson–Paradise
	"las vegas,nv":       "Las Vegas-Henderson-Paradise, NV",
	"henderson,nv":       "Las Vegas-Henderson-Paradise, NV",
	"north las vegas,nv": "Las Vegas-Henderson-Paradise, NV",

	// Cincinnati
	"cincinnati,oh": "Cincinnati, OH-KY-IN",

	// Kansas City
	"kansas city,mo":   "Kansas City, MO-KS",
	"kansas city,ks":   "Kansas City, MO-KS",
	"overland park,ks": "Kansas City, MO-KS",

	// Columbus
	"columbus,oh": "Columbus, OH",

	// Indianapolis–Carmel–Anderson
	"indianapolis,in": "Indianapolis-Carmel-Anderson, IN",
	"carmel,in":       "Indianapolis-Carmel-Anderson, IN",

	// Cleveland–Elyria
	"cleveland,oh": "Cleveland-Elyria, OH",

	// San Jose–Sunnyvale–Santa Clara
	"san jose,ca":       "San Jose-Sunnyvale-Santa Clara, CA",
	"sunnyvale,ca":      "San Jose-Sunnyvale-Santa Clara, CA",
	"santa clara,ca":    "San Jose-Sunnyvale-Santa Clara, CA",
	"mountain view,ca":  "San Jose-Sunnyvale-Santa Clara, CA",
	"palo alto,ca":      "San Jose-Sunnyvale-Santa Clara, CA",
	"cupertino,ca":      "San Jose-Sunnyvale-Santa Clara, CA",
	"milpitas,ca":       "San Jose-Sunnyvale-Santa Clara, CA",
	"campbell,ca":       "San Jose-Sunnyvale-Santa Clara, CA",
	"los gatos,ca":      "San Jose-Sunnyvale-Santa Clara, CA",
	"saratoga,ca":       "San Jose-Sunnyvale-Santa Clara, CA",
	"morgan hill,ca":    "San Jose-Sunnyvale-Santa Clara, CA",
	"gilroy,ca":         "San Jose-Sunnyvale-Santa Clara, CA",
	"menlo park,ca":     "San Jose-Sunnyvale-Santa Clara, CA",
	"atherton,ca":       "San Jose-Sunnyvale-Santa Clara, CA",
	"los altos,ca":      "San Jose-Sunnyvale-Santa Clara, CA",
	"woodside,ca":       "San Jose-Sunnyvale-Santa Clara, CA",
	"portola valley,ca": "San Jose-Sunnyvale-Santa Clara, CA",

	// Nashville–Davidson–Murfreesboro–Franklin
	"nashville,tn":    "Nashville-Davidson-Murfreesboro-Franklin, TN",
	"murfreesboro,tn": "Nashville-Davidson-Murfreesboro-Franklin, TN",
	"franklin,tn":     "Nashville-Davidson-Murfreesboro-Franklin, TN",

	// Virginia Beach–Norfolk–Newport News
	"virginia beach,va": "Virginia Beach-Norfolk-Newport News, VA-NC",
	"norfolk,va":        "Virginia Beach-Norfolk-Newport News, VA-NC",
	"newport news,va":   "Virginia Beach-Norfolk-Newport News, VA-NC",
	"chesapeake,va":     "Virginia Beach-Norfolk-Newport News, VA-NC",
	"hampton,va":        "Virginia Beach-Norfolk-Newport News, VA-NC",

	// Providence–Warwick
	"providence,ri": "Providence-Warwick, RI-MA",
	"warwick,ri":    "Providence-Warwick, RI-MA",
	"cranston,ri":   "Providence-Warwick, RI-MA",

	// Milwaukee–Waukesha
	"milwaukee,wi": "Milwaukee-Waukesha, WI",
	"waukesha,wi":  "Milwaukee-Waukesha, WI",

	// Jacksonville
	"jacksonville,fl": "Jacksonville, FL",

	// Memphis
	"memphis,tn": "Memphis, TN-MS-AR",

	// Oklahoma City
	"oklahoma city,ok": "Oklahoma City, OK",

	// Raleigh–Cary
	"raleigh,nc": "Raleigh-Cary, NC",
	"cary,nc":    "Raleigh-Cary, NC",
	"durham,nc":  "Raleigh-Cary, NC",

	// Richmond
	"richmond,va": "Richmond, VA",

	// Louisville/Jefferson County
	"louisville,ky": "Louisville/Jefferson County, KY-IN",

	// New Orleans–Metairie
	"new orleans,la": "New Orleans-Metairie, LA",
	"metairie,la":    "New Orleans-Metairie, LA",

	// Salt Lake City
	"salt lake city,ut":   "Salt Lake City, UT",
	"west jordan,ut":      "Salt Lake City, UT",
	"provo,ut":            "Salt Lake City, UT",
	"sandy,ut":            "Salt Lake City, UT",
	"west valley city,ut": "Salt Lake City, UT",
	"orem,ut":             "Salt Lake City, UT",
	"draper,ut":           "Salt Lake City, UT",
	"lehi,ut":             "Salt Lake City, UT",
	"ogden,ut":            "Salt Lake City, UT",

	// Hartford–East Hartford–Middletown
	"hartford,ct":      "Hartford-East Hartford-Middletown, CT",
	"east hartford,ct": "Hartford-East Hartford-Middletown, CT",

	// Birmingham–Hoover
	"birmingham,al": "Birmingham-Hoover, AL",
	"hoover,al":     "Birmingham-Hoover, AL",

	// Buffalo–Cheektowaga
	"buffalo,ny": "Buffalo-Cheektowaga, NY",

	// Rochester
	"rochester,ny": "Rochester, NY",

	// Grand Rapids–Kentwood
	"grand rapids,mi": "Grand Rapids-Kentwood, MI",

	// Tucson
	"tucson,az": "Tucson, AZ",

	// Tulsa
	"tulsa,ok": "Tulsa, OK",

	// Honolulu
	"honolulu,hi": "Urban Honolulu, HI",

	// Omaha–Council Bluffs
	"omaha,ne": "Omaha-Council Bluffs, NE-IA",

	// Albuquerque
	"albuquerque,nm": "Albuquerque, NM",

	// Boise City
	"boise,id": "Boise City, ID",

	// El Paso
	"el paso,tx": "El Paso, TX",

	// Knoxville
	"knoxville,tn": "Knoxville, TN",

	// Charleston–North Charleston
	"charleston,sc":       "Charleston-North Charleston, SC",
	"north charleston,sc": "Charleston-North Charleston, SC",

	// Greenville–Anderson
	"greenville,sc": "Greenville-Anderson, SC",

	// Des Moines–West Des Moines
	"des moines,ia":      "Des Moines-West Des Moines, IA",
	"west des moines,ia": "Des Moines-West Des Moines, IA",

	// Little Rock–North Little Rock–Conway
	"little rock,ar": "Little Rock-North Little Rock-Conway, AR",

	// Bridgeport–Stamford–Norwalk
	"bridgeport,ct": "Bridgeport-Stamford-Norwalk, CT",
	"norwalk,ct":    "Bridgeport-Stamford-Norwalk, CT",
}
