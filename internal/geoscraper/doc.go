// Package geoscraper provides a framework for ingesting geospatial data from
// national and state-level sources (HIFLD, FEMA, EPA, TIGER, Census, FCC, NRCS, OSM).
// It mirrors the fedsync Dataset/Engine/Registry pattern, populating geo.* tables
// and enqueuing addresses for geocoding via PostSync hooks.
package geoscraper
