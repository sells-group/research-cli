// Package analysis provides a framework for batch geospatial analysis:
// proximity matrix, parcel scoring, owner analysis, cross-source correlation,
// opportunity ranking, and export.
//
// It follows the same interface/registry/engine pattern used by fedsync and
// geoscraper, with the addition of dependency-aware topological ordering
// for parallel execution.
package analysis
