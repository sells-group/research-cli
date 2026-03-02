package analysis

// RegisterSpatial registers all spatial analysis analyzers.
func RegisterSpatial(reg *Registry) {
	reg.Register(&ProximityMatrix{})
}

// RegisterScoring registers all scoring analyzers.
func RegisterScoring(reg *Registry) {
	reg.Register(&ParcelScore{})
}

// RegisterAll registers all analyzer implementations.
func RegisterAll(reg *Registry) {
	RegisterSpatial(reg)
	RegisterScoring(reg)
}
