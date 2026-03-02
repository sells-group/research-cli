package analysis

// RegisterSpatial registers all spatial analysis analyzers.
func RegisterSpatial(reg *Registry) {
	reg.Register(&ProximityMatrix{})
}

// RegisterAll registers all analyzer implementations.
func RegisterAll(reg *Registry) {
	RegisterSpatial(reg)
}
