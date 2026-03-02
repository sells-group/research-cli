package geo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name       string
		isWithin   bool
		centroidKM float64
		edgeKM     float64
		expected   string
	}{
		{
			name:       "urban_core: within MSA, close to centroid",
			isWithin:   true,
			centroidKM: 5.0,
			edgeKM:     0.0,
			expected:   ClassUrbanCore,
		},
		{
			name:       "urban_core: within MSA, at threshold",
			isWithin:   true,
			centroidKM: 8.0,
			edgeKM:     0.0,
			expected:   ClassUrbanCore,
		},
		{
			name:       "suburban: within MSA, far from centroid",
			isWithin:   true,
			centroidKM: 15.0,
			edgeKM:     0.0,
			expected:   ClassSuburban,
		},
		{
			name:       "suburban: within MSA, barely past threshold",
			isWithin:   true,
			centroidKM: 8.1,
			edgeKM:     0.0,
			expected:   ClassSuburban,
		},
		{
			name:       "exurban: outside MSA, close to edge",
			isWithin:   false,
			centroidKM: 50.0,
			edgeKM:     20.0,
			expected:   ClassExurban,
		},
		{
			name:       "exurban: outside MSA, at edge threshold",
			isWithin:   false,
			centroidKM: 60.0,
			edgeKM:     40.0,
			expected:   ClassExurban,
		},
		{
			name:       "rural: outside MSA, far from edge",
			isWithin:   false,
			centroidKM: 200.0,
			edgeKM:     50.0,
			expected:   ClassRural,
		},
		{
			name:       "rural: outside MSA, barely past threshold",
			isWithin:   false,
			centroidKM: 100.0,
			edgeKM:     40.1,
			expected:   ClassRural,
		},
		{
			name:       "urban_core: within MSA, zero distance",
			isWithin:   true,
			centroidKM: 0.0,
			edgeKM:     0.0,
			expected:   ClassUrbanCore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Classify(tt.isWithin, tt.centroidKM, tt.edgeKM)
			assert.Equal(t, tt.expected, result)
		})
	}
}
