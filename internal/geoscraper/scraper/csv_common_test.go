package scraper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCsvColIndex(t *testing.T) {
	idx := csvColIndex([]string{"  Name ", "Lat", "Lon"})
	assert.Equal(t, 0, idx["Name"])
	assert.Equal(t, 1, idx["Lat"])
	assert.Equal(t, 2, idx["Lon"])
}

func TestCsvString(t *testing.T) {
	row := []string{" hello ", "world"}
	assert.Equal(t, "hello", csvString(row, 0))
	assert.Equal(t, "world", csvString(row, 1))
	assert.Equal(t, "", csvString(row, -1))
	assert.Equal(t, "", csvString(row, 5))
}

func TestCsvFloat64(t *testing.T) {
	row := []string{"3.14", "", "abc", "42"}
	assert.InDelta(t, 3.14, csvFloat64(row, 0), 0.001)
	assert.Equal(t, 0.0, csvFloat64(row, 1))
	assert.Equal(t, 0.0, csvFloat64(row, 2))
	assert.InDelta(t, 42.0, csvFloat64(row, 3), 0.001)
	assert.Equal(t, 0.0, csvFloat64(row, -1))
}

func TestCsvProperties(t *testing.T) {
	header := []string{"Name", "Lat", "Extra"}
	row := []string{"Foo", "30.0", "bar"}
	exclude := map[string]bool{"Name": true, "Lat": true}

	props := csvProperties(row, header, exclude)
	assert.Contains(t, string(props), `"Extra":"bar"`)
	assert.NotContains(t, string(props), `"Name"`)

	// Empty values omitted.
	row2 := []string{"Foo", "30.0", ""}
	props2 := csvProperties(row2, header, exclude)
	assert.NotContains(t, string(props2), `"Extra"`)

	// Row shorter than header.
	row3 := []string{"Foo"}
	props3 := csvProperties(row3, header, exclude)
	assert.Equal(t, "{}", string(props3))
}
