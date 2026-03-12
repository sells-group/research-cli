package scraper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEpaURL_Default(t *testing.T) {
	got := epaURL("")
	assert.Equal(t, epaBaseURL, got)
}

func TestEpaURL_Override(t *testing.T) {
	got := epaURL("http://test")
	assert.Equal(t, "http://test", got)
}
