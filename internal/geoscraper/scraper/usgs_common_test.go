package scraper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUsgsURL_Default(t *testing.T) {
	got := usgsURL("", "FOO")
	assert.Equal(t, "FOO", got)
}

func TestUsgsURL_Override(t *testing.T) {
	got := usgsURL("http://test", "FOO")
	assert.Equal(t, "http://test", got)
}
