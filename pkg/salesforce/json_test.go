package salesforce

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeJSON_Success(t *testing.T) {
	body := `{"name":"Account","label":"Account","fields":[{"name":"Id","label":"Account ID","type":"id","length":18,"updateable":false}]}`
	reader := strings.NewReader(body)

	var desc SObjectDescription
	err := decodeJSON(reader, &desc)
	require.NoError(t, err)
	assert.Equal(t, "Account", desc.Name)
	assert.Equal(t, "Account", desc.Label)
	require.Len(t, desc.Fields, 1)
	assert.Equal(t, "Id", desc.Fields[0].Name)
	assert.Equal(t, "Account ID", desc.Fields[0].Label)
	assert.Equal(t, "id", desc.Fields[0].Type)
	assert.Equal(t, 18, desc.Fields[0].Length)
	assert.False(t, desc.Fields[0].Updateable)
}

func TestDecodeJSON_InvalidJSON(t *testing.T) {
	body := `{invalid json`
	reader := strings.NewReader(body)

	var desc SObjectDescription
	err := decodeJSON(reader, &desc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode json")
}

func TestDecodeJSON_EmptyBody(t *testing.T) {
	reader := strings.NewReader("")

	var desc SObjectDescription
	err := decodeJSON(reader, &desc)
	assert.Error(t, err)
}

func TestDecodeJSON_EmptyObject(t *testing.T) {
	reader := strings.NewReader("{}")

	var desc SObjectDescription
	err := decodeJSON(reader, &desc)
	require.NoError(t, err)
	assert.Equal(t, "", desc.Name)
	assert.Nil(t, desc.Fields)
}

func TestDecodeJSON_IntoSlice(t *testing.T) {
	body := `[{"Id":"001xx","Name":"Acme"},{"Id":"002xx","Name":"Beta"}]`
	reader := strings.NewReader(body)

	var accounts []Account
	err := decodeJSON(reader, &accounts)
	require.NoError(t, err)
	require.Len(t, accounts, 2)
	assert.Equal(t, "001xx", accounts[0].ID)
	assert.Equal(t, "Beta", accounts[1].Name)
}

func TestDecodeJSON_IntoMap(t *testing.T) {
	body := `{"key":"value","num":42}`
	reader := strings.NewReader(body)

	var result map[string]any
	err := decodeJSON(reader, &result)
	require.NoError(t, err)
	assert.Equal(t, "value", result["key"])
	assert.Equal(t, float64(42), result["num"])
}
