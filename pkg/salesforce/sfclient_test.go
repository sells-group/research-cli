package salesforce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	gosf "github.com/k-capehart/go-salesforce/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestSFClient creates an sfClient backed by an httptest server.
func newTestSFClient(t *testing.T, handler http.Handler) (Client, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(handler)

	sf, err := gosf.Init(gosf.Creds{
		AccessToken: "test-token",
		Domain:      ts.URL,
	},
		gosf.WithValidateAuthentication(false),
		gosf.WithRoundTripper(http.DefaultTransport),
	)
	require.NoError(t, err)
	require.NotNil(t, sf)

	return NewClient(sf), ts
}

func TestSFClient_Query(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/query")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"totalSize": 1,
			"done":      true,
			"records": []map[string]any{
				{
					"attributes": map[string]any{"type": "Account"},
					"Id":         "001xx",
					"Name":       "Acme Corp",
					"Website":    "acme.com",
				},
			},
		})
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	var accounts []Account
	err := client.Query(context.Background(), "SELECT Id, Name FROM Account", &accounts)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.Equal(t, "001xx", accounts[0].ID)
	assert.Equal(t, "Acme Corp", accounts[0].Name)
}

func TestSFClient_Query_Error(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"message": "invalid SOQL", "errorCode": "MALFORMED_QUERY"},
		})
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	var accounts []Account
	err := client.Query(context.Background(), "INVALID SOQL", &accounts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf: query")
}

func TestSFClient_InsertOne(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path != "/query" {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "001new",
				"success": true,
				"errors":  []any{},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	id, err := client.InsertOne(context.Background(), "Account", map[string]any{
		"Name": "New Corp",
	})
	require.NoError(t, err)
	assert.Equal(t, "001new", id)
}

func TestSFClient_InsertOne_Failure(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      "",
				"success": false,
				"errors":  []map[string]any{{"message": "required field missing"}},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	_, err := client.InsertOne(context.Background(), "Account", map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insert Account failed")
}

func TestSFClient_UpdateOne(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	err := client.UpdateOne(context.Background(), "Account", "001xx", map[string]any{
		"Industry": "Technology",
	})
	require.NoError(t, err)
}

func TestSFClient_UpdateOne_Error(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"message": "invalid field", "errorCode": "INVALID_FIELD"},
		})
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	err := client.UpdateOne(context.Background(), "Account", "001xx", map[string]any{
		"BadField": "value",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf: update")
}

func TestSFClient_UpdateCollection(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": "001xx", "success": true, "errors": []any{}},
				{"id": "002xx", "success": true, "errors": []any{}},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	records := []CollectionRecord{
		{ID: "001xx", Fields: map[string]any{"Name": "A"}},
		{ID: "002xx", Fields: map[string]any{"Name": "B"}},
	}
	results, err := client.UpdateCollection(context.Background(), "Account", records)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.True(t, results[0].Success)
	assert.Equal(t, "001xx", results[0].ID)
}

func TestSFClient_UpdateCollection_Error(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"message": "batch error"},
		})
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	records := []CollectionRecord{
		{ID: "001xx", Fields: map[string]any{"Name": "A"}},
	}
	_, err := client.UpdateCollection(context.Background(), "Account", records)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf: update collection")
}

func TestSFClient_DescribeSObject(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// go-salesforce constructs URL as: InstanceUrl + /services/data/vXX.X + uri
		assert.Contains(t, r.URL.Path, "/sobjects/Account/describe")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":  "Account",
			"label": "Account",
			"fields": []map[string]any{
				{"name": "Id", "label": "Account ID", "type": "id", "length": 18, "updateable": false},
				{"name": "Name", "label": "Account Name", "type": "string", "length": 255, "updateable": true},
			},
		})
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	desc, err := client.DescribeSObject(context.Background(), "Account")
	require.NoError(t, err)
	require.NotNil(t, desc)
	assert.Equal(t, "Account", desc.Name)
	assert.Equal(t, "Account", desc.Label)
	require.Len(t, desc.Fields, 2)
	assert.Equal(t, "Id", desc.Fields[0].Name)
	assert.False(t, desc.Fields[0].Updateable)
	assert.Equal(t, "Name", desc.Fields[1].Name)
	assert.True(t, desc.Fields[1].Updateable)
}

func TestSFClient_DescribeSObject_Error(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"message": "sobject not found", "errorCode": "NOT_FOUND"},
		})
	})

	client, ts := newTestSFClient(t, handler)
	defer ts.Close()

	_, err := client.DescribeSObject(context.Background(), "NonExistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sf: describe")
}
