package salesforce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAccount(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedObject string
		var capturedFields map[string]any
		mc := &mockClient{
			insertOneFn: func(_ context.Context, sObject string, record map[string]any) (string, error) {
				capturedObject = sObject
				capturedFields = record
				return "001NEW", nil
			},
		}

		fields := map[string]any{"Name": "Acme Corp", "Website": "https://acme.com"}
		id, err := CreateAccount(context.Background(), mc, fields)
		require.NoError(t, err)
		assert.Equal(t, "001NEW", id)
		assert.Equal(t, "Account", capturedObject)
		assert.Equal(t, "Acme Corp", capturedFields["Name"])
		assert.Equal(t, "https://acme.com", capturedFields["Website"])
	})

	t.Run("missing name", func(t *testing.T) {
		mc := &mockClient{}
		_, err := CreateAccount(context.Background(), mc, map[string]any{"Website": "https://acme.com"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Name is required")
	})

	t.Run("empty name", func(t *testing.T) {
		mc := &mockClient{}
		_, err := CreateAccount(context.Background(), mc, map[string]any{"Name": ""})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Name is required")
	})

	t.Run("propagates error", func(t *testing.T) {
		mc := &mockClient{
			insertOneFn: func(_ context.Context, _ string, _ map[string]any) (string, error) {
				return "", errors.New("api error")
			},
		}
		_, err := CreateAccount(context.Background(), mc, map[string]any{"Name": "Test"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "create account")
	})
}

func TestCreateContact(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedObject string
		var capturedFields map[string]any
		mc := &mockClient{
			insertOneFn: func(_ context.Context, sObject string, record map[string]any) (string, error) {
				capturedObject = sObject
				capturedFields = record
				return "003NEW", nil
			},
		}

		fields := map[string]any{"LastName": "Doe", "FirstName": "Jane"}
		id, err := CreateContact(context.Background(), mc, "001ACCT", fields)
		require.NoError(t, err)
		assert.Equal(t, "003NEW", id)
		assert.Equal(t, "Contact", capturedObject)
		assert.Equal(t, "001ACCT", capturedFields["AccountId"])
		assert.Equal(t, "Doe", capturedFields["LastName"])
	})

	t.Run("empty account id", func(t *testing.T) {
		mc := &mockClient{}
		_, err := CreateContact(context.Background(), mc, "", map[string]any{"LastName": "Doe"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "account id is required")
	})

	t.Run("propagates error", func(t *testing.T) {
		mc := &mockClient{
			insertOneFn: func(_ context.Context, _ string, _ map[string]any) (string, error) {
				return "", errors.New("api error")
			},
		}
		_, err := CreateContact(context.Background(), mc, "001ACCT", map[string]any{"LastName": "Doe"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "create contact")
	})
}

func TestUpdateAccount(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedID string
		var capturedFields map[string]any
		mock := &mockClient{
			updateOneFn: func(_ context.Context, sObject string, id string, fields map[string]any) error {
				assert.Equal(t, "Account", sObject)
				capturedID = id
				capturedFields = fields
				return nil
			},
		}

		fields := map[string]any{"Industry": "Technology", "Phone": "555-1234"}
		err := UpdateAccount(context.Background(), mock, "001xx", fields)
		require.NoError(t, err)
		assert.Equal(t, "001xx", capturedID)
		assert.Equal(t, "Technology", capturedFields["Industry"])
	})

	t.Run("empty id", func(t *testing.T) {
		mock := &mockClient{}
		err := UpdateAccount(context.Background(), mock, "", map[string]any{"Name": "test"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "account id is required")
	})

	t.Run("empty fields", func(t *testing.T) {
		mock := &mockClient{}
		err := UpdateAccount(context.Background(), mock, "001xx", map[string]any{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no fields to update")
	})

	t.Run("propagates error", func(t *testing.T) {
		mock := &mockClient{
			updateOneFn: func(_ context.Context, _ string, _ string, _ map[string]any) error {
				return errors.New("unauthorized")
			},
		}

		err := UpdateAccount(context.Background(), mock, "001xx", map[string]any{"Name": "test"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "update account")
	})
}

func TestUpdateContact(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var capturedID string
		var capturedFields map[string]any
		mock := &mockClient{
			updateOneFn: func(_ context.Context, sObject string, id string, fields map[string]any) error {
				assert.Equal(t, "Contact", sObject)
				capturedID = id
				capturedFields = fields
				return nil
			},
		}

		fields := map[string]any{"Title": "CEO", "Phone": "555-1234"}
		err := UpdateContact(context.Background(), mock, "003xx", fields)
		require.NoError(t, err)
		assert.Equal(t, "003xx", capturedID)
		assert.Equal(t, "CEO", capturedFields["Title"])
	})

	t.Run("empty id", func(t *testing.T) {
		mock := &mockClient{}
		err := UpdateContact(context.Background(), mock, "", map[string]any{"Title": "VP"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contact id is required")
	})

	t.Run("empty fields", func(t *testing.T) {
		mock := &mockClient{}
		err := UpdateContact(context.Background(), mock, "003xx", map[string]any{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no fields to update")
	})

	t.Run("propagates error", func(t *testing.T) {
		mock := &mockClient{
			updateOneFn: func(_ context.Context, _ string, _ string, _ map[string]any) error {
				return errors.New("unauthorized")
			},
		}

		err := UpdateContact(context.Background(), mock, "003xx", map[string]any{"Title": "VP"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "update contact")
	})
}
