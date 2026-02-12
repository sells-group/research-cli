package salesforce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
