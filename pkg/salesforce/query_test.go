package salesforce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindAccountByWebsite(t *testing.T) {
	t.Run("returns account when found", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, soql string, out any) error {
				assert.Contains(t, soql, "Website LIKE 'acme.com'")
				assert.Contains(t, soql, "SELECT Id, Name")

				accounts := out.(*[]Account)
				*accounts = []Account{
					{ID: "001xx", Name: "Acme Corp", Website: "acme.com"},
				}
				return nil
			},
		}

		acct, err := FindAccountByWebsite(context.Background(), mock, "acme.com")
		require.NoError(t, err)
		require.NotNil(t, acct)
		assert.Equal(t, "001xx", acct.ID)
		assert.Equal(t, "Acme Corp", acct.Name)
	})

	t.Run("returns nil when not found", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, _ string, out any) error {
				accounts := out.(*[]Account)
				*accounts = []Account{}
				return nil
			},
		}

		acct, err := FindAccountByWebsite(context.Background(), mock, "nonexistent.com")
		require.NoError(t, err)
		assert.Nil(t, acct)
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, _ string, _ any) error {
				return errors.New("connection refused")
			},
		}

		acct, err := FindAccountByWebsite(context.Background(), mock, "acme.com")
		assert.Error(t, err)
		assert.Nil(t, acct)
		assert.Contains(t, err.Error(), "find account by website")
	})
}

func TestFindAccountByID(t *testing.T) {
	t.Run("returns account when found", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, soql string, out any) error {
				assert.Contains(t, soql, "Id = '001xx'")
				assert.Contains(t, soql, "LIMIT 1")

				accounts := out.(*[]Account)
				*accounts = []Account{
					{ID: "001xx", Name: "Acme Corp"},
				}
				return nil
			},
		}

		acct, err := FindAccountByID(context.Background(), mock, "001xx")
		require.NoError(t, err)
		require.NotNil(t, acct)
		assert.Equal(t, "001xx", acct.ID)
	})

	t.Run("returns nil when not found", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, _ string, out any) error {
				accounts := out.(*[]Account)
				*accounts = []Account{}
				return nil
			},
		}

		acct, err := FindAccountByID(context.Background(), mock, "001notfound")
		require.NoError(t, err)
		assert.Nil(t, acct)
	})
}

func TestEscapeSoql(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"acme.com", "acme.com"},
		{"O'Reilly", "O\\'Reilly"},
		{"it's a test's case", "it\\'s a test\\'s case"},
		{"no-quotes", "no-quotes"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, escapeSoql(tt.input))
		})
	}
}

func TestSOQLContainsAllAccountFields(t *testing.T) {
	mock := &mockClient{
		queryFn: func(_ context.Context, soql string, out any) error {
			for _, field := range accountFields {
				assert.Contains(t, soql, field, "SOQL should contain field: %s", field)
			}
			accounts := out.(*[]Account)
			*accounts = []Account{}
			return nil
		},
	}

	_, _ = FindAccountByWebsite(context.Background(), mock, "test.com")
}

func TestFindContactsByAccountID(t *testing.T) {
	t.Run("returns contacts when found", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, soql string, out any) error {
				assert.Contains(t, soql, "AccountId = '001xx'")
				assert.Contains(t, soql, "SELECT Id, FirstName, LastName, Email")

				contacts := out.(*[]Contact)
				*contacts = []Contact{
					{ID: "003a", FirstName: "Jane", LastName: "Doe", Email: "jane@acme.com", AccountID: "001xx"},
					{ID: "003b", FirstName: "John", LastName: "Smith", AccountID: "001xx"},
				}
				return nil
			},
		}

		contacts, err := FindContactsByAccountID(context.Background(), mock, "001xx")
		require.NoError(t, err)
		require.Len(t, contacts, 2)
		assert.Equal(t, "003a", contacts[0].ID)
		assert.Equal(t, "jane@acme.com", contacts[0].Email)
		assert.Equal(t, "003b", contacts[1].ID)
	})

	t.Run("returns empty slice when none found", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, _ string, out any) error {
				contacts := out.(*[]Contact)
				*contacts = []Contact{}
				return nil
			},
		}

		contacts, err := FindContactsByAccountID(context.Background(), mock, "001empty")
		require.NoError(t, err)
		assert.Empty(t, contacts)
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		mock := &mockClient{
			queryFn: func(_ context.Context, _ string, _ any) error {
				return errors.New("timeout")
			},
		}

		contacts, err := FindContactsByAccountID(context.Background(), mock, "001fail")
		assert.Error(t, err)
		assert.Nil(t, contacts)
		assert.Contains(t, err.Error(), "find contacts for account")
	})
}
