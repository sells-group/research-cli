package fedsync

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncResult_Defaults(t *testing.T) {
	r := &SyncResult{}
	assert.Equal(t, int64(0), r.RowsSynced)
	assert.Nil(t, r.Metadata)
}

func TestSyncEntry_Fields(t *testing.T) {
	e := SyncEntry{
		ID:      1,
		Dataset: "cbp",
		Status:  "complete",
	}
	assert.Equal(t, "cbp", e.Dataset)
	assert.Equal(t, "complete", e.Status)
	assert.Nil(t, e.CompletedAt)
	assert.Empty(t, e.Error)
}
