package geospatial

import (
	"context"
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueueADVFirms_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Query returns 2 firms.
	mock.ExpectQuery(`SELECT crd_number::text`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"crd_number", "address"}).
				AddRow("12345", "100 Main St, Miami, FL 33131").
				AddRow("67890", "200 Broadway, New York, NY 10001"),
		)

	// Batch enqueue transaction.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("fed_data.adv_firms", "12345", "100 Main St, Miami, FL 33131").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("fed_data.adv_firms", "67890", "200 Broadway, New York, NY 10001").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	n, err := bridge.EnqueueADVFirms(context.Background(), 100)

	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueADVFirms_NoResults(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT crd_number::text`).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"crd_number", "address"}))

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	n, err := bridge.EnqueueADVFirms(context.Background(), 100)

	require.NoError(t, err)
	assert.Equal(t, 0, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueADVFirms_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT crd_number::text`).
		WithArgs(100).
		WillReturnError(fmt.Errorf("connection refused"))

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	_, err = bridge.EnqueueADVFirms(context.Background(), 100)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query adv firms")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueEPAFacilities_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Query returns 1 facility.
	mock.ExpectQuery(`SELECT registry_id`).
		WithArgs(50).
		WillReturnRows(
			pgxmock.NewRows([]string{"registry_id", "address"}).
				AddRow("TXD000001234", "Acme Corp, Houston, TX 77001"),
		)

	// Batch enqueue transaction.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO geo.geocode_queue`).
		WithArgs("fed_data.epa_facilities", "TXD000001234", "Acme Corp, Houston, TX 77001").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	n, err := bridge.EnqueueEPAFacilities(context.Background(), 50)

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueEPAFacilities_NoResults(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT registry_id`).
		WithArgs(50).
		WillReturnRows(pgxmock.NewRows([]string{"registry_id", "address"}))

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	n, err := bridge.EnqueueEPAFacilities(context.Background(), 50)

	require.NoError(t, err)
	assert.Equal(t, 0, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueEPAFacilities_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT registry_id`).
		WithArgs(50).
		WillReturnError(fmt.Errorf("timeout"))

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	_, err = bridge.EnqueueEPAFacilities(context.Background(), 50)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query epa facilities")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueADVFirms_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Use RowError to force an error during row iteration.
	mock.ExpectQuery(`SELECT crd_number::text`).
		WithArgs(100).
		WillReturnRows(
			pgxmock.NewRows([]string{"crd_number", "address"}).
				AddRow("12345", "100 Main St, Miami, FL 33131").
				RowError(0, fmt.Errorf("scan error")),
		)

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	_, err = bridge.EnqueueADVFirms(context.Background(), 100)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan adv firm")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEnqueueEPAFacilities_ScanError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Use RowError to force an error during row scanning.
	mock.ExpectQuery(`SELECT registry_id`).
		WithArgs(50).
		WillReturnRows(
			pgxmock.NewRows([]string{"registry_id", "address"}).
				AddRow("TXD000001234", "Acme Corp, Houston, TX 77001").
				RowError(0, fmt.Errorf("scan error")),
		)

	queue := NewGeocodeQueue(mock, nil, 100)
	bridge := NewFedBridge(mock, queue)
	_, err = bridge.EnqueueEPAFacilities(context.Background(), 50)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan epa facility")
	require.NoError(t, mock.ExpectationsWereMet())
}
