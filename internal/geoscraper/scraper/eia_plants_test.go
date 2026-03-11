package scraper

import (
	"archive/zip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tealeg/xlsx/v2"

	"github.com/sells-group/research-cli/internal/fetcher"
	"github.com/sells-group/research-cli/internal/geoscraper"
)

func TestEIAPlants_Metadata(t *testing.T) {
	s := &EIAPlants{}
	assert.Equal(t, "eia_plants", s.Name())
	assert.Equal(t, "geo.infrastructure", s.Table())
	assert.Equal(t, geoscraper.National, s.Category())
	assert.Equal(t, geoscraper.Annual, s.Cadence())
}

func TestEIAPlants_ShouldRun(t *testing.T) {
	s := &EIAPlants{}
	now := fixedNow()

	assert.True(t, s.ShouldRun(now, nil))

	recent := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	assert.False(t, s.ShouldRun(now, &recent))

	stale := time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)
	assert.True(t, s.ShouldRun(now, &stale))
}

func TestEIAPlants_Sync(t *testing.T) {
	zipPath := createTestEIAZip(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 2)

	s := &EIAPlants{downloadURL: srv.URL + "/eia860.zip", year: 2024}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsSynced)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEIAPlants_SkipZeroLatLon(t *testing.T) {
	zipPath := createTestEIAZipWithZeroCoords(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	expectBulkUpsert(mock, 1)

	s := &EIAPlants{downloadURL: srv.URL + "/eia860.zip", year: 2024}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	result, err := s.Sync(context.Background(), mock, f, t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsSynced) // one plant skipped (0,0 coords)
}

func TestEIAPlants_DownloadError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EIAPlants{downloadURL: srv.URL + "/bad.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download")
}

func TestEIAPlants_UpsertError(t *testing.T) {
	zipPath := createTestEIAZip(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, err := os.ReadFile(zipPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(assert.AnError)

	s := &EIAPlants{downloadURL: srv.URL + "/eia860.zip", year: 2024}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upsert")
}

func TestEIAPlants_BuildURL_Default(t *testing.T) {
	s := &EIAPlants{}
	url := s.buildURL(2024)
	assert.Contains(t, url, "eia.gov")
	assert.Contains(t, url, "eia8602024.zip")
}

func TestFindSheet(t *testing.T) {
	wb := xlsx.NewFile()
	_, _ = wb.AddSheet("Readme")
	_, _ = wb.AddSheet("2___Plant_Y2024")

	s := findSheet(wb, "plant")
	require.NotNil(t, s)
	assert.Equal(t, "2___Plant_Y2024", s.Name)

	assert.Nil(t, findSheet(wb, "nonexistent"))
}

func TestFindEIAFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "2___Plant_Y2024.xlsx"), []byte("test"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("test"), 0o644))

	path, err := findEIAFile(dir, "plant")
	require.NoError(t, err)
	assert.Contains(t, path, "Plant_Y2024.xlsx")

	_, err = findEIAFile(dir, "generator")
	require.Error(t, err)
}

func TestXlsxVal(t *testing.T) {
	sheet := xlsx.NewFile()
	s, _ := sheet.AddSheet("test")
	row := s.AddRow()
	row.AddCell().SetValue("hello")
	row.AddCell().SetValue("  world  ")

	colIdx := map[string]int{"col1": 0, "col2": 1}
	assert.Equal(t, "hello", xlsxVal(row, colIdx, "col1"))
	assert.Equal(t, "world", xlsxVal(row, colIdx, "col2"))
	assert.Equal(t, "", xlsxVal(row, colIdx, "missing"))
}

func TestEnrichPlantsWithGenerators(t *testing.T) {
	plants := []*eiaPlant{
		{code: 1, name: "Plant A"},
		{code: 2, name: "Plant B"},
	}
	genData := map[int]*eiaGeneratorAgg{
		1: {totalCapacity: 500.0, primaryFuel: "NG", maxCapFuel: 300.0},
	}

	enrichPlantsWithGenerators(plants, genData)
	assert.Equal(t, 500.0, plants[0].capacity)
	assert.Equal(t, "NG", plants[0].fuelType)
	assert.Equal(t, 0.0, plants[1].capacity) // no generator data
}

// ---------- Helpers ----------

// createTestEIAZip creates a test EIA-860 ZIP with plant and generator XLSX files.
func createTestEIAZip(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Create plant XLSX.
	plantWB := xlsx.NewFile()
	plantSheet, err := plantWB.AddSheet("Plant")
	require.NoError(t, err)

	header := plantSheet.AddRow()
	for _, col := range []string{"Plant Code", "Plant Name", "State", "County", "Latitude", "Longitude", "Sector Name"} {
		header.AddCell().SetValue(col)
	}
	addPlantRow(plantSheet, "1001", "Austin Power Plant", "TX", "Travis", "30.267", "-97.743", "Electric Utility")
	addPlantRow(plantSheet, "1002", "Houston Generator", "TX", "Harris", "29.760", "-95.370", "IPP Non-CHP")

	plantPath := filepath.Join(dir, "2___Plant_Y2024.xlsx")
	require.NoError(t, plantWB.Save(plantPath))

	// Create generator XLSX.
	genWB := xlsx.NewFile()
	genSheet, err := genWB.AddSheet("Operable")
	require.NoError(t, err)

	genHeader := genSheet.AddRow()
	for _, col := range []string{"Plant Code", "Generator ID", "Nameplate Capacity (MW)", "Energy Source 1"} {
		genHeader.AddCell().SetValue(col)
	}
	addGenRow(genSheet, "1001", "GEN1", "200", "NG")
	addGenRow(genSheet, "1001", "GEN2", "300", "NG")
	addGenRow(genSheet, "1002", "GEN1", "150", "SUN")

	genPath := filepath.Join(dir, "3_1_Generator_Y2024.xlsx")
	require.NoError(t, genWB.Save(genPath))

	// ZIP both files.
	zipPath := filepath.Join(dir, "eia860.zip")
	zipFile, err := os.Create(zipPath)
	require.NoError(t, err)
	defer zipFile.Close() //nolint:errcheck

	zw := zip.NewWriter(zipFile)
	for _, name := range []string{"2___Plant_Y2024.xlsx", "3_1_Generator_Y2024.xlsx"} {
		data, readErr := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, readErr)
		fw, createErr := zw.Create(name)
		require.NoError(t, createErr)
		_, _ = fw.Write(data)
	}
	require.NoError(t, zw.Close())

	return zipPath
}

// createTestEIAZipWithZeroCoords creates a ZIP where one plant has 0,0 coordinates.
func createTestEIAZipWithZeroCoords(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	plantWB := xlsx.NewFile()
	plantSheet, err := plantWB.AddSheet("Plant")
	require.NoError(t, err)

	header := plantSheet.AddRow()
	for _, col := range []string{"Plant Code", "Plant Name", "State", "County", "Latitude", "Longitude", "Sector Name"} {
		header.AddCell().SetValue(col)
	}
	addPlantRow(plantSheet, "1001", "Valid Plant", "TX", "Travis", "30.267", "-97.743", "Utility")
	addPlantRow(plantSheet, "1002", "No Coords", "TX", "Harris", "0", "0", "Utility")

	plantPath := filepath.Join(dir, "2___Plant_Y2024.xlsx")
	require.NoError(t, plantWB.Save(plantPath))

	zipPath := filepath.Join(dir, "eia860.zip")
	zipFile, err := os.Create(zipPath)
	require.NoError(t, err)
	defer zipFile.Close() //nolint:errcheck

	zw := zip.NewWriter(zipFile)
	data, err := os.ReadFile(plantPath)
	require.NoError(t, err)
	fw, err := zw.Create("2___Plant_Y2024.xlsx")
	require.NoError(t, err)
	_, _ = fw.Write(data)
	require.NoError(t, zw.Close())

	return zipPath
}

func addPlantRow(sheet *xlsx.Sheet, code, name, state, county, lat, lon, sector string) {
	row := sheet.AddRow()
	for _, val := range []string{code, name, state, county, lat, lon, sector} {
		row.AddCell().SetValue(val)
	}
}

func addGenRow(sheet *xlsx.Sheet, code, genID, capacity, fuel string) {
	row := sheet.AddRow()
	for _, val := range []string{code, genID, capacity, fuel} {
		row.AddCell().SetValue(val)
	}
}
