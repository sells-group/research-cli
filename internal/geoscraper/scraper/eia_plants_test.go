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

func TestEIAPlants_ContextCancelled(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	s := &EIAPlants{downloadURL: "http://127.0.0.1:1/eia860.zip"}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(ctx, mock, f, t.TempDir())
	require.Error(t, err)
}

func TestEIAPlants_ExtractError(t *testing.T) {
	// Serve a corrupt ZIP to trigger extraction error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a zip file"))
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EIAPlants{downloadURL: srv.URL + "/eia860.zip", year: 2024}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract")
}

func TestEIAPlants_NoPlantFile(t *testing.T) {
	// Create a ZIP with no plant XLSX.
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "eia860.zip")
	zf, err := os.Create(zipPath)
	require.NoError(t, err)
	zw := zip.NewWriter(zf)
	fw, err := zw.Create("readme.txt")
	require.NoError(t, err)
	_, _ = fw.Write([]byte("no plant file"))
	require.NoError(t, zw.Close())
	require.NoError(t, zf.Close())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		data, rErr := os.ReadFile(zipPath)
		if rErr != nil {
			http.Error(w, rErr.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EIAPlants{downloadURL: srv.URL + "/eia860.zip", year: 2024}
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "find plant file")
}

func TestEIAPlants_YearDefault(t *testing.T) {
	// When year is 0, Sync uses default eiaPlantYear.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	s := &EIAPlants{downloadURL: srv.URL + "/eia860.zip"} // year=0
	f := fetcher.NewHTTPFetcher(fetcher.HTTPOptions{MaxRetries: 0})
	_, err = s.Sync(context.Background(), mock, f, t.TempDir())
	require.Error(t, err) // download fails with 404
	assert.Contains(t, err.Error(), "download")
}

func TestEIAPlantProperties(t *testing.T) {
	p := &eiaPlant{
		code:   1001,
		name:   "Test Plant",
		state:  "TX",
		county: "Travis",
		sector: "Electric Utility",
		extra:  map[string]string{"City": "Austin", "Empty": ""},
	}
	props := p.properties()
	assert.Contains(t, string(props), "\"state\":\"TX\"")
	assert.Contains(t, string(props), "\"county\":\"Travis\"")
	assert.Contains(t, string(props), "\"sector\":\"Electric Utility\"")
	assert.Contains(t, string(props), "\"City\":\"Austin\"")
	assert.NotContains(t, string(props), "Empty")
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

func TestFindEIAFile_NotFound(t *testing.T) {
	dir := t.TempDir()
	// Empty directory — no .xlsx files at all.
	_, err := findEIAFile(dir, "plant")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no plant .xlsx file found")
}

func TestParseEIAPlantXLSX_NoHeader(t *testing.T) {
	dir := t.TempDir()
	wb := xlsx.NewFile()
	sheet, err := wb.AddSheet("Plant")
	require.NoError(t, err)

	// Add 15 rows of non-header data (exceeds the 10-row search window).
	for i := 0; i < 15; i++ {
		row := sheet.AddRow()
		row.AddCell().SetValue("not a header")
		row.AddCell().SetValue("still not")
	}

	path := filepath.Join(dir, "plant.xlsx")
	require.NoError(t, wb.Save(path))

	_, err = parseEIAPlantXLSX(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not find header row")
}

func TestParseEIAGeneratorXLSX_NoHeader(t *testing.T) {
	dir := t.TempDir()
	wb := xlsx.NewFile()
	sheet, err := wb.AddSheet("Operable")
	require.NoError(t, err)

	// Add 15 rows without a "Plant Code" header.
	for i := 0; i < 15; i++ {
		row := sheet.AddRow()
		row.AddCell().SetValue("irrelevant")
		row.AddCell().SetValue("data")
	}

	path := filepath.Join(dir, "generator.xlsx")
	require.NoError(t, wb.Save(path))

	_, err = parseEIAGeneratorXLSX(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no header row found")
}

func TestEIAPlants_Sync_NoGeneratorFile(t *testing.T) {
	// Create a ZIP with only the plant XLSX — no generator file.
	zipPath := createTestEIAZipPlantOnly(t)

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

func TestEIAPlants_Sync_GeneratorParseError(t *testing.T) {
	// Create a ZIP with valid plant XLSX but corrupt generator XLSX.
	// The generator file exists (so findEIAFile succeeds) but has no valid sheet.
	zipPath := createTestEIAZipBadGenerator(t)

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
	assert.Equal(t, int64(2), result.RowsSynced) // plants parsed, generator error logged as warning
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFindEIAFile_SkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	// Create a subdirectory named "plant_data.xlsx" — should be skipped.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "plant_data.xlsx"), 0o750))
	// Create the actual file.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "2___Plant_Y2024.xlsx"), []byte("test"), 0o644))

	path, err := findEIAFile(dir, "plant")
	require.NoError(t, err)
	assert.Contains(t, path, "Plant_Y2024.xlsx")
}

func TestFindEIAFile_BadDir(t *testing.T) {
	_, err := findEIAFile("/nonexistent/dir/does/not/exist", "plant")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read directory")
}

func TestParseEIAPlantXLSX_NoSheet(t *testing.T) {
	dir := t.TempDir()
	wb := xlsx.NewFile()
	_, err := wb.AddSheet("Readme") // No "Plant" sheet
	require.NoError(t, err)

	path := filepath.Join(dir, "plant.xlsx")
	require.NoError(t, wb.Save(path))

	_, err = parseEIAPlantXLSX(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no 'Plant' sheet")
}

func TestParseEIAPlantXLSX_BadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.xlsx")
	require.NoError(t, os.WriteFile(path, []byte("not xlsx"), 0o644))

	_, err := parseEIAPlantXLSX(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open plant XLSX")
}

func TestParseEIAGeneratorXLSX_NoSheet(t *testing.T) {
	dir := t.TempDir()
	wb := xlsx.NewFile()
	_, err := wb.AddSheet("Readme") // No "Operable" or "Generator" sheet
	require.NoError(t, err)

	path := filepath.Join(dir, "generator.xlsx")
	require.NoError(t, wb.Save(path))

	_, err = parseEIAGeneratorXLSX(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no generator sheet")
}

func TestParseEIAGeneratorXLSX_BadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.xlsx")
	require.NoError(t, os.WriteFile(path, []byte("not xlsx"), 0o644))

	_, err := parseEIAGeneratorXLSX(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open generator XLSX")
}

func TestParseEIAGeneratorXLSX_FallbackToGeneratorSheet(t *testing.T) {
	// Test the fallback from "operable" to "generator" sheet name.
	dir := t.TempDir()
	wb := xlsx.NewFile()
	sheet, err := wb.AddSheet("Generator Y2024")
	require.NoError(t, err)

	header := sheet.AddRow()
	for _, col := range []string{"Plant Code", "Generator ID", "Nameplate Capacity (MW)", "Energy Source 1"} {
		header.AddCell().SetValue(col)
	}
	addGenRow(sheet, "1001", "GEN1", "200", "NG")

	path := filepath.Join(dir, "generator.xlsx")
	require.NoError(t, wb.Save(path))

	result, err := parseEIAGeneratorXLSX(path)
	require.NoError(t, err)
	require.Contains(t, result, 1001)
	assert.InDelta(t, 200.0, result[1001].totalCapacity, 0.01)
}

func TestXlsxVal_OutOfBounds(t *testing.T) {
	sheet := xlsx.NewFile()
	s, _ := sheet.AddSheet("test")
	row := s.AddRow()
	row.AddCell().SetValue("only one cell")

	colIdx := map[string]int{"col1": 0, "col2": 5}
	assert.Equal(t, "only one cell", xlsxVal(row, colIdx, "col1"))
	assert.Equal(t, "", xlsxVal(row, colIdx, "col2")) // out of bounds
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

// createTestEIAZipBadGenerator creates a ZIP with valid plant XLSX and a generator
// XLSX that has no valid sheet (triggers parse warning, not a fatal error).
func createTestEIAZipBadGenerator(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Create plant XLSX (same as createTestEIAZip).
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

	// Create generator XLSX with no valid sheet (no "Operable"/"Generator" name).
	genWB := xlsx.NewFile()
	_, err = genWB.AddSheet("Readme")
	require.NoError(t, err)
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

// createTestEIAZipPlantOnly creates a ZIP with only a plant XLSX (no generator file).
func createTestEIAZipPlantOnly(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

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
