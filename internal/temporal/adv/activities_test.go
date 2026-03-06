package adv

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"

	"github.com/sells-group/research-cli/internal/docling"
)

// --- mocks ---

type mockFetcher struct {
	downloadToFileFn func(ctx context.Context, url string, path string) (int64, error)
}

func (m *mockFetcher) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockFetcher) DownloadToFile(ctx context.Context, url string, path string) (int64, error) {
	return m.downloadToFileFn(ctx, url, path)
}

func (m *mockFetcher) HeadETag(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (m *mockFetcher) DownloadIfChanged(_ context.Context, _ string, _ string) (io.ReadCloser, string, bool, error) {
	return nil, "", false, fmt.Errorf("not implemented")
}

type mockDocling struct {
	convertFn func(ctx context.Context, pdfData []byte, opts docling.ConvertOpts) (*docling.Document, error)
}

func (m *mockDocling) Convert(ctx context.Context, pdfData []byte, opts docling.ConvertOpts) (*docling.Document, error) {
	return m.convertFn(ctx, pdfData, opts)
}

// --- helpers ---

func createTestZIP(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	zipPath := filepath.Join(dir, "test.zip")
	f, err := os.Create(zipPath)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck
	w := zip.NewWriter(f)
	for name, content := range files {
		fw, err := w.Create(name)
		require.NoError(t, err)
		_, err = fw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return zipPath
}

// --- DownloadFOIAZIP tests ---

func TestDownloadFOIAZIP_Success(t *testing.T) {
	dir := t.TempDir()

	act := &Activities{
		Fetcher: &mockFetcher{
			downloadToFileFn: func(_ context.Context, _ string, path string) (int64, error) {
				// Write a small file so the path exists.
				if err := os.WriteFile(path, []byte("zipdata"), 0o600); err != nil {
					return 0, err
				}
				return 7, nil
			},
		},
	}

	params := DownloadParams{
		ZIPURL:  "https://example.com/file.zip",
		TempDir: filepath.Join(dir, "sub"),
		Name:    "brochures",
	}

	got, err := act.DownloadFOIAZIP(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "sub", "brochures.zip"), got)

	// Verify temp dir was created.
	info, err := os.Stat(filepath.Join(dir, "sub"))
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestDownloadFOIAZIP_Error(t *testing.T) {
	dir := t.TempDir()

	act := &Activities{
		Fetcher: &mockFetcher{
			downloadToFileFn: func(_ context.Context, _ string, _ string) (int64, error) {
				return 0, fmt.Errorf("network timeout")
			},
		},
	}

	params := DownloadParams{
		ZIPURL:  "https://example.com/file.zip",
		TempDir: dir,
		Name:    "brochures",
	}

	_, err := act.DownloadFOIAZIP(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adv: download ZIP")
	require.Contains(t, err.Error(), "network timeout")
}

// --- ExtractAndMapPDFs tests ---

func TestExtractAndMapPDFs_Success(t *testing.T) {
	dir := t.TempDir()

	csvContent := "crd_number,doc_id,date_filed,pdf_filename\n" +
		"12345,DOC-001,2025-01-15,brochure_12345.pdf\n" +
		"67890,DOC-002,2025-02-20,brochure_67890.pdf\n"

	zipPath := createTestZIP(t, dir, map[string]string{
		"brochure_mapping.csv": csvContent,
		"brochure_12345.pdf":   "fake-pdf-content-1",
		"brochure_67890.pdf":   "fake-pdf-content-2",
	})

	act := &Activities{}
	params := ExtractParams{
		ZIPPath: zipPath,
		TempDir: dir,
		Name:    "adv_brochures",
	}

	results, err := act.ExtractAndMapPDFs(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, results, 2)

	require.Equal(t, 12345, results[0].CRDNumber)
	require.Equal(t, "DOC-001", results[0].DocID)
	require.Equal(t, "2025-01-15", results[0].DateFiled)
	require.Equal(t, "brochure_12345.pdf", results[0].PDFFileName)
	require.NotEmpty(t, results[0].PDFPath)

	require.Equal(t, 67890, results[1].CRDNumber)
	require.Equal(t, "DOC-002", results[1].DocID)
	require.NotEmpty(t, results[1].PDFPath)
}

func TestExtractAndMapPDFs_MissingMappingCSV(t *testing.T) {
	dir := t.TempDir()

	zipPath := createTestZIP(t, dir, map[string]string{
		"brochure_12345.pdf": "fake-pdf",
		"readme.txt":         "no mapping here",
	})

	act := &Activities{}
	params := ExtractParams{
		ZIPPath: zipPath,
		TempDir: dir,
		Name:    "adv_brochures",
	}

	_, err := act.ExtractAndMapPDFs(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mapping CSV not found")
}

func TestExtractAndMapPDFs_UnmatchedPDF(t *testing.T) {
	dir := t.TempDir()

	csvContent := "crd_number,doc_id,date_filed,pdf_filename\n" +
		"11111,DOC-X,2025-03-01,missing_file.pdf\n"

	zipPath := createTestZIP(t, dir, map[string]string{
		"brochure_mapping.csv": csvContent,
	})

	act := &Activities{}
	params := ExtractParams{
		ZIPPath: zipPath,
		TempDir: dir,
		Name:    "test",
	}

	results, err := act.ExtractAndMapPDFs(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 11111, results[0].CRDNumber)
	require.Empty(t, results[0].PDFPath) // PDF not in ZIP, so path is empty
}

// --- ProcessPDFViaDocling tests ---

func TestProcessPDFViaDocling_Success(t *testing.T) {
	dir := t.TempDir()
	pdfPath := filepath.Join(dir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("fake-pdf-bytes"), 0o600))

	act := &Activities{
		Docling: &mockDocling{
			convertFn: func(_ context.Context, _ []byte, _ docling.ConvertOpts) (*docling.Document, error) {
				return &docling.Document{
					Pages: []docling.Page{
						{
							Elements: []docling.Element{
								{Type: "heading", Text: "Introduction"},
								{Type: "paragraph", Text: "This is the intro."},
								{Type: "heading", Text: "Fees"},
								{Type: "paragraph", Text: "We charge 1%."},
							},
						},
					},
				}, nil
			},
		},
	}

	params := ProcessPDFParams{
		PDFPath:   pdfPath,
		CRDNumber: 99999,
		DocID:     "DOC-ABC",
		DateFiled: "2025-06-01",
	}

	result, err := act.ProcessPDFViaDocling(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, 99999, result.CRDNumber)
	require.Equal(t, "DOC-ABC", result.DocID)
	require.Equal(t, "2025-06-01", result.DateFiled)
	require.Contains(t, result.FullText, "Introduction")
	require.Contains(t, result.FullText, "This is the intro.")
	require.Contains(t, result.FullText, "Fees")
	require.Equal(t, []string{"Introduction", "Fees"}, result.SectionKeys)
}

func TestProcessPDFViaDocling_ReadFailure(t *testing.T) {
	act := &Activities{
		Docling: &mockDocling{
			convertFn: func(_ context.Context, _ []byte, _ docling.ConvertOpts) (*docling.Document, error) {
				return nil, fmt.Errorf("should not be called")
			},
		},
	}

	params := ProcessPDFParams{
		PDFPath:   "/nonexistent/path/to/file.pdf",
		CRDNumber: 1,
		DocID:     "X",
	}

	_, err := act.ProcessPDFViaDocling(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adv: read PDF")
}

func TestProcessPDFViaDocling_ConvertFailure(t *testing.T) {
	dir := t.TempDir()
	pdfPath := filepath.Join(dir, "test.pdf")
	require.NoError(t, os.WriteFile(pdfPath, []byte("fake"), 0o600))

	act := &Activities{
		Docling: &mockDocling{
			convertFn: func(_ context.Context, _ []byte, _ docling.ConvertOpts) (*docling.Document, error) {
				return nil, fmt.Errorf("docling service unavailable")
			},
		},
	}

	params := ProcessPDFParams{
		PDFPath:   pdfPath,
		CRDNumber: 42,
		DocID:     "DOC-Z",
	}

	_, err := act.ProcessPDFViaDocling(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adv: docling convert")
	require.Contains(t, err.Error(), "docling service unavailable")
}

// --- UpsertDocumentBatch tests ---

func TestUpsertDocumentBatch_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_adv_documents"}, []string{"crd_number", "doc_id", "full_text"}).WillReturnResult(2)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mock.ExpectCommit()

	act := &Activities{Pool: mock}
	params := UpsertParams{
		Table:        "fed_data.adv_documents",
		Columns:      []string{"crd_number", "doc_id", "full_text"},
		ConflictKeys: []string{"crd_number", "doc_id"},
		Rows:         [][]any{{100, "D1", "text1"}, {200, "D2", "text2"}},
	}

	n, err := act.UpsertDocumentBatch(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertDocumentBatch_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("connection refused"))

	act := &Activities{Pool: mock}
	params := UpsertParams{
		Table:        "fed_data.adv_documents",
		Columns:      []string{"crd_number", "doc_id", "full_text"},
		ConflictKeys: []string{"crd_number", "doc_id"},
		Rows:         [][]any{{100, "D1", "text1"}},
	}

	_, err = act.UpsertDocumentBatch(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adv: upsert documents")
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- UpsertSectionBatch tests ---

func TestUpsertSectionBatch_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_adv_sections"}, []string{"crd_number", "doc_id", "section_key", "content"}).WillReturnResult(3)
	mock.ExpectExec("DELETE FROM").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(pgxmock.NewResult("INSERT", 3))
	mock.ExpectCommit()

	act := &Activities{Pool: mock}
	params := UpsertSectionParams{
		Table:        "fed_data.adv_sections",
		Columns:      []string{"crd_number", "doc_id", "section_key", "content"},
		ConflictKeys: []string{"crd_number", "doc_id", "section_key"},
		Rows:         [][]any{{100, "D1", "intro", "text"}, {100, "D1", "fees", "1%"}, {200, "D2", "intro", "hi"}},
	}

	n, err := act.UpsertSectionBatch(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertSectionBatch_Error(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TEMP TABLE").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	mock.ExpectCopyFrom(pgx.Identifier{"_tmp_upsert_fed_data_adv_sections"}, []string{"section_key", "content"}).WillReturnError(fmt.Errorf("copy failed"))
	mock.ExpectRollback()

	act := &Activities{Pool: mock}
	params := UpsertSectionParams{
		Table:        "fed_data.adv_sections",
		Columns:      []string{"section_key", "content"},
		ConflictKeys: []string{"section_key"},
		Rows:         [][]any{{"intro", "text"}},
	}

	_, err = act.UpsertSectionBatch(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adv: upsert sections")
	require.NoError(t, mock.ExpectationsWereMet())
}
