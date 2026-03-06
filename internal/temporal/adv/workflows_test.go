package adv

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestDocumentSyncWorkflow_Success(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	// Register activities so the test environment knows them.
	a := &Activities{}
	env.RegisterActivity(a.DownloadFOIAZIP)
	env.RegisterActivity(a.ExtractAndMapPDFs)
	env.RegisterActivity(a.ProcessPDFViaDocling)
	env.RegisterActivity(a.UpsertDocumentBatch)
	env.RegisterActivity(a.UpsertSectionBatch)

	params := SyncWorkflowParams{
		ZIPURL:        "https://example.com/brochures.zip",
		TempDir:       "/tmp/test",
		Name:          "adv_part2",
		Table:         "fed_data.adv_brochures",
		SectionsTable: "fed_data.adv_brochure_sections",
		Columns:       []string{"crd_number", "brochure_id", "filing_date", "text_content", "updated_at"},
		ConflictKeys:  []string{"crd_number", "brochure_id"},
		BatchSize:     500,
		FirmLimit:     10,
	}

	// Mock DownloadFOIAZIP.
	env.OnActivity(a.DownloadFOIAZIP, mock.Anything, DownloadParams{
		ZIPURL:  params.ZIPURL,
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Return("/tmp/test/adv_part2.zip", nil)

	// Mock ExtractAndMapPDFs.
	env.OnActivity(a.ExtractAndMapPDFs, mock.Anything, ExtractParams{
		ZIPPath: "/tmp/test/adv_part2.zip",
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Return([]DocMappingResult{
		{CRDNumber: 12345, DocID: "doc1", DateFiled: "2024-01-15", PDFFileName: "12345.pdf", PDFPath: "/tmp/test/12345.pdf"},
		{CRDNumber: 67890, DocID: "doc2", DateFiled: "2024-02-20", PDFFileName: "67890.pdf", PDFPath: "/tmp/test/67890.pdf"},
	}, nil)

	// Mock ProcessPDFViaDocling for each PDF.
	env.OnActivity(a.ProcessPDFViaDocling, mock.Anything, mock.Anything).Return(
		&ProcessPDFResult{
			FullText:    "Sample brochure text",
			CRDNumber:   12345,
			DocID:       "doc1",
			DateFiled:   "2024-01-15",
			SectionKeys: []string{"Item 4", "Item 5"},
		}, nil)

	// Mock UpsertDocumentBatch.
	env.OnActivity(a.UpsertDocumentBatch, mock.Anything, mock.Anything).Return(int64(2), nil)

	env.ExecuteWorkflow(DocumentSyncWorkflow, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestDocumentSyncWorkflow_DownloadFails(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a.DownloadFOIAZIP)
	env.RegisterActivity(a.ExtractAndMapPDFs)
	env.RegisterActivity(a.ProcessPDFViaDocling)
	env.RegisterActivity(a.UpsertDocumentBatch)
	env.RegisterActivity(a.UpsertSectionBatch)

	params := SyncWorkflowParams{
		ZIPURL:        "https://example.com/brochures.zip",
		TempDir:       "/tmp/test",
		Name:          "adv_part2",
		Table:         "fed_data.adv_brochures",
		SectionsTable: "fed_data.adv_brochure_sections",
		Columns:       []string{"crd_number", "brochure_id", "filing_date", "text_content", "updated_at"},
		ConflictKeys:  []string{"crd_number", "brochure_id"},
		BatchSize:     500,
		FirmLimit:     10,
	}

	// Mock DownloadFOIAZIP to fail.
	env.OnActivity(a.DownloadFOIAZIP, mock.Anything, mock.Anything).
		Return("", errDownloadFailed)

	env.ExecuteWorkflow(DocumentSyncWorkflow, params)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func TestDocumentSyncWorkflow_WithSections(t *testing.T) {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()

	a := &Activities{}
	env.RegisterActivity(a.DownloadFOIAZIP)
	env.RegisterActivity(a.ExtractAndMapPDFs)
	env.RegisterActivity(a.ProcessPDFViaDocling)
	env.RegisterActivity(a.UpsertDocumentBatch)
	env.RegisterActivity(a.UpsertSectionBatch)

	params := SyncWorkflowParams{
		ZIPURL:        "https://example.com/brochures.zip",
		TempDir:       "/tmp/test",
		Name:          "adv_part2",
		Table:         "fed_data.adv_brochures",
		SectionsTable: "fed_data.adv_brochure_sections",
		Columns:       []string{"crd_number", "brochure_id", "filing_date", "text_content", "updated_at"},
		ConflictKeys:  []string{"crd_number", "brochure_id"},
		BatchSize:     500,
		FirmLimit:     10,
	}

	env.OnActivity(a.DownloadFOIAZIP, mock.Anything, DownloadParams{
		ZIPURL:  params.ZIPURL,
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Return("/tmp/test/adv_part2.zip", nil)

	env.OnActivity(a.ExtractAndMapPDFs, mock.Anything, ExtractParams{
		ZIPPath: "/tmp/test/adv_part2.zip",
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Return([]DocMappingResult{
		{CRDNumber: 12345, DocID: "doc1", DateFiled: "2024-01-15", PDFFileName: "12345.pdf", PDFPath: "/tmp/test/12345.pdf"},
	}, nil)

	env.OnActivity(a.ProcessPDFViaDocling, mock.Anything, mock.Anything).Return(
		&ProcessPDFResult{
			FullText:    "Sample brochure text with sections",
			CRDNumber:   12345,
			DocID:       "doc1",
			DateFiled:   "2024-01-15",
			SectionKeys: []string{"Item 4", "Item 5"},
		}, nil)

	// The current workflow declares sectionRows but never populates it,
	// so UpsertSectionBatch should NOT be called.
	env.OnActivity(a.UpsertDocumentBatch, mock.Anything, mock.Anything).Return(int64(1), nil)

	env.ExecuteWorkflow(DocumentSyncWorkflow, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestContainsCRS(t *testing.T) {
	tests := []struct {
		table string
		want  bool
	}{
		{"fed_data.adv_crs_sections", true},
		{"fed_data.crs_documents", true},
		{"crs", true},
		{"fed_data.adv_brochure_sections", false},
		{"fed_data.adv_brochures", false},
		{"", false},
		{"cr", false},
		{"CRS", false}, // implementation is case-sensitive lowercase only
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			require.Equal(t, tt.want, containsCRS(tt.table))
		})
	}
}

func TestSectionColumns(t *testing.T) {
	t.Run("brochure table", func(t *testing.T) {
		cols := sectionColumns("fed_data.adv_brochure_sections")
		require.Equal(t, []string{
			"crd_number", "brochure_id", "section_key", "section_title",
			"text_content", "tables", "metadata", "updated_at",
		}, cols)
	})

	t.Run("CRS table", func(t *testing.T) {
		cols := sectionColumns("fed_data.adv_crs_sections")
		require.Equal(t, []string{
			"crd_number", "crs_id", "section_key", "section_title",
			"text_content", "tables", "metadata", "updated_at",
		}, cols)
	})
}

func TestSectionConflictKeys(t *testing.T) {
	t.Run("brochure table", func(t *testing.T) {
		keys := sectionConflictKeys("fed_data.adv_brochure_sections")
		require.Equal(t, []string{"crd_number", "brochure_id", "section_key"}, keys)
	})

	t.Run("CRS table", func(t *testing.T) {
		keys := sectionConflictKeys("fed_data.adv_crs_sections")
		require.Equal(t, []string{"crd_number", "crs_id", "section_key"}, keys)
	})
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"12345", 12345},
		{"0", 0},
		{"  42  ", 42},
		{"abc", 0},
		{"12abc34", 1234},
		{"", 0},
		{" 100 ", 100},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.want, parseInt(tt.input))
		})
	}
}

var errDownloadFailed = &testError{msg: "download failed: connection timeout"}

type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }
