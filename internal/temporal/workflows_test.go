package temporal

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/sells-group/research-cli/internal/temporal/adv"
)

// These tests verify the backward-compatible re-exports work correctly.
// Comprehensive tests live in internal/temporal/adv/.

func TestADVDocumentSyncWorkflow_Success(t *testing.T) {
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

	env.OnActivity(a.DownloadFOIAZIP, mock.Anything, adv.DownloadParams{
		ZIPURL:  params.ZIPURL,
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Return("/tmp/test/adv_part2.zip", nil)

	env.OnActivity(a.ExtractAndMapPDFs, mock.Anything, adv.ExtractParams{
		ZIPPath: "/tmp/test/adv_part2.zip",
		TempDir: params.TempDir,
		Name:    params.Name,
	}).Return([]adv.DocMappingResult{
		{CRDNumber: 12345, DocID: "doc1", DateFiled: "2024-01-15", PDFFileName: "12345.pdf", PDFPath: "/tmp/test/12345.pdf"},
		{CRDNumber: 67890, DocID: "doc2", DateFiled: "2024-02-20", PDFFileName: "67890.pdf", PDFPath: "/tmp/test/67890.pdf"},
	}, nil)

	env.OnActivity(a.ProcessPDFViaDocling, mock.Anything, mock.Anything).Return(
		&adv.ProcessPDFResult{
			FullText:    "Sample brochure text",
			CRDNumber:   12345,
			DocID:       "doc1",
			DateFiled:   "2024-01-15",
			SectionKeys: []string{"Item 4", "Item 5"},
		}, nil)

	env.OnActivity(a.UpsertDocumentBatch, mock.Anything, mock.Anything).Return(int64(2), nil)

	env.ExecuteWorkflow(ADVDocumentSyncWorkflow, params)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestADVDocumentSyncWorkflow_DownloadFails(t *testing.T) {
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

	env.OnActivity(a.DownloadFOIAZIP, mock.Anything, mock.Anything).
		Return("", errDownloadFailed)

	env.ExecuteWorkflow(ADVDocumentSyncWorkflow, params)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

var errDownloadFailed = &testError{msg: "download failed: connection timeout"}

type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }
