package temporal

import "github.com/sells-group/research-cli/internal/temporal/adv"

// Activities holds dependencies for ADV Temporal activities.
// Deprecated: use adv.Activities instead.
type Activities = adv.Activities

// DownloadParams holds input for the DownloadFOIAZIP activity.
type DownloadParams = adv.DownloadParams

// ExtractParams holds input for the ExtractAndMapPDFs activity.
type ExtractParams = adv.ExtractParams

// DocMappingResult represents one row from a brochure/CRS mapping CSV.
type DocMappingResult = adv.DocMappingResult

// ProcessPDFParams holds input for the ProcessPDFViaDocling activity.
type ProcessPDFParams = adv.ProcessPDFParams

// ProcessPDFResult holds the output of PDF processing via Docling.
type ProcessPDFResult = adv.ProcessPDFResult

// UpsertParams holds input for the UpsertDocumentBatch activity.
type UpsertParams = adv.UpsertParams

// UpsertSectionParams holds input for the UpsertSectionBatch activity.
type UpsertSectionParams = adv.UpsertSectionParams
