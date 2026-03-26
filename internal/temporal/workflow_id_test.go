package temporal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuildWorkflowID_NormalizesAndTruncates(t *testing.T) {
	at := time.Date(2026, time.March, 26, 14, 5, 6, 789123456, time.FixedZone("PDT", -7*60*60))

	id := BuildWorkflowID(at, "Enrich Retry", "HTTPS://Acme.COM/path?q=1", "very-long-label-with-many-segments-and-extra-text")

	assert.Equal(t, "enrich-retry-https-acme-com-path-q-1-very-long-label-with-many-segmen-20260326t210506789123456z", id)
}

func TestBuildWorkflowID_DropsEmptyLabelsAndFallsBackKind(t *testing.T) {
	at := time.Date(2026, time.March, 26, 14, 5, 6, 0, time.UTC)

	id := BuildWorkflowID(at, "!!!", "", "  ")

	assert.Equal(t, "workflow-20260326t140506000000000z", id)
}

func TestWorkflowHostLabel_UsesHostWhenPossible(t *testing.T) {
	assert.Equal(t, "subdomain-example-com", WorkflowHostLabel("https://subdomain.example.com/path?q=1"))
	assert.Equal(t, "acme-com", WorkflowHostLabel("acme.com"))
	assert.Equal(t, "example-org", WorkflowHostLabel(" example.org/no-scheme "))
}

func TestBuildStableWorkflowID_Normalizes(t *testing.T) {
	id := BuildStableWorkflowID("Enrich Retry", "HTTPS://Acme.COM/path?q=1")
	assert.Equal(t, "enrich-retry-https-acme-com-path-q-1", id)
}

func TestWorkflowURLKey_NormalizesAndHashes(t *testing.T) {
	keyA := WorkflowURLKey("HTTPS://Acme.COM/")
	keyB := WorkflowURLKey("https://acme.com")
	keyC := WorkflowURLKey("https://acme.com/about")

	assert.Equal(t, keyA, keyB)
	assert.NotEqual(t, keyA, keyC)
	assert.Contains(t, keyA, "acme-com-")
}
