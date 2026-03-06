package temporal

import "go.temporal.io/sdk/worker"

// WorkerSetup holds registration info for a single task queue.
type WorkerSetup struct {
	Queue    string
	Register func(w worker.Worker)
}

// AllQueues returns the names of all known task queues.
func AllQueues() []string {
	return []string{FedsyncTaskQueue, GeoTaskQueue, EnrichmentTaskQueue, ADVDocumentQueue}
}
