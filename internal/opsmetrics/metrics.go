// Package opsmetrics provides lightweight Prometheus-style operational metrics.
package opsmetrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var defaultCollector = New()

var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Collector stores operational metrics for HTTP and cache activity.
type Collector struct {
	mu           sync.RWMutex
	requests     map[string]uint64
	errors       map[string]uint64
	cacheEvents  map[string]uint64
	latencyCount map[string]uint64
	latencySum   map[string]float64
	latencyBins  map[string][]uint64
}

// New creates a new Collector.
func New() *Collector {
	return &Collector{
		requests:     make(map[string]uint64),
		errors:       make(map[string]uint64),
		cacheEvents:  make(map[string]uint64),
		latencyCount: make(map[string]uint64),
		latencySum:   make(map[string]float64),
		latencyBins:  make(map[string][]uint64),
	}
}

// RecordHTTPRequest records request counters and latency buckets.
func RecordHTTPRequest(method string, route string, status int, duration time.Duration) {
	defaultCollector.RecordHTTPRequest(method, route, status, duration)
}

// RecordCacheEvent records cache activity.
func RecordCacheEvent(operation string, target string, backend string) {
	defaultCollector.RecordCacheEvent(operation, target, backend)
}

// Handler exposes Prometheus text metrics.
func Handler(w http.ResponseWriter, _ *http.Request) {
	defaultCollector.ServeHTTP(w)
}

// RecordHTTPRequest records request counters and latency buckets.
func (c *Collector) RecordHTTPRequest(method string, route string, status int, duration time.Duration) {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = "UNKNOWN"
	}
	route = normalizeRoute(route)
	statusLabel := strconv.Itoa(status)
	latencyKey := method + "|" + route
	requestKey := method + "|" + route + "|" + statusLabel

	c.mu.Lock()
	c.requests[requestKey]++
	if status >= http.StatusBadRequest {
		c.errors[requestKey]++
	}
	c.latencyCount[latencyKey]++
	seconds := duration.Seconds()
	c.latencySum[latencyKey] += seconds
	if _, ok := c.latencyBins[latencyKey]; !ok {
		c.latencyBins[latencyKey] = make([]uint64, len(durationBuckets))
	}
	for i, bucket := range durationBuckets {
		if seconds <= bucket {
			c.latencyBins[latencyKey][i]++
		}
	}
	c.mu.Unlock()
}

// RecordCacheEvent records cache activity.
func (c *Collector) RecordCacheEvent(operation string, target string, backend string) {
	operation = strings.ToLower(strings.TrimSpace(operation))
	target = strings.TrimSpace(target)
	backend = strings.TrimSpace(backend)
	if operation == "" {
		operation = "unknown"
	}
	if target == "" {
		target = "unknown"
	}
	if backend == "" {
		backend = "unknown"
	}

	c.mu.Lock()
	c.cacheEvents[operation+"|"+target+"|"+backend]++
	c.mu.Unlock()
}

// ServeHTTP writes metrics in Prometheus exposition format.
func (c *Collector) ServeHTTP(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	c.mu.RLock()
	defer c.mu.RUnlock()

	var lines []string

	lines = append(lines,
		"# HELP research_api_requests_total Total HTTP requests by route, method, and status.",
		"# TYPE research_api_requests_total counter",
	)
	requestKeys := sortedKeys(c.requests)
	for _, key := range requestKeys {
		method, route, status := split3(key)
		lines = append(lines, fmt.Sprintf(
			`research_api_requests_total{method=%q,route=%q,status=%q} %d`,
			method, route, status, c.requests[key],
		))
	}

	lines = append(lines,
		"# HELP research_api_request_errors_total Total HTTP error responses by route, method, and status.",
		"# TYPE research_api_request_errors_total counter",
	)
	errorKeys := sortedKeys(c.errors)
	for _, key := range errorKeys {
		method, route, status := split3(key)
		lines = append(lines, fmt.Sprintf(
			`research_api_request_errors_total{method=%q,route=%q,status=%q} %d`,
			method, route, status, c.errors[key],
		))
	}

	lines = append(lines,
		"# HELP research_api_request_duration_seconds HTTP request duration buckets.",
		"# TYPE research_api_request_duration_seconds histogram",
	)
	latencyKeys := sortedKeys(c.latencyCount)
	for _, key := range latencyKeys {
		method, route, _ := split3(key + "|")
		var cumulative uint64
		for i, bucket := range durationBuckets {
			cumulative += c.latencyBins[key][i]
			lines = append(lines, fmt.Sprintf(
				`research_api_request_duration_seconds_bucket{method=%q,route=%q,le=%q} %d`,
				method, route, trimFloat(bucket), cumulative,
			))
		}
		lines = append(lines, fmt.Sprintf(
			`research_api_request_duration_seconds_bucket{method=%q,route=%q,le="+Inf"} %d`,
			method, route, c.latencyCount[key],
		))
		lines = append(lines, fmt.Sprintf(
			`research_api_request_duration_seconds_sum{method=%q,route=%q} %s`,
			method, route, trimFloat(c.latencySum[key]),
		))
		lines = append(lines, fmt.Sprintf(
			`research_api_request_duration_seconds_count{method=%q,route=%q} %d`,
			method, route, c.latencyCount[key],
		))
	}

	lines = append(lines,
		"# HELP research_api_cache_events_total Cache operations by target and backend.",
		"# TYPE research_api_cache_events_total counter",
	)
	cacheKeys := sortedKeys(c.cacheEvents)
	for _, key := range cacheKeys {
		operation, target, backend := split3(key)
		lines = append(lines, fmt.Sprintf(
			`research_api_cache_events_total{operation=%q,target=%q,backend=%q} %d`,
			operation, target, backend, c.cacheEvents[key],
		))
	}

	_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
}

func normalizeRoute(route string) string {
	route = strings.TrimSpace(route)
	if route == "" {
		return "unmatched"
	}
	return route
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func split3(value string) (string, string, string) {
	parts := strings.SplitN(value, "|", 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}

func trimFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
