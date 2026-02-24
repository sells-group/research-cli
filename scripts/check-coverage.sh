#!/usr/bin/env bash
# check-coverage.sh — Run tests with coverage and check against threshold.
#
# Usage:
#   ./scripts/check-coverage.sh              # default 50% threshold
#   ./scripts/check-coverage.sh 60           # custom threshold
#   COVERAGE_THRESHOLD=70 ./scripts/check-coverage.sh

set -euo pipefail

THRESHOLD="${1:-${COVERAGE_THRESHOLD:-50}}"
PROFILE="coverage.out"

echo "Running tests with coverage..."
go test -coverprofile="$PROFILE" -covermode=atomic ./...

echo ""
echo "=== Coverage by package ==="
go tool cover -func="$PROFILE" | grep -v '0.0%'

TOTAL=$(go tool cover -func="$PROFILE" | grep '^total:' | awk '{print $NF}' | tr -d '%')
echo ""
echo "Total coverage: ${TOTAL}%"
echo "Threshold:      ${THRESHOLD}%"

if [ "$(echo "${TOTAL} < ${THRESHOLD}" | bc -l)" -eq 1 ]; then
  echo "FAIL: coverage ${TOTAL}% is below threshold ${THRESHOLD}%"
  exit 1
fi

echo "PASS: coverage ${TOTAL}% meets threshold ${THRESHOLD}%"
