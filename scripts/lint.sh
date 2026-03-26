#!/usr/bin/env bash

set -euo pipefail

readonly required_version_prefix="2.10."
readonly fallback_package="github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.0"

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/.." && pwd)

cd "${repo_root}"

if [ "$#" -eq 0 ]; then
  targets=(./...)
else
  targets=("$@")
fi

mapfile -t package_dirs < <(go list -f '{{.Dir}}' "${targets[@]}")
if [ "${#package_dirs[@]}" -eq 0 ]; then
  echo "lint: no packages found for targets: ${targets[*]}" >&2
  exit 1
fi

packages=()
for dir in "${package_dirs[@]}"; do
  if [ "${dir}" = "${repo_root}" ]; then
    packages+=(".")
    continue
  fi

  rel=".${dir#${repo_root}}"
  packages+=("${rel}")
done

version_matches() {
  local binary=$1
  local version

  if ! version=$("${binary}" version 2>/dev/null); then
    return 1
  fi

  [[ "${version}" == *"version ${required_version_prefix}"* ]]
}

resolve_linter() {
  if command -v golangci-lint >/dev/null 2>&1 && version_matches "$(command -v golangci-lint)"; then
    printf '%s\n' "$(command -v golangci-lint)"
    return 0
  fi

  if [ -x "${HOME}/go/bin/golangci-lint" ] && version_matches "${HOME}/go/bin/golangci-lint"; then
    printf '%s\n' "${HOME}/go/bin/golangci-lint"
    return 0
  fi

  printf '__fallback__\n'
}

linter=$(resolve_linter)
if [ "${linter}" = "__fallback__" ]; then
  echo "lint: using go run ${fallback_package}"
  go run "${fallback_package}" run --config .golangci.yml "${packages[@]}"
  exit 0
fi

echo "lint: using ${linter}"
"${linter}" run --config .golangci.yml "${packages[@]}"
