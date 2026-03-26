# Research Platform — Development Commands
# Install just: brew install just
# Usage: just dev

set dotenv-load
set positional-arguments

node_path     := "/Users/blake/Library/Application Support/Zed/node/node-v24.11.0-darwin-arm64/bin"
podman        := "/opt/podman/bin/podman"
air           := env("GOPATH", "/Users/blake/go") + "/bin/air"
compose       := podman + " compose"
local_pg      := "postgres://research:research@localhost:5432/research?sslmode=disable"

# Env vars exported into every recipe.
# Database URLs come from config.dev.yaml (Neon) — no local override needed.
export RESEARCH_STORE_DRIVER          := "postgres"
export RESEARCH_LOG_LEVEL             := "debug"
export RESEARCH_LOG_FORMAT            := "console"

# ─── Main Commands ────────────────────────────────────────────────────

# Start full local dev environment (postgres + air + vite) with auto port detection
dev: _ensure-tools
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="{{ node_path }}:$PATH"

    # ── Find available ports ──────────────────────────────────────────
    find_port() {
        local port=$1
        while lsof -iTCP:"$port" -sTCP:LISTEN &>/dev/null; do
            port=$((port + 1))
        done
        echo "$port"
    }

    API_PORT=$(find_port 8080)
    VITE_PORT=$(find_port 5173)

    # ── Wire ports into env ───────────────────────────────────────────
    export RESEARCH_SERVER_PORT="$API_PORT"
    export API_PORT="$API_PORT"

    cleanup() {
        echo ""
        echo "Shutting down..."
        [ -n "${AIR_PID:-}" ]  && kill "$AIR_PID"  2>/dev/null && wait "$AIR_PID"  2>/dev/null
        [ -n "${VITE_PID:-}" ] && kill "$VITE_PID" 2>/dev/null && wait "$VITE_PID" 2>/dev/null
        echo "Done."
    }
    trap cleanup EXIT INT TERM

    # 1. Run migrations against Neon (main schema + fedsync)
    echo "==> Running migrations..."
    go run -tags=integration ./cmd migrate 2>&1 || true
    go run -tags=integration ./cmd fedsync migrate 2>&1 || true

    # 3. Start Go server with air (hot reload, port from RESEARCH_SERVER_PORT)
    echo "==> Starting Go server (air) on port ${API_PORT}..."
    {{ air }} &
    AIR_PID=$!

    # 4. Wait for Go server to be healthy before starting vite
    echo -n "Waiting for API server on port ${API_PORT}"
    for i in $(seq 1 60); do
        if curl -sf "http://localhost:${API_PORT}/api/v1/health" >/dev/null 2>&1; then
            echo " ready!"
            break
        fi
        if ! kill -0 "$AIR_PID" 2>/dev/null; then
            echo " air process died!"
            exit 1
        fi
        echo -n "."
        sleep 1
    done
    if ! curl -sf "http://localhost:${API_PORT}/api/v1/health" >/dev/null 2>&1; then
        echo " timed out after 60s!"
        exit 1
    fi

    # 5. Start Vite dev server (proxies /api → Go server)
    echo "==> Starting Vite dev server on port ${VITE_PORT}..."
    cd frontend && npm run dev -- --host --port "$VITE_PORT" 2>&1 &
    VITE_PID=$!

    echo ""
    echo "┌──────────────────────────────────────────────┐"
    echo "│  Research Platform Dev Environment            │"
    echo "├──────────────────────────────────────────────┤"
    printf "│  Frontend:  %-33s│\n" "http://localhost:${VITE_PORT}"
    printf "│  API:       %-33s│\n" "http://localhost:${API_PORT}"
    printf "│  Database:  %-33s│\n" "Neon (production)"
    printf "│  Health:    %-33s│\n" "localhost:${API_PORT}/api/v1/health"
    echo "└──────────────────────────────────────────────┘"
    echo ""

    # 6. Open frontend in Edge
    sleep 2
    open -a "Microsoft Edge" "http://localhost:${VITE_PORT}" 2>/dev/null || true

    # Wait for either process to exit
    wait

# Start only infrastructure (postgres + temporal + docling)
infra:
    {{ compose }} up -d

# Start only postgres (default port 5432)
db:
    {{ compose }} up -d postgres
    just _wait-for-pg 5432

# Start Go server with hot reload (assumes postgres is running)
server: _ensure-air
    {{ air }}

# Start frontend dev server only
frontend:
    #!/usr/bin/env bash
    export PATH="{{ node_path }}:$PATH"
    cd frontend && npm run dev

# ─── Build ────────────────────────────────────────────────────────────

# Build Go binary
build:
    go build -tags=integration -o research-cli ./cmd

# Build frontend for production
build-frontend:
    #!/usr/bin/env bash
    export PATH="{{ node_path }}:$PATH"
    cd frontend && npm run build

# Build container image with podman
build-image:
    {{ podman }} build -t research-cli .

# ─── Test & Lint ──────────────────────────────────────────────────────

# Run all Go tests
test:
    go test ./... -race

# Run Go tests with coverage
test-coverage:
    go test ./... -race -coverprofile=coverage.out
    go tool cover -func=coverage.out

# Run integration tests (requires running postgres)
test-integration:
    go test -tags=integration ./... -race

# Check frontend types
check-frontend:
    #!/usr/bin/env bash
    export PATH="{{ node_path }}:$PATH"
    cd frontend && npm run check

# Run frontend tests
test-frontend:
    #!/usr/bin/env bash
    export PATH="{{ node_path }}:$PATH"
    cd frontend && npm run test

# Lint Go code
lint:
    ./scripts/lint.sh ./...

# Format repo files
fmt:
    treefmt

# Fix Go code
fix:
    go fix ./...

# Run all checks (Go tests + lint + frontend checks)
check-all: test lint check-frontend

# ─── Database ─────────────────────────────────────────────────────────

# Connect to local postgres via psql
psql:
    {{ podman }} exec -it research-cli-postgres-1 psql -U research -d research

# Run fedsync migrations against local postgres
migrate:
    go run -tags=integration ./cmd fedsync migrate

# Show fedsync sync status
sync-status:
    go run -tags=integration ./cmd fedsync status

# ─── Infrastructure ──────────────────────────────────────────────────

# Stop all infrastructure containers
down:
    {{ compose }} down

# Stop and remove all volumes (DESTROYS DATA)
nuke:
    {{ compose }} down -v

# Show container status
status:
    {{ compose }} ps

# Tail container logs
logs *ARGS:
    {{ compose }} logs {{ ARGS }}

# ─── Setup ────────────────────────────────────────────────────────────

# First-time setup: install all dependencies + tools
setup: _install-air
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="{{ node_path }}:$PATH"

    echo "==> Installing frontend dependencies..."
    cd frontend && npm ci
    cd ..

    echo "==> Installing pre-commit hook..."
    ln -sf ../../scripts/pre-commit .git/hooks/pre-commit

    echo ""
    echo "Setup complete! Next steps:"
    echo ""
    echo "  1. Ensure config.yaml exists with your API keys:"
    echo "     cp config.example.yaml config.yaml"
    echo ""
    echo "  2. Start the dev environment:"
    echo "     just dev"
    echo ""
    echo "  The justfile automatically overrides store.driver and"
    echo "  store.database_url to point at the local podman postgres."
    echo "  Your config.yaml API keys (Anthropic, Notion, etc.) are"
    echo "  still read from config.yaml as usual."

# Install frontend dependencies
install-frontend:
    #!/usr/bin/env bash
    export PATH="{{ node_path }}:$PATH"
    cd frontend && npm ci

# ─── Helpers (prefixed with _ = hidden from `just --list`) ───────────

# Wait for postgres to accept connections on given host port (max 30s)
_wait-for-pg port="5432":
    #!/usr/bin/env bash
    echo -n "Waiting for postgres on port {{ port }}"
    for i in $(seq 1 30); do
        if {{ podman }} exec research-cli-postgres-1 pg_isready -U research -q 2>/dev/null; then
            echo " ready!"
            exit 0
        fi
        echo -n "."
        sleep 1
    done
    echo " timed out!"
    exit 1

# Ensure air is installed
_ensure-air:
    #!/usr/bin/env bash
    if [ ! -f "{{ air }}" ]; then
        echo "Installing air (Go live-reload)..."
        go install github.com/air-verse/air@latest
    fi

# Install air unconditionally
_install-air:
    go install github.com/air-verse/air@latest

# Ensure all dev tools are available
_ensure-tools: _ensure-air
    #!/usr/bin/env bash
    export PATH="{{ node_path }}:$PATH"
    if ! command -v node &>/dev/null; then
        echo "ERROR: node not found at {{ node_path }}"
        echo "Install Node.js or update node_path in justfile."
        exit 1
    fi
    if [ ! -d "frontend/node_modules" ]; then
        echo "==> Installing frontend dependencies..."
        cd frontend && npm ci
    fi
