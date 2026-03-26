.PHONY: build test test-coverage test-integration lint fmt fix mocks clean

build:
	go build -o research-cli ./cmd

test:
	go test ./... -race

test-coverage:
	go test ./... -race -coverprofile=coverage.out
	go tool cover -func=coverage.out

test-integration:
	go test -tags=integration ./... -race

lint:
	./scripts/lint.sh ./...

fmt:
	treefmt

fix:
	go fix ./...

mocks:
	mockery

clean:
	rm -f research-cli coverage.out
