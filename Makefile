.PHONY: build test test-coverage test-integration lint fix mocks clean

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
	golangci-lint run

fix:
	go fix ./...

mocks:
	mockery

clean:
	rm -f research-cli coverage.out
