.PHONY: default

default: init

init:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.2
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.39.0

linter:
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

lint:
	golangci-lint run -c .golangci.yml --timeout=5m -v

test:
	go test ./... -bench . -benchmem

tidy:
	go mod tidy