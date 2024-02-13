BIN := assigner

.PHONY: all build clean test

all: vet test build

build:
	go build -ldflags "-X 'github.com/ipni/storetheindex/internal/version.GitVersion=$(git rev-list -1 HEAD)'"

install:
	go install

lint:
	golangci-lint run

test:
	go test ./...

vet:
	go vet ./...

clean:
	go clean
