BIN := storetheindex

.PHONY: all build clean test

all: vet test build

build:
	go build -ldflags "-X 'github.com/filecoin-project/storetheindex/internal/version.GitVersion=$(git rev-list -1 HEAD)'"

docker: Dockerfile clean
	docker build . --force-rm -f Dockerfile -t storetheindex:$(shell git rev-parse --short HEAD)

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
