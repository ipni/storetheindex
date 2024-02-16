BIN := storetheindex

.PHONY: all build clean test

all: vet test build

build:
	go build

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
