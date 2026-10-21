BIN := storetheindex

.PHONY: all build clean test cross-announce

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
	rm -f cross-announce

cross-announce:
	go build -o cross-announce ./scripts/cross_announce
