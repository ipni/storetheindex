BIN := storetheindex

.PHONY: all assigner build clean test

all: vet test build

assigner:
	@make -C assigner

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
	@make -C assigner clean
