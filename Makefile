BIN := storetheindex

.PHONY: all build clean test

all: build

build: $(BIN)

docker: Dockerfile clean
	docker build . --force-rm -f Dockerfile -t storetheindex:$(shell git rev-parse --short HEAD)

$(BIN): vet test
	go build -o $@

lint:
	golangci-lint run

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -f $(BIN)
	go clean
