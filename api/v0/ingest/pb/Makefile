PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

all: $(GO)

%.pb.go: %.proto
	protoc --proto_path=$(GOPATH)/pkg/mod:. --proto_path=/usr/include --gogofaster_out=. $<

clean:
	rm -f *.pb.go
	rm -f *.go
