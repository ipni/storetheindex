FROM golang:1.25-rc-bullseye as builder

WORKDIR /storetheindex
COPY go.* .
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 go build

# Debug non-root image used as base in order to provide easier administration and debugging.
FROM gcr.io/distroless/cc:debug-nonroot
COPY --from=builder /storetheindex/storetheindex /usr/local/bin/

# Default port configuration:
#  - 3000 Finder interface
#  - 3001 Ingest interface
#  - 3002 Admin interface
#  - 3003 libp2p interface
# Note: exposed ports below will have no effect if the default config is overridden.
EXPOSE 3000-3003

ENTRYPOINT ["/usr/local/bin/storetheindex"]
CMD ["daemon"]
