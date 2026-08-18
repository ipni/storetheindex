FROM golang:1.26-bookworm as builder

WORKDIR /storetheindex
COPY go.* .
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 go build
# CGO_ENABLED differs from the line above, so this step shares no build cache and recompiles the dependency graph.
RUN CGO_ENABLED=0 go build -o cross-announce ./scripts/cross_announce

# Debug non-root image used as base in order to provide easier administration and debugging.
FROM gcr.io/distroless/cc:debug-nonroot
COPY --from=builder /storetheindex/storetheindex /usr/local/bin/
COPY --from=builder /storetheindex/cross-announce /usr/local/bin/

# Default port configuration:
#  - 3000 Finder interface
#  - 3001 Ingest interface
#  - 3002 Admin interface
#  - 3003 libp2p interface
# Note: exposed ports below will have no effect if the default config is overridden.
EXPOSE 3000-3003

# The image also contains /usr/local/bin/cross-announce. Run it by overriding the entrypoint or command.
ENTRYPOINT ["/usr/local/bin/storetheindex"]
CMD ["daemon"]
