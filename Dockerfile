FROM golang:1.26-bookworm AS builder

WORKDIR /storetheindex
COPY go.* .
RUN go mod download -x
COPY . .

RUN CGO_ENABLED=1 go build

FROM debian:bookworm-slim AS jemalloc
RUN apt-get update \
    && apt-get install -y --no-install-recommends libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

# Debug non-root image used as base in order to provide easier administration and debugging.
# distroless/cc includes libstdc++, which Debian jemalloc requires.
FROM gcr.io/distroless/cc:debug-nonroot
COPY --from=builder /storetheindex/storetheindex /usr/local/bin/
COPY --from=jemalloc \
    /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
    /usr/lib/x86_64-linux-gnu/
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000

# Default port configuration:
#  - 3000 Finder interface
#  - 3001 Ingest interface
#  - 3002 Admin interface
#  - 3003 libp2p interface
# Note: exposed ports below will have no effect if the default config is overridden.
EXPOSE 3000-3003

ENTRYPOINT ["/usr/local/bin/storetheindex"]
CMD ["daemon"]
