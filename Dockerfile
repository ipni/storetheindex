FROM golang:1.16-stretch as builder
LABEL maintainer="Andrew Gillis <andrew.gillis@protocol.ai>"

# Install deps
RUN apt-get update && apt-get install -y \
    ca-certificates

ENV SRC_DIR /storetheindex

RUN mkdir /storetheindex
WORKDIR /storetheindex

# Download packages first so they can be cached.
COPY go.mod go.sum /storetheindex/
RUN go mod download
COPY . .

# Build the executable
RUN go build

# Get su-exec, a very minimal tool for dropping privileges,
# and tini, a very minimal init daemon for containers
ENV \
    SUEXEC_VERSION=v0.2 \
    TINI_VERSION=v0.19.0
    
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        "amd64" | "armhf" | "arm64") tiniArch="tini-static-$dpkgArch" ;;\
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    cd /tmp \
    && git clone https://github.com/ncopa/su-exec.git \
    && cd su-exec \
    && git checkout -q $SUEXEC_VERSION \
    && make su-exec-static \
    && cd /tmp \
    && wget -q -O tini https://github.com/krallin/tini/releases/download/$TINI_VERSION/$tiniArch \
    && chmod +x tini

# Create the target image
FROM busybox:1.31-glibc
LABEL maintainer="Andrew Gillis <andrew.gillis@protocol.ai>"

# Get the storetheindex binary, entrypoint script, and TLS CAs from the build container.
ENV SRC_DIR /storetheindex
COPY --from=builder storetheindex/storetheindex /usr/local/bin/storetheindex
COPY --from=builder storetheindex/scripts/start_storetheindex /usr/local/bin/start_storetheindex
COPY --from=builder /tmp/su-exec/su-exec-static /sbin/su-exec
COPY --from=builder /tmp/tini /sbin/tini
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

# Set permissions on storetheindex
RUN chmod 0755 /usr/local/bin/storetheindex

# This shared lib (part of glibc) doesn't seem to be included with busybox.
COPY --from=builder /lib/*-linux-gnu*/libdl.so.2 /lib/

# Admin interface
EXPOSE 3002
# Finder interface
EXPOSE 3000
# Ingest interface
EXPOSE 3001

ENV \
    STORETHEINDEX_LOTUS_GATEWAY="wss://api.chain.love" \
    STORETHEINDEX_PATH="/data/storetheindex"

# Create the repo directory and switch to a non-privileged user.
RUN mkdir -p $STORETHEINDEX_PATH \
    && adduser -D -h $STORETHEINDEX_PATH -u 1000 -G users storetheindex \
    && chown storetheindex:users $STORETHEINDEX_PATH

# Expose the repo as a volume.
# start_storetheindex initializes a repo if none is mounted.
# Important this happens after the USER directive so permissions are correct.
VOLUME $STORETHEINDEX_PATH

# This initializes the storetheindex repo if one does not already exist
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/start_storetheindex"]

# Execute the daemon subcommand by default
CMD ["daemon"]
