FROM golang:1.21-bullseye as builder
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
RUN cd assigner && \
    go build -ldflags "-X 'github.com/ipni/storetheindex/internal/version.GitVersion=$(git rev-list -1 HEAD)'"

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
FROM busybox:1.34-glibc
LABEL maintainer="Andrew Gillis <andrew.gillis@protocol.ai>"

# Get the storetheindex binary, entrypoint script, and TLS CAs from the build container.
ENV SRC_DIR /assigner
COPY --from=builder storetheindex/assigner/assigner /usr/local/bin/assigner
COPY --from=builder storetheindex/assigner/scripts/start_assigner /usr/local/bin/start_assigner
COPY --from=builder /tmp/su-exec/su-exec-static /sbin/su-exec
COPY --from=builder /tmp/tini /sbin/tini
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

# Set permissions on assigner
RUN chmod 0755 /usr/local/bin/assigner

# This shared lib (part of glibc) doesn't seem to be included with busybox.
COPY --from=builder /lib/*-linux-gnu*/libdl.so.2 /lib/

# P2P interface
EXPOSE 3000
# HTTP interface
EXPOSE 3001

ENV \
    ASSIGNER_PATH="/data/assigner" \
    GOLOG_LOG_FMT="json"

# Create the repo directory and switch to a non-privileged user.
RUN mkdir -p $ASSIGNER_PATH \
    && adduser -D -h $ASSIGNER_PATH -u 1000 -G users storetheindex \
    && chown storetheindex:users $ASSIGNER_PATH

# Expose the repo as a volume.
# start_assigner initializes a repo if none is mounted.
# Important this happens after the USER directive so permissions are correct.
VOLUME $ASSIGNER_PATH

# This initializes the storetheindex repo if one does not already exist
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/start_assigner"]

# Execute the daemon subcommand by default
CMD ["daemon"]
