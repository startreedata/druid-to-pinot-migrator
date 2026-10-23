# Locally-built MinIO client (`mc`) image for the live Docker stack.
#
# Companion to Dockerfile.minio — the one-shot `minio-init` job uses `mc` to
# create the `dpm-test` bucket before the Pinot controller comes up. The
# pinned `minio/mc:RELEASE.2025-04-16T18-13-26Z` tag is likewise no longer
# anonymously pullable from Docker Hub (issue #69), so we rebuild it from
# MinIO's official source at its tagged commit.
#
# Pinned source
# ─────────────
#   minio/mc  RELEASE.2025-04-16T18-13-26Z
#   commit    b00526b153a31b36767991a4f5ce2cced435ee8e

# ── Build stage: compile the mc binary from pinned source ─────────────────
# go.mod at this commit requires `go 1.23.0` (toolchain go1.23.6); the 1.24
# toolchain is a backward-compatible superset, so GOTOOLCHAIN=local builds
# it deterministically with no mid-build toolchain download.
FROM golang:1.24-bookworm AS build
ENV GOTOOLCHAIN=local

ARG MC_COMMIT=b00526b153a31b36767991a4f5ce2cced435ee8e
ARG MC_RELEASE=RELEASE.2025-04-16T18-13-26Z

WORKDIR /src
# --checksum verifies the fetched archive against the pinned SHA256 (see the
# companion note in Dockerfile.minio). Recompute with:
#   curl -sSL https://github.com/minio/mc/archive/<commit>.tar.gz | sha256sum
ADD --checksum=sha256:4cd13e34daeeb8481c3ba8686b082f161b8dc1f7aad52d715a706a587349c6ae \
    https://github.com/minio/mc/archive/${MC_COMMIT}.tar.gz /tmp/mc.tar.gz
RUN tar -xzf /tmp/mc.tar.gz --strip-components=1 -C /src && rm /tmp/mc.tar.gz

# Mirror the upstream Makefile `build` target (CGO off, -tags kqueue,
# -trimpath) with release-matching version ldflags.
RUN CGO_ENABLED=0 go build -tags kqueue -trimpath \
      -ldflags "-s -w \
        -X github.com/minio/mc/cmd.Version=${MC_RELEASE} \
        -X github.com/minio/mc/cmd.ReleaseTag=${MC_RELEASE} \
        -X github.com/minio/mc/cmd.CommitID=${MC_COMMIT}" \
      -o /out/mc .

# ── Runtime stage: slim Debian with /bin/sh (the init job runs an sh -c) ──
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/mc /usr/bin/mc

ENTRYPOINT ["/usr/bin/mc"]
