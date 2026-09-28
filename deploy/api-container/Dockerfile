# The Sumi Go API as a Cloudflare Container image (apps/api-container).
#
# The container disk is discarded whenever the instance stops, so the API runs
# with SUMI_API_JOURNAL_MIRROR=postgres: its journal directories are restored
# from PostgreSQL before it serves, and every acknowledged journal write is
# committed there. Build from the repository root:
#   docker build -f deploy/api-container/Dockerfile -t sumi-api-container .
FROM golang:1.23-bookworm AS build
WORKDIR /src/apps/api
COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download
COPY apps/api/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /usr/local/bin/sumi-api ./cmd/server

# The debug variant carries busybox for the entrypoint script only.
FROM gcr.io/distroless/static-debian12:debug
COPY --from=build /usr/local/bin/sumi-api /usr/local/bin/sumi-api
COPY deploy/api-container/entrypoint.sh /usr/local/bin/sumi-api-entrypoint
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/busybox/sh", "/usr/local/bin/sumi-api-entrypoint"]
