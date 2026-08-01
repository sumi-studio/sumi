# Multi-stage Dockerfile for the Sumi Go API (control plane + WebSocket gateway)
# and the migration helper. The debug tag includes busybox for healthchecks.
FROM golang:1.23-bookworm AS build
WORKDIR /src/apps/api
COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download
COPY apps/api/ ./
RUN CGO_ENABLED=0 go build -o /usr/local/bin/sumi-api ./cmd/server && \
    CGO_ENABLED=0 go build -o /usr/local/bin/sumi-migrate ./cmd/migrate

FROM gcr.io/distroless/static-debian12:debug
COPY --from=build /usr/local/bin/sumi-api /usr/local/bin/sumi-api
COPY --from=build /usr/local/bin/sumi-migrate /usr/local/bin/sumi-migrate
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sumi-api"]
