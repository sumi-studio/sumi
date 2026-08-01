# Multi-stage Dockerfile for the Sumi Go API (control plane + WebSocket gateway).
# The web frontend is served separately by the Vite dev server; this image only
# runs the API.
FROM golang:1.23-bookworm AS build
WORKDIR /src/apps/api
COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download
COPY apps/api/ ./
RUN CGO_ENABLED=0 go build -o /usr/local/bin/sumi-api ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=build /usr/local/bin/sumi-api /usr/local/bin/sumi-api
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sumi-api"]
