# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build

WORKDIR /src/apps/api
COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download
COPY apps/api/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sumi-api ./cmd/server \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sumi-migrate ./cmd/migrate

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S sumi \
    && adduser -S -G sumi sumi \
    && install -d -o sumi -g sumi /var/lib/sumi/command-log /var/lib/sumi/agent-runtime

COPY --from=build /out/sumi-api /usr/local/bin/sumi-api
COPY --from=build /out/sumi-migrate /usr/local/bin/sumi-migrate

USER sumi
EXPOSE 8080
CMD ["/usr/local/bin/sumi-api"]
