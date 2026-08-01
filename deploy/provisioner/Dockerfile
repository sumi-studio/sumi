FROM golang:1.23-bookworm AS build
WORKDIR /src/apps/api
COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download
COPY apps/api/ ./
RUN CGO_ENABLED=0 go build -o /usr/local/bin/sumi-runtime-provisioner ./cmd/runtime-provisioner

FROM docker:27-cli
RUN apk add --no-cache bash coreutils util-linux
WORKDIR /opt/sumi
COPY . /opt/sumi
COPY --from=build /usr/local/bin/sumi-runtime-provisioner /usr/local/bin/sumi-runtime-provisioner
RUN chmod 0755 /usr/local/bin/sumi-runtime-provisioner /opt/sumi/deploy/agent/supervisor
ENTRYPOINT ["/usr/local/bin/sumi-runtime-provisioner"]
CMD ["-socket", "/run/sumi/runtime-provisioner/control.sock", "-socket-gid", "20000", "-supervisor", "/opt/sumi/deploy/agent/supervisor"]
