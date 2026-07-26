# T26 local/CI container harness. Cloud uses the same per-service mount and
# identity contract inside a tenant microVM; this image is not a substitute for
# the T27 cgroup/recovery supervisor.
FROM rust:1.88-bookworm AS build
WORKDIR /src/apps/agent

COPY apps/agent/Cargo.toml apps/agent/Cargo.lock ./
COPY apps/agent/src ./src
COPY apps/agent/migrations ./migrations
COPY apps/agent/prompts ./prompts
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 sumi-runtime \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin sumi-runtime \
    && groupadd --gid 10002 sumi-tool \
    && useradd --uid 10002 --gid 10002 --no-create-home --shell /usr/sbin/nologin sumi-tool \
    && groupadd --gid 10003 sumi-artifact \
    && useradd --uid 10003 --gid 10003 --no-create-home --shell /usr/sbin/nologin sumi-artifact \
    && groupadd --gid 10010 sumi-ipc \
    && install -d -m 0755 /workspace /var/lib/sumi /run/sumi

COPY --from=build /src/apps/agent/target/release/sumi-agent /usr/local/bin/sumi-agent
COPY deploy/agent/container-entrypoint /usr/local/bin/sumi-entrypoint
RUN chmod 0755 /usr/local/bin/sumi-agent /usr/local/bin/sumi-entrypoint

ENTRYPOINT ["/usr/local/bin/sumi-entrypoint"]
