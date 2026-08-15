FROM rust:1.88-bookworm AS build
WORKDIR /src/apps/agent

COPY apps/agent/Cargo.toml apps/agent/Cargo.lock ./
COPY apps/agent/src ./src
COPY apps/agent/migrations ./migrations
COPY apps/agent/prompts ./prompts
RUN cargo build --release --locked

FROM debian:bookworm-slim
# Docker cannot create bind/secret targets inside a read-only rootfs. The real
# PAID socket and four activation secret files mount over inert placeholders.
RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10000 sumi-supervisor \
    && useradd --uid 10000 --gid 10000 --no-create-home --shell /usr/sbin/nologin sumi-supervisor \
    && groupadd --gid 10001 sumi-runtime \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin sumi-runtime \
    && groupadd --gid 10002 sumi-executor \
    && useradd --uid 10002 --gid 10002 --no-create-home --shell /usr/sbin/nologin sumi-executor \
    && groupadd --gid 10003 sumi-broker \
    && useradd --uid 10003 --gid 10003 --no-create-home --shell /usr/sbin/nologin sumi-broker \
    && groupadd --gid 10020 sumi-executor-ipc \
    && groupadd --gid 10021 sumi-broker-ipc \
    && groupadd --gid 10022 sumi-local-control-ipc \
    && install -d -m 0755 /var/lib/sumi /var/lib/sumi-artifacts /workspace \
       /run/sumi /run/sumi/local-control /run/sumi/identity \
       /run/sumi/executor /run/sumi/broker /run/secrets \
    && install -d -m 0700 /var/lib/sumi-allocator-root \
       /var/lib/sumi-allocator-root/state \
       /var/lib/sumi-allocator-root/identity-output \
       /var/lib/sumi-allocator-root/identity-output/runtime \
       /var/lib/sumi-allocator-root/identity-output/executor \
       /var/lib/sumi-allocator-root/identity-output/broker \
    && for path in \
       /run/sumi/local-control/control.sock \
       /run/secrets/sumi_local_control_bearer \
       /run/secrets/sumi_agent_wrapping_key \
       /run/secrets/sumi_approval_secret_digest_key \
       /run/secrets/sumi_provider_api_key \
       /run/secrets/sumi_execution_reviewer_api_key \
       /run/secrets/sumi_escalation_reviewer_api_key; do \
         install -m 000 /dev/null "${path}"; \
       done

COPY --from=build /src/apps/agent/target/release/sumi-agent /usr/local/bin/sumi-agent
COPY deploy/agent/container-entrypoint /usr/local/bin/sumi-entrypoint
RUN chmod 0755 /usr/local/bin/sumi-agent /usr/local/bin/sumi-entrypoint

USER 10001:10001
ENTRYPOINT ["/usr/local/bin/sumi-entrypoint"]
