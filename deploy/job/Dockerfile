# sumi-job — the pinned image Cloud subprocess jobs run in.
#
# Purpose: one ephemeral container per job, launched by the root runtime
# provisioner with the fixed hardened spec in
# apps/api/internal/runtimeprovision/process_docker.go (read-only rootfs,
# --network none, uid 10002, no caps, no-new-privileges, resource limits,
# tmpfs /tmp). The job's only writable persistent surface is /workspace —
# the persona's canonical files scope, bind-mounted after mount/volume/
# binding verification.
#
# Toolchain contract (verified by the jobexec e2e and the image check
# script): POSIX shell + coreutils + timeout for plain commands, and a
# real build path — GNU make, gcc, python3, pip, and curl — so a job can
# compile and run a small C program or a Python utility, and fetch or
# install a normal public dependency.
#
# Networking: the container itself still gets no NIC (--network none).
# When the provisioner enables job egress (SUMI_JOB_EGRESS_DIR), the
# launch mounts the egress socket directory at /run/sumi/egress and the
# wrapper starts sumi-egress-bridge, a loopback proxy endpoint that
# splices onto that unix socket. The deployment-side proxy allows only
# public TCP :443/:80 destinations. PIP_BREAK_SYSTEM_PACKAGES is set
# because the image is a single-purpose ephemeral toolchain — there is no
# system Python environment to protect, and pip needs the flag on Debian
# to honor --user/--target installs into the workspace.
FROM golang:1.23-bookworm AS bridge
WORKDIR /src/apps/api
COPY apps/api/go.mod apps/api/go.sum ./
RUN go mod download
COPY apps/api/ ./
RUN CGO_ENABLED=0 go build -o /usr/local/bin/sumi-egress-bridge ./cmd/egress-bridge

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       bash coreutils util-linux findutils grep sed gawk \
       make gcc libc6-dev \
       python3 python3-pip \
       curl git \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10002 sumi-executor \
    && useradd --uid 10002 --gid 10002 --no-create-home --shell /usr/sbin/nologin sumi-executor \
    && install -d -m 0777 /workspace /tmp \
    && install -d -m 0755 /run/sumi/egress

COPY --from=bridge /usr/local/bin/sumi-egress-bridge /usr/local/bin/sumi-egress-bridge

# Login shells (the shared terminal runs `bash -l`) reset PATH in
# /etc/profile, which would drop the egress user-site bin dir the
# provisioner puts on the container PATH. This profile.d snippet restores
# it — only while the egress mount is present — so `pip --user` console
# scripts stay runnable by name across terminal sessions.
COPY deploy/job/sumi-egress-path.sh /etc/profile.d/sumi-egress-path.sh

ENV PIP_BREAK_SYSTEM_PACKAGES=1

# No ENTRYPOINT — the provisioner supplies --entrypoint /bin/bash and the
# marker/timeout/argv line. /bin/bash must exist for the launch contract.
USER 10002:10002
WORKDIR /workspace
CMD ["/bin/bash"]
