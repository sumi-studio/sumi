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
# real build path — GNU make, gcc, and python3 — so a job can compile and
# run a small C program or a Python utility. There is deliberately no
# package manager network path (jobs run with --network none), no cargo,
# and no node/npm.
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       bash coreutils util-linux findutils grep sed gawk \
       make gcc libc6-dev \
       python3 \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10002 sumi-executor \
    && useradd --uid 10002 --gid 10002 --no-create-home --shell /usr/sbin/nologin sumi-executor \
    && install -d -m 0777 /workspace /tmp

# No ENTRYPOINT — the provisioner supplies --entrypoint /bin/bash and the
# marker/timeout/argv line. /bin/bash must exist for the launch contract.
USER 10002:10002
WORKDIR /workspace
CMD ["/bin/bash"]
