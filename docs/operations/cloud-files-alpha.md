# Canonical files for Cloud jobs and terminals

The files service and Linux job containers share one canonical JuiceFS volume. Each secretary uses the compact persona UUID as its scope directory. The Core accesses files through the API; Linux jobs and terminals bind that same scope at `/workspace`.

`deploy/files/sumi-files-format` formats an explicitly provisioned volume. Keep its volume UUID as operational configuration. `sumi-files-mount` mounts that volume, and `sumi-files-check` verifies the live mount and UUID before a service starts. The mount unit template is `deploy/files/systemd/sumi-files-mount@.service`; its `service` instance reads the user's `sumi-files/service.env`, and the root job host's `executor` instance reads `/etc/sumi-files/executor.env`. Install the scripts in the location configured by `SUMI_FILES_BIN`.

The service environment supplies the metadata URL, object-store credentials, mountpoint, cache path, and pinned volume UUID. Keep credential files private. The `executor` mount runs as root with access for container UID 10002; the files service runs under its configured service user. Both mounts use the same volume identity. Do not substitute an ordinary local directory when a mount is unavailable.

Apply `deploy/files/compose.provisioner-files.yaml` with the deployment's Compose configuration. It supplies `SUMI_FILES_MOUNTPOINT`, `SUMI_FILES_VOLUME_UUID`, and the check executable to the process provisioner, and shares the mount with `rslave` propagation. The provisioner checks the live volume, persona directory and durable binding before a new process starts. A missing, changed or mismatched scope fails that operation. Existing operation records remain readable when a mount is unavailable.

The job image is pinned through `SUMI_JOB_IMAGE_TAG`. Containers use the verified bind, an unprivileged UID, a read-only root, bounded resources and `--network none`; optional public egress goes through the separate proxy socket. There is no persistent secretary container or separate runtime workspace volume. See [Linux jobs](linux-on-demand-jobs.md) for process recovery and [Core architecture](../core-architecture.md) for the execution boundary.

For a full user-data reset, stop writers and remove the persona scopes and user objects along with their API rows, process journals and bindings. Preserve only the chosen operational volume configuration. Do not launch jobs against bindings from the previous user-data set.
