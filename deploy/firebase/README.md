# Firebase Auth emulator

This is a standalone, durable Firebase Auth emulator for Sumi development.
It exposes only Auth on port `9099`; all users live in the named Docker volume
and are re-imported after the service is restarted.

Start it for the Tailnet development host:

```sh
FIREBASE_AUTH_EMULATOR_BIND_HOST=100.116.25.99 \
  FIREBASE_PROJECT_ID=sumi-studio \
  docker compose -f deploy/firebase/compose.yaml up -d
```

Compose pulls `ghcr.io/sumi-studio/sumi-firebase:latest` on every start. Set
`SUMI_FIREBASE_IMAGE_TAG` to a Jenkins-published short commit SHA to pin a
specific image. Authenticate with `docker login ghcr.io` first when the package
is private.

The bind host is deliberately an exact IP address. It defaults to `127.0.0.1`
for local-only use; use the WSL host's Tailnet IP for team access. It is the
only published port, so no Tailnet ACL addition beyond `9099` is required.

Point the Sumi API and browser at the same host:

```sh
export FIREBASE_AUTH_EMULATOR_HOST=100.116.25.99:9099
export VITE_FIREBASE_AUTH_EMULATOR_URL=http://100.116.25.99:9099
```

The Firebase web config's project ID and `FIREBASE_PROJECT_ID` must equal
`SUMI_AUTH_FIREBASE_PROJECT_ID` (normally `sumi-studio`). There are no
credentials in this service.

Stop without losing identities:

```sh
docker compose -f deploy/firebase/compose.yaml down
```

Do not append `-v` unless intentionally deleting all emulator identities.

For Compose integration, add the `firebase-auth` service and
`firebase-auth-data` volume from [`compose.yaml`](./compose.yaml) to the
owning development Compose file. The API service then needs
`FIREBASE_AUTH_EMULATOR_HOST=firebase-auth:9099` inside that Compose network;
the browser must keep using the published Tailnet address.

Run the lifecycle verification (it uses a temporary, isolated Compose project
and removes its own volume):

```sh
bash scripts/dev/firebase-auth-emulator-check
```
