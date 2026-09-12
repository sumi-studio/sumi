# Sumi alpha on Workers

The `alpha` Wrangler environment serves the frontend at
`https://sumi-alpha.pdhaku0.workers.dev`. API, agent execution, and storage remain
on the operator's WSL machine. WSL must be running for those features to work.
A custom domain can be added later; also update authentication origins,
Firebase authorized domains, and browser notification registrations when changing
origins.

## Connection

`SUMI_ORIGIN` is a **VPC Service** binding to the dedicated `sumi-alpha-api`
service. Its destination is fixed to the WSL API at `100.116.25.99:8080`, through
`sumi-alpha-wsl`. It is not a VPC Network binding or a public forward proxy.
Only the browser API routes in `cloudflare/route-policy.ts` reach the service.
Internal agent endpoints and development source paths stay blocked.

The browser uses HTTPS. The private API hop uses HTTP, retains the public Host
and Origin, and replaces client-supplied proxy headers. Redirects are not followed
by the proxy. Streaming and WebSocket responses pass through without buffering.

On this host, the dedicated `sumi-alpha-tunnel.service` user service starts with
WSL's user service manager (lingering is enabled). Its token is stored outside the
repository with restricted permissions. Do not put tunnel tokens into Wrangler
configuration, documentation, or logs.

## Release checks

1. Build a verified revision with its Firebase public configuration and exact
   `SUMI_RELEASE_SHA`; use its generated `release.json` to identify the assets.
2. Configure the exact public origin in the API's browser-origin allowlist and
   Firebase's authorized domains. Ensure production session cookies are Secure.
3. Run the edge contract, artifact, and runtime checks, then deploy using the
   `alpha` environment and the verified static assets directory.
4. Verify actual Tunnel/VPC traffic: authentication, CSRF-protected writes,
   WebSocket conversation delivery, and reconnect. A static page or local
   service-binding test alone does not establish an operational deployment.

The original Wrangler environment remains available for installations using an
owned domain with a Worker Route. The alpha environment does not require buying
a domain.
