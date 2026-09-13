# Sumi alpha on Workers

The `alpha` Wrangler environment targets the frontend origin
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
   Firebase's authorized domains. Verify the HTTPS edge adds Secure to session cookies.
3. Run the edge contract, artifact, and runtime checks, then deploy using the
   `alpha` environment and the verified static assets directory.
4. Verify actual Tunnel/VPC traffic: authentication, CSRF-protected writes,
   WebSocket conversation delivery, and reconnect. A static page or local
   service-binding test alone does not establish an operational deployment.

The original Wrangler environment remains available for installations using an
owned domain with a Worker Route. The alpha environment does not require buying
a domain.

## Deployment command and origin configuration

Before publishing, append `https://sumi-alpha.pdhaku0.workers.dev` to
`SUMI_BROWSER_WS_ALLOWED_ORIGINS` without removing origins still in use. The
HTTPS VPC edge adds `Secure` to each origin cookie while preserving its other
attributes. It does not change the shared API cookie setting: local HTTP
development sessions can keep working. WebSocket 101 responses pass through
without reconstruction. Apply the origin allowlist through the normal API release
procedure. Add the hostname
`sumi-alpha.pdhaku0.workers.dev` to Firebase Authentication's authorized domains.
The browser establishes a new session for this origin; an existing Tailnet-origin
cookie is not transferred.

### Same-tab Firebase sign-in

The alpha Worker transparently proxies `/__/auth/` GET/POST requests to
`sumi-studio.firebaseapp.com`, selected by `SUMI_FIREBASE_AUTH_DOMAIN` in the
Worker environment. It strips Sumi credentials and disables caching. This is
Firebase's [same-origin redirect setup](https://firebase.google.com/docs/auth/web/redirect-best-practices#option-3-proxy-auth-requests-to-firebaseappcom):
the browser must stay on the application origin for the helper iframe to read
the redirect result when third-party storage is blocked.

Before switching the alpha frontend to redirect sign-in:

- Add `https://sumi-alpha.pdhaku0.workers.dev/__/auth/handler` to the Google
  OAuth client's authorized redirect URIs and the GitHub OAuth app's callback
  URLs. Keep existing callback URLs needed by other deployments.
- Build alpha assets with
  `VITE_FIREBASE_AUTH_DOMAIN=sumi-alpha.pdhaku0.workers.dev`. The other Firebase
  client values still describe project `sumi-studio`.
- Verify `/__/auth/handler` and `/__/auth/iframe` return Firebase helper HTML,
  with no SPA fallback or redirect to `firebaseapp.com`. Verify both providers
  accept the new callback; reaching a provider's login page alone does not
  establish a completed authentication round trip.

The application CSP permits its own helper iframe. The proxy preserves the
Firebase helper's response policy rather than applying the application's
`frame-ancestors 'none'` policy to it. Existing local development environments
still need an equivalent same-origin setup when using real redirect auth;
the auth emulator is independent of the hosted-helper proxy.

From `apps/web`, with an absolute path to verified assets:

```sh
node node_modules/wrangler/bin/wrangler.js deploy --env alpha --assets "$SUMI_VERIFIED_ASSETS" --dry-run
node node_modules/wrangler/bin/wrangler.js deploy --env alpha --assets "$SUMI_VERIFIED_ASSETS"
```

Confirm the dry-run lists the expected `SUMI_ORIGIN` VPC Service and assets before
running the second command. Keep the asset revision (`release.json`) and Worker
source revision in the release record when deploying them from different commits.
After deployment, verify `/release.json`, sign-in, a CSRF-protected write, and a
WebSocket conversation against the public origin. These checks require the real
WSL origin and cannot be replaced by the local service-binding test.
