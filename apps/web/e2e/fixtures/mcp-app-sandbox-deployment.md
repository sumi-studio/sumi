# MCP App sandbox deployment contract

`mcp-app-sandbox.html` is an inert E2E fixture for a future MCP Apps renderer.
It is deliberately outside `public/` and must not be included in the Sumi web
artifact. Rendering remains unavailable until both the authenticated backend
projection and a separate-origin deployment exist.

Deploy the file at a dedicated HTTPS origin that serves no Sumi credentials,
application APIs, or user-controlled files. At build time, replace
`__SUMI_MCP_SANDBOX_DEPLOYMENT__` with the lowercase SHA-256 digest of the exact
deployed sandbox artifact. Configure the same digest as
`VITE_MCP_APP_SANDBOX_DEPLOYMENT_ID` and its absolute URL as
`VITE_MCP_APP_SANDBOX_URL`.

The sandbox response must be non-cacheable and send this header:

```text
Content-Security-Policy: default-src * data: blob:; script-src * data: blob: 'unsafe-inline'; style-src * data: blob: 'unsafe-inline'; img-src * data: blob:; font-src * data: blob:; media-src * data: blob:; connect-src https: wss:; frame-src *; worker-src * blob:; object-src 'none'; base-uri *; form-action 'none'; frame-ancestors https://<exact-sumi-host>
```

The broad source directives are required because the proxy does not receive the
resource CSP until after this response loads. For each resource, the proxy
creates a distinct nested policy-boundary document. Its CSP is inherited by the
View, is also injected before the raw View HTML, and uses `frame-src 'self'`
plus only the resource's declared frame origins. This outer resource boundary
therefore continues to limit replacement navigations after an inner `srcdoc`
policy would otherwise disappear. The boundary permanently removes bridge
trust on every replacement load or blocked direct-frame navigation.

Do not narrow the deployment directives per application, accept CSP through
query parameters, omit either resource policy, or forward messages directly
from the View. Those changes would break declared resource domains or bypass
the policy/trust boundary. `frame-ancestors` must name the exact Sumi host
origin and is enforced only from the HTTP response header.

Also send:

```text
Cache-Control: no-store
Referrer-Policy: no-referrer
X-Content-Type-Options: nosniff
```

The Sumi host must embed the outer iframe with
`sandbox="allow-scripts allow-same-origin"`, an exact-origin postMessage
transport, and `credentialless`. The proxy creates an inner iframe with
`sandbox="allow-scripts"` and a denied Permission Policy inside a second,
resource-specific policy-boundary iframe. The View remains the untrusted
execution boundary; the added wrapper is the per-resource CSP and
navigation-trust boundary. Do not collapse them.

This follows the MCP Apps 2026-01-26 sandbox-proxy requirements: the Host and
Sandbox remain different origins, raw resource HTML is delivered only after
the ready notification, declared domains are the CSP ceiling, and non-reserved
MCP JSON-RPC messages are transparently forwarded while the View remains on its
initial document.
