# MCP App sandbox deployment contract

`mcp-app-sandbox.html` is inert source for a future MCP Apps renderer. Do not
serve it from the Sumi web origin. Rendering must remain unavailable until both
the authenticated backend projection and this separate-origin deployment
exist.

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

The broad source directives are required because `srcdoc` inherits and
intersects the outer document's policy. The proxy injects the restrictive
resource-specific policy before the View HTML is parsed. Do not narrow the
outer source directives per application, accept CSP through query parameters,
or omit the inner policy; each would either break declared resource domains or
create a policy-selection trust bug. `frame-ancestors` must name the exact Sumi
host origin and is enforced only from the HTTP response header.

Also send:

```text
Cache-Control: no-store
Referrer-Policy: no-referrer
X-Content-Type-Options: nosniff
```

The Sumi host must embed the outer iframe with
`sandbox="allow-scripts allow-same-origin"`, an exact-origin postMessage
transport, and `credentialless`. The proxy creates an inner iframe with
`sandbox="allow-scripts"` and a denied Permission Policy. These are two
distinct isolation boundaries and must not be collapsed.
