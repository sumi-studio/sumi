# Shared browser runtime

This is the first Electron browser runtime, not the complete Sumi desktop app.
It attaches a real `WebContentsView` to a host-supplied `BaseWindow`. A person
interacts with that view normally; a host-side caller reads and acts on that
same view through `BrowserTabPort`. There is no second automation tab, remote
debugging listener, renderer IPC bridge, or copied website login state.

The main-process implementation is `src/browser/runtime.ts`; the portable
TypeScript contract is `src/browser/contract.ts`. `apps/web` remains the product
renderer. `src/browser-dev.ts` only opens a plain engineering window.

## Run

Requires Node 22.18+, pnpm, and the platform libraries required by Electron.
Electron is pinned in this package and its download hook is enabled in the
workspace's `allowBuilds`. Install from the repository root:

```sh
pnpm --filter @sumi/desktop install --frozen-lockfile
SUMI_BROWSER_URL=https://example.com pnpm --filter @sumi/desktop dev:browser
```

For WSL without a display, prefix the second command with `xvfb-run -a` (or use
WSLg to interact with the visible window). Do not disable Chromium's sandbox.
The development entry uses profile `development` under Electron's user-data
directory. No existing Chrome profile or Sumi credential storage is reused.

## Host integration

After Electron is ready, a **trusted main-process host** can do:

```ts
const browser = new SharedBrowserRuntime({ profileId: "host-owned-stable-id" });
const window = new BaseWindow({ width: 1100, height: 800 });
const tab = await browser.openTab(window, "https://example.com");
const page = await browser.observe(tab);
// Select an actual target from page.targets before clicking/filling it.
await browser.act(tab, page.binding, { kind: "scroll", x: 0, y: 400 });
const result = await browser.observe(tab);
// Close during host shutdown, even when the parent window remains alive.
browser.dispose();
```

Pass only `BrowserTabPort` plus authorized tab references to an adapter. The
runtime itself does **not** authenticate secretaries or check application grants.
The connected adapter in `src/browser/host.ts` supplies that boundary through the
API's human-owned attachment grants and Core's durable job ledger. It exposes
`browser.tabs`, `browser.observe` and `browser.act` to the granted secretary.
See [connected host setup](../../docs/shared-browser-host.md) for the configured
launch path, authentication, revocation and result-loss semantics. No API token,
cookie getter, raw CDP, arbitrary JavaScript or arbitrary selector operation is
included in the page/caller contract. The desktop host opens no inbound listener
or generic renderer IPC bridge.

The host chooses a stable profile ID, and the runtime creates a dedicated
`persist:sumi-browser-<profileId>` Electron Session. That ID must identify a
browser profile, not be populated from an untrusted caller's requested account.
Each open tab receives a random browser-owned tab ID and belongs to a random
runtime incarnation. Electron's integer `webContents.id` is never a public ID.
This slice permits one tab per supplied window and at most 16 open tabs; the
host owns window/chrome layout. Runtime recreation reuses the profile partition
but never reattaches an old tab reference.

## Behavior and limits

- `observe` returns bounded top-level viewport text and visible form/link/button
  targets. It omits password/file values, hidden/offscreen text, child-frame
  contents, shadow DOM and canvas content. It is not a full accessibility tree.
  Observations contain untrusted website content, not host instructions.
- `act` supports DOM click, text/password input or textarea fill, scroll, and
  HTTP(S) navigation. DOM click/input events are synthetic; websites requiring
  trusted input, custom editors, select menus, file upload, drag/drop or nested
  frames need a later interaction adapter. This is not a complete browser-use
  engine. No high-level agent loop or Jev integration is included.
- An observation creates opaque target IDs retained only in a separate isolated
  JavaScript world. Bindings are single-use, expire after 30 seconds, and are
  replaced by the next observation. Main-frame navigation (including history
  changes), tab closure, and runtime shutdown invalidate them. A removed,
  disabled, hidden or covered target fails instead of choosing another element.
  They bind a document/target, not a frozen page: a website can update text and
  behavior within that document. A future exact-effect approval adapter must
  define and revalidate its own effect constraints.
- Concurrent secretary calls on a tab return `tab_busy`; no unbounded command
  queue is created. Human input remains ordinary Chromium input. This runtime
  imposes no human-priority takeover, control lease, or cognitive policy.
- Actions return `dispatched`, not proof that a website saved data or finished
  loading. Observe again to inspect the result. `tab_navigating` means the caller
  should wait for loading to settle; `listTabs()` exposes that state to the host.
  A navigation error is not automatically retried.
- Operations time out after five seconds. An unresponsive tab is marked
  unavailable and must be closed; a timed-out mutation may already have had an
  effect and must not be automatically retried. Delayed DOM operations also
  check their deadline before dispatch. Closing a window closes its WebContents;
  a renderer crash is explicit and never starts an invisible substitute browser.

Remote pages have no preload, Node, Electron API or Sumi IPC. Sandboxing, context
isolation and web security are enabled. Each profile denies site/device
permissions, popups and downloads; privileged/local protocols are blocked.
Permission/download UI is not implemented. HTTP(S) content retains Chromium's
normal origin and cookie boundaries. Sumi shell credentials must remain in a
different session. These defenses do not claim immunity to Chromium exploits;
shipping a general desktop browser requires continued Electron security updates.

## Verification

```sh
pnpm --filter @sumi/desktop check-types
pnpm --filter @sumi/desktop test
SUMI_BROWSER_TEST_HOME=/path/to/owned/browser-test-profile \
SUMI_BROWSER_TEST_ARTIFACTS=/path/to/owned/browser-test-artifacts \
xvfb-run -a pnpm --filter @sumi/desktop test:browser
```

Use an isolated fixture profile, never real user data. The acceptance program
starts its own local fixture server and actual Electron windows with sandboxing
enabled. It sends native Chromium mouse/keyboard input to the attached view
(simulated human input; verified `isTrusted`), observes through the secretary
port, fills/clicks through that port, and checks the same visible WebContents and
rendered screenshot. It also tests stale handles, navigation, concurrency,
removed targets, permissions, page isolation, origin/session boundaries, popup
and download blocking, crash/closure/disposal, and navigation timeout. The
intentional renderer-crash test emits Chromium's "Crashing because hung" line.
The fixture's plain page is test content, not proposed product screen design.

Verified scope is Linux/WSL under Xvfb. Same-process runtime recreation retains
profile storage. Electron persistent storage is configured, but process-restart
persistence has not been acceptance-tested here. Live-tab restart reattachment,
tab/history restoration, Mac/Windows packaging/signing, desktop sign-in UI,
bundled `apps/web` integration, and OS-specific validation remain open. The
configured outbound API bridge now provides authenticated secretary access;
its real-browser acceptance uses loopback and a substituted human-login proof.

Relevant primary references:
[WebContentsView](https://www.electronjs.org/docs/latest/api/web-contents-view),
[webContents](https://www.electronjs.org/docs/latest/api/web-contents),
[Session](https://www.electronjs.org/docs/latest/api/session),
[Electron security](https://www.electronjs.org/docs/latest/tutorial/security),
[ES modules](https://www.electronjs.org/docs/latest/tutorial/esm).

### Host network authority

A granted shared tab uses the browser host's ordinary HTTP(S) network reachability,
including local and private sites the host can reach. This is intentional for the
application people and their secretary share. The standing tab grant permits
observing those pages and, when actions are enabled, navigating to them; it does
not add a separate approval for each private destination. This is distinct from
the API's server-side public-web/MCP proxy, which restricts private-network egress.
Chromium origin and session isolation still apply. Non-web privileged protocols
remain blocked, and malformed request URLs are cancelled with a completed callback.
