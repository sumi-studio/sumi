import {
  type FormEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { ApiClientError, DecisionApi, type HumanSessionPayload } from "./api";
import type { Choice, DecisionRequest } from "./contracts";

const api = new DecisionApi();
const CACHE_PREFIX = "sumi-decision-inbox:v1:";

type AuthState = "checking" | "required" | "ready";
type View = "pending" | "history" | "settings";
type PushState =
  | "checking"
  | "unsupported"
  | "off"
  | "on"
  | "denied"
  | "expired";

function cacheWrite(key: string, value: unknown): void {
  try {
    localStorage.setItem(`${CACHE_PREFIX}${key}`, JSON.stringify(value));
  } catch {
    // The network response remains authoritative; cache failure is non-fatal.
  }
}

function cacheRead<T>(key: string): T | null {
  try {
    const stored = localStorage.getItem(`${CACHE_PREFIX}${key}`);
    return stored ? (JSON.parse(stored) as T) : null;
  } catch {
    return null;
  }
}

function cacheClear(): void {
  try {
    for (let index = localStorage.length - 1; index >= 0; index -= 1) {
      const key = localStorage.key(index);
      if (key?.startsWith(CACHE_PREFIX)) localStorage.removeItem(key);
    }
  } catch {
    // Sign-out still invalidates the authoritative server session.
  }
}

function requestIdFromPath(): string | null {
  const match = window.location.pathname.match(
    /^\/requests\/([A-Za-z0-9_-]{20,64})$/u,
  );
  return match?.[1] ?? null;
}

function navigate(path: string): void {
  window.history.pushState({}, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function relativeTime(value: string): string {
  const difference = Date.parse(value) - Date.now();
  const absolute = Math.abs(difference);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (absolute < 60_000)
    return formatter.format(Math.round(difference / 1_000), "second");
  if (absolute < 3_600_000)
    return formatter.format(Math.round(difference / 60_000), "minute");
  if (absolute < 86_400_000)
    return formatter.format(Math.round(difference / 3_600_000), "hour");
  return formatter.format(Math.round(difference / 86_400_000), "day");
}

function fullTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function base64UrlToBytes(value: string): Uint8Array<ArrayBuffer> {
  const padded = value + "=".repeat((4 - (value.length % 4)) % 4);
  const binary = atob(padded.replace(/-/g, "+").replace(/_/g, "/"));
  const bytes = new Uint8Array(new ArrayBuffer(binary.length));
  for (let index = 0; index < binary.length; index += 1)
    bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/u, "");
}

async function pushEndpointHash(endpoint: string): Promise<string> {
  const bytes = new TextEncoder().encode(`push:${endpoint}`);
  return bytesToBase64Url(
    new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)),
  );
}

function Icon({
  name,
  size = 18,
}: {
  name: "arrow" | "bell" | "check" | "clock" | "history" | "inbox" | "settings";
  size?: number;
}) {
  const paths: Record<typeof name, ReactNode> = {
    arrow: <path d="M15 18l-6-6 6-6" />,
    bell: <path d="M18 8a6 6 0 00-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4" />,
    check: <path d="M20 6L9 17l-5-5" />,
    clock: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M12 7v5l3 2" />
      </>
    ),
    history: (
      <>
        <path d="M3 12a9 9 0 109-9 9 9 0 00-6.4 2.7L3 8" />
        <path d="M3 3v5h5M12 7v5l3 2" />
      </>
    ),
    inbox: (
      <>
        <path d="M4 5h16v14H4z" />
        <path d="M4 14h4l2 2h4l2-2h4" />
      </>
    ),
    settings: (
      <>
        <circle cx="12" cy="12" r="3" />
        <path d="M19.4 15a1.7 1.7 0 00.3 1.9l.1.1-2.8 2.8-.1-.1a1.7 1.7 0 00-1.9-.3 1.7 1.7 0 00-1 1.5V21h-4v-.1a1.7 1.7 0 00-1-1.5 1.7 1.7 0 00-1.9.3l-.1.1L4.2 17l.1-.1a1.7 1.7 0 00.3-1.9 1.7 1.7 0 00-1.5-1H3v-4h.1a1.7 1.7 0 001.5-1 1.7 1.7 0 00-.3-1.9L4.2 7 7 4.2l.1.1a1.7 1.7 0 001.9.3 1.7 1.7 0 001-1.5V3h4v.1a1.7 1.7 0 001 1.5 1.7 1.7 0 001.9-.3l.1-.1L19.8 7l-.1.1a1.7 1.7 0 00-.3 1.9 1.7 1.7 0 001.5 1h.1v4h-.1a1.7 1.7 0 00-1.5 1z" />
      </>
    ),
  };
  return (
    <svg
      aria-hidden="true"
      className="icon"
      fill="none"
      height={size}
      viewBox="0 0 24 24"
      width={size}
    >
      {paths[name]}
    </svg>
  );
}

function StatusPill({ status }: { status: DecisionRequest["status"] }) {
  const labels = {
    pending: "Pending",
    resolved: "Resolved",
    cancelled: "Cancelled",
    expired: "Expired",
  };
  return (
    <span className={`status status--${status}`}>
      {status === "resolved" && <Icon name="check" size={13} />}
      {labels[status]}
    </span>
  );
}

function OfflineBanner() {
  return (
    <div className="offline-banner" role="status">
      Offline · Showing the last saved view. Actions are paused.
    </div>
  );
}

function LoadingScreen() {
  return (
    <main className="center-state">
      <div className="brand-mark" aria-hidden="true">
        S
      </div>
      <p>Opening your inbox…</p>
    </main>
  );
}

function SignIn({
  initialError,
  onReady,
}: {
  initialError: string;
  onReady: (session: HumanSessionPayload) => void;
}) {
  const [token, setToken] = useState("");
  const [error, setError] = useState(initialError);
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!navigator.onLine)
      return setError("Connect to the internet to sign in.");
    setBusy(true);
    setError("");
    try {
      onReady(await api.bootstrap(token));
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "The private link could not be used.",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="auth-shell">
      <section className="auth-panel" aria-labelledby="sign-in-title">
        <div className="brand-mark" aria-hidden="true">
          S
        </div>
        <p className="eyebrow">Private utility</p>
        <h1 id="sign-in-title">Decision inbox</h1>
        <p className="auth-copy">
          A quiet place for the few decisions that need your judgment. Use the
          private link issued by your publisher, or paste its one-time code.
        </p>
        <form onSubmit={submit}>
          <label htmlFor="bootstrap-token">One-time code</label>
          <input
            id="bootstrap-token"
            autoComplete="off"
            autoCapitalize="none"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            placeholder="Paste private code"
            required
            minLength={16}
          />
          {error && (
            <p className="form-error" role="alert">
              {error}
            </p>
          )}
          <button
            className="button button--primary button--wide"
            disabled={busy}
            type="submit"
          >
            {busy ? "Checking…" : "Open inbox"}
          </button>
        </form>
        <p className="fine-print">
          The code is exchanged for a private browser session and cannot be used
          again.
        </p>
      </section>
    </main>
  );
}

function EmptyState({ view }: { view: "pending" | "history" }) {
  return (
    <div className="empty-state">
      <div className="empty-icon">
        <Icon name={view === "pending" ? "inbox" : "history"} size={22} />
      </div>
      <h2>{view === "pending" ? "Nothing needs you" : "No decisions yet"}</h2>
      <p>
        {view === "pending"
          ? "New requests will appear here and, if enabled, arrive as a push notification."
          : "Resolved, cancelled, and expired requests will collect here."}
      </p>
    </div>
  );
}

function RequestList({
  requests,
  view,
  stale,
}: {
  requests: DecisionRequest[];
  view: "pending" | "history";
  stale: boolean;
}) {
  if (requests.length === 0) return <EmptyState view={view} />;
  return (
    <div className="request-list" aria-live="polite">
      {requests.map((request) => (
        <button
          className="request-row"
          key={request.id}
          onClick={() => navigate(`/requests/${request.id}`)}
          type="button"
        >
          <span className="request-row__top">
            <span className="source">{request.source}</span>
            <StatusPill status={request.status} />
          </span>
          <strong>{request.title}</strong>
          <span className="request-preview">{request.body}</span>
          <span className="request-time">
            {request.status === "pending"
              ? `Expires ${relativeTime(request.expiresAt)}`
              : `Updated ${relativeTime(request.updatedAt)}`}
            {stale ? " · saved" : ""}
          </span>
        </button>
      ))}
    </div>
  );
}

function PushSettings({
  session,
  pushState,
  setPushState,
  online,
}: {
  session: HumanSessionPayload;
  pushState: PushState;
  setPushState: (state: PushState) => void;
  online: boolean;
}) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");

  async function enable() {
    if (!online)
      return setMessage(
        "Connect to the internet to change notification settings.",
      );
    setBusy(true);
    setMessage("");
    try {
      const permission = await Notification.requestPermission();
      if (permission !== "granted") {
        setPushState("denied");
        setMessage(
          "Notifications are blocked in browser settings. Nothing else was changed.",
        );
        return;
      }
      const registration = await navigator.serviceWorker.ready;
      let subscription = await registration.pushManager.getSubscription();
      if (!subscription) {
        subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: base64UrlToBytes(session.vapidPublicKey),
        });
      }
      await api.subscribe(subscription.toJSON());
      setPushState("on");
      setMessage("Push notifications are on for this device.");
    } catch (cause) {
      setPushState("expired");
      setMessage(
        cause instanceof Error ? cause.message : "Push could not be enabled.",
      );
    } finally {
      setBusy(false);
    }
  }

  async function disable() {
    if (!online)
      return setMessage(
        "Connect to the internet to change notification settings.",
      );
    setBusy(true);
    setMessage("");
    try {
      const registration = await navigator.serviceWorker.ready;
      const subscription = await registration.pushManager.getSubscription();
      if (subscription) {
        await api.unsubscribe(subscription.endpoint);
        await subscription.unsubscribe();
      }
      setPushState("off");
      setMessage("Push notifications are off for this device.");
    } catch (cause) {
      setMessage(
        cause instanceof Error ? cause.message : "Push could not be disabled.",
      );
    } finally {
      setBusy(false);
    }
  }

  const statusCopy = {
    checking: "Checking this device…",
    unsupported: "This browser does not support standard Web Push.",
    off: "Off on this device",
    on: "On for this device",
    denied: "Blocked by browser settings",
    expired: "Subscription needs to be renewed",
  }[pushState];

  return (
    <section className="settings-section">
      <div className="settings-icon">
        <Icon name="bell" />
      </div>
      <div className="settings-body">
        <h2>Push notifications</h2>
        <p>{statusCopy}</p>
        {message && (
          <p className="settings-message" role="status">
            {message}
          </p>
        )}
        {pushState !== "unsupported" && pushState !== "checking" && (
          <div className="button-row">
            {pushState === "on" ? (
              <button
                className="button"
                disabled={busy || !online}
                onClick={disable}
                type="button"
              >
                Turn off
              </button>
            ) : (
              <button
                className="button button--primary"
                disabled={busy || !online}
                onClick={enable}
                type="button"
              >
                {busy ? "Working…" : "Enable on this device"}
              </button>
            )}
          </div>
        )}
        <p className="fine-print">
          Permission is requested only after you tap enable. Expired
          subscriptions are replaced instead of silently assumed to work.
        </p>
      </div>
    </section>
  );
}

function SettingsView({
  session,
  pushState,
  setPushState,
  online,
  onLogout,
}: {
  session: HumanSessionPayload;
  pushState: PushState;
  setPushState: (state: PushState) => void;
  online: boolean;
  onLogout: () => Promise<void>;
}) {
  return (
    <div className="content settings-view">
      <div className="section-heading">
        <p className="eyebrow">This device</p>
        <h1>Settings</h1>
      </div>
      <PushSettings
        session={session}
        pushState={pushState}
        setPushState={setPushState}
        online={online}
      />
      <section className="settings-section">
        <div className="settings-icon">
          <Icon name="settings" />
        </div>
        <div className="settings-body">
          <h2>Private session</h2>
          <p>Active until {fullTime(session.expiresAt)}.</p>
          <div className="button-row">
            <button className="button" onClick={onLogout} type="button">
              Sign out on this device
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}

function ResolvedSummary({ request }: { request: DecisionRequest }) {
  const selected = request.choices.find(
    (choice) => choice.id === request.response?.choiceId,
  );
  return (
    <div className={`resolution resolution--${request.status}`}>
      <div className="resolution__icon">
        {request.status === "resolved" ? (
          <Icon name="check" size={20} />
        ) : (
          <Icon name="clock" size={20} />
        )}
      </div>
      <div>
        <p className="eyebrow">{request.status}</p>
        <h2>
          {request.status === "resolved"
            ? (selected?.label ?? "Written response sent")
            : request.status === "cancelled"
              ? "Request cancelled"
              : "Request expired"}
        </h2>
        {request.response?.reply && (
          <p className="resolution__reply">“{request.response.reply}”</p>
        )}
        <p className="fine-print">
          {request.status === "resolved"
            ? `Recorded ${fullTime(request.response?.createdAt ?? request.updatedAt)}`
            : `Updated ${fullTime(request.updatedAt)}`}
        </p>
      </div>
    </div>
  );
}

function DecisionDetail({
  request,
  online,
  stale,
  onResolved,
}: {
  request: DecisionRequest;
  online: boolean;
  stale: boolean;
  onResolved: (request: DecisionRequest) => void;
}) {
  const [selected, setSelected] = useState<string | undefined>();
  const [reply, setReply] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const idempotencyKey = useRef(crypto.randomUUID());
  const canSubmit =
    request.status === "pending" &&
    Boolean(selected || reply.trim()) &&
    online &&
    !busy;

  async function submit() {
    if (!online)
      return setError(
        "Connect to the internet before sending. Nothing has been queued.",
      );
    if (!selected && !reply.trim())
      return setError("Choose an option or write a reply.");
    setBusy(true);
    setError("");
    try {
      const resolved = await api.respond(request.id, {
        ...(selected ? { choiceId: selected } : {}),
        ...(reply.trim() ? { reply: reply.trim() } : {}),
        idempotencyKey: idempotencyKey.current,
      });
      cacheWrite(`request:${request.id}`, resolved);
      onResolved(resolved);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "The decision was not sent.",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="detail-page">
      <div className="detail-nav">
        <button
          className="icon-button"
          aria-label="Back to inbox"
          onClick={() => navigate("/")}
          type="button"
        >
          <Icon name="arrow" />
        </button>
        <StatusPill status={request.status} />
      </div>
      <article className="decision-sheet">
        <header className="decision-header">
          <p className="eyebrow">{request.source}</p>
          <h1>{request.title}</h1>
          <div className="metadata">
            <span>Sent {relativeTime(request.createdAt)}</span>
            <span aria-hidden="true">·</span>
            <span title={fullTime(request.expiresAt)}>
              Expires {relativeTime(request.expiresAt)}
            </span>
            {stale && (
              <>
                <span aria-hidden="true">·</span>
                <span>Saved view</span>
              </>
            )}
          </div>
        </header>
        <section className="context-block" aria-label="Context">
          <p>{request.body}</p>
        </section>
        {request.status === "pending" ? (
          <>
            <fieldset className="choice-list">
              <legend>Choose a response</legend>
              {request.choices.map((choice: Choice) => (
                <button
                  aria-pressed={selected === choice.id}
                  className={`choice choice--${choice.tone}${selected === choice.id ? " choice--selected" : ""}`}
                  key={choice.id}
                  onClick={() => setSelected(choice.id)}
                  type="button"
                >
                  <span>{choice.label}</span>
                  <span className="choice-indicator" aria-hidden="true">
                    {selected === choice.id && <Icon name="check" size={15} />}
                  </span>
                </button>
              ))}
            </fieldset>
            {request.allowFreeText && (
              <div className="reply-field">
                <label htmlFor="reply">
                  Add a short note <span>optional</span>
                </label>
                <textarea
                  id="reply"
                  maxLength={500}
                  rows={3}
                  value={reply}
                  onChange={(event) => setReply(event.target.value)}
                  placeholder="What should the agent know?"
                />
                <span className="character-count">{reply.length}/500</span>
              </div>
            )}
            {!online && (
              <p className="inline-warning" role="status">
                You are offline. Review is available, but sending is paused and
                will not be queued.
              </p>
            )}
            {error && (
              <p className="form-error" role="alert">
                {error}
              </p>
            )}
          </>
        ) : (
          <ResolvedSummary request={request} />
        )}
      </article>
      {request.status === "pending" && (
        <div className="action-dock">
          <div className="action-dock__inner">
            <button
              className="button button--primary button--wide"
              disabled={!canSubmit}
              onClick={submit}
              type="button"
            >
              {busy ? "Sending…" : "Send decision"}
            </button>
            <p>
              One final action. Repeated taps cannot create a second response.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

export function App() {
  const [auth, setAuth] = useState<AuthState>("checking");
  const [authError, setAuthError] = useState("");
  const [session, setSession] = useState<HumanSessionPayload | null>(null);
  const [view, setView] = useState<View>("pending");
  const [requestId, setRequestId] = useState(requestIdFromPath);
  const [requests, setRequests] = useState<DecisionRequest[]>([]);
  const [detail, setDetail] = useState<DecisionRequest | null>(null);
  const [loading, setLoading] = useState(true);
  const [stale, setStale] = useState(false);
  const [error, setError] = useState("");
  const [online, setOnline] = useState(navigator.onLine);
  const [pushState, setPushState] = useState<PushState>("checking");

  useEffect(() => {
    const onPop = () => setRequestId(requestIdFromPath());
    const onOnline = () => setOnline(true);
    const onOffline = () => setOnline(false);
    window.addEventListener("popstate", onPop);
    window.addEventListener("online", onOnline);
    window.addEventListener("offline", onOffline);
    return () => {
      window.removeEventListener("popstate", onPop);
      window.removeEventListener("online", onOnline);
      window.removeEventListener("offline", onOffline);
    };
  }, []);

  useEffect(() => {
    if (!("serviceWorker" in navigator)) return;
    void navigator.serviceWorker.register("/sw.js");
  }, []);

  const acceptSession = useCallback((value: HumanSessionPayload) => {
    api.setCsrfToken(value.csrfToken);
    setSession(value);
    setAuth("ready");
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function authenticate() {
      const params = new URLSearchParams(window.location.hash.slice(1));
      const bootstrap = params.get("bootstrap");
      if (bootstrap)
        window.history.replaceState(
          {},
          "",
          `${window.location.pathname}${window.location.search}`,
        );
      try {
        const result = bootstrap
          ? await api.bootstrap(bootstrap)
          : await api.session();
        if (!cancelled) acceptSession(result);
      } catch (cause) {
        if (cancelled) return;
        setAuthError(bootstrap && cause instanceof Error ? cause.message : "");
        setAuth("required");
      }
    }
    void authenticate();
    return () => {
      cancelled = true;
    };
  }, [acceptSession]);

  useEffect(() => {
    if (auth !== "ready" || !session) return;
    if (
      !("Notification" in window) ||
      !("serviceWorker" in navigator) ||
      !("PushManager" in window)
    ) {
      setPushState("unsupported");
      return;
    }
    if (Notification.permission === "denied") {
      setPushState("denied");
      return;
    }
    void navigator.serviceWorker.ready
      .then((registration) => registration.pushManager.getSubscription())
      .then(async (subscription) => {
        if (subscription) {
          const endpointHash = await pushEndpointHash(subscription.endpoint);
          if (session.registeredEndpointHashes.includes(endpointHash)) {
            setPushState("on");
          } else {
            await subscription.unsubscribe();
            setPushState("expired");
          }
        } else if (
          Notification.permission === "granted" ||
          session.pushSubscriptionCount > 0
        ) {
          setPushState("expired");
        } else {
          setPushState("off");
        }
      })
      .catch(() => setPushState("expired"));
  }, [auth, session]);

  const load = useCallback(async () => {
    if (auth !== "ready") return;
    setLoading(true);
    setError("");
    const listView = view === "history" ? "history" : "pending";
    if (!online) {
      const cached = requestId
        ? cacheRead<DecisionRequest>(`request:${requestId}`)
        : cacheRead<DecisionRequest[]>(`list:${listView}`);
      if (cached) {
        if (requestId) setDetail(cached as DecisionRequest);
        else setRequests(cached as DecisionRequest[]);
        setStale(true);
      } else if (view !== "settings") {
        setError("No saved copy is available for this view.");
      }
      setLoading(false);
      return;
    }
    try {
      if (requestId) {
        const value = await api.get(requestId);
        setDetail(value);
        cacheWrite(`request:${requestId}`, value);
      } else if (view !== "settings") {
        const values = await api.list(listView);
        setRequests(values);
        cacheWrite(`list:${listView}`, values);
      }
      setStale(false);
    } catch (cause) {
      if (cause instanceof ApiClientError && cause.status === 401) {
        setAuth("required");
        setSession(null);
        return;
      }
      const cached = requestId
        ? cacheRead<DecisionRequest>(`request:${requestId}`)
        : cacheRead<DecisionRequest[]>(`list:${listView}`);
      if (cached) {
        if (requestId) setDetail(cached as DecisionRequest);
        else setRequests(cached as DecisionRequest[]);
        setStale(true);
      } else {
        setError(
          cause instanceof Error
            ? cause.message
            : "The inbox could not be loaded.",
        );
      }
    } finally {
      setLoading(false);
    }
  }, [auth, online, requestId, view]);

  useEffect(() => {
    void load();
  }, [load]);

  const pendingCount = useMemo(
    () =>
      view === "pending"
        ? requests.length
        : (cacheRead<DecisionRequest[]>("list:pending")?.length ?? 0),
    [requests, view],
  );

  async function logout() {
    if (!online) return;
    await api.logout();
    cacheClear();
    setSession(null);
    setAuth("required");
  }

  function changeView(next: View) {
    if (next !== "settings") {
      const cached = cacheRead<DecisionRequest[]>(`list:${next}`);
      setRequests(cached ?? []);
      setStale(Boolean(cached));
    }
    setView(next);
  }

  function recordResolution(resolved: DecisionRequest) {
    setDetail(resolved);
    const pending = (cacheRead<DecisionRequest[]>("list:pending") ?? []).filter(
      (request) => request.id !== resolved.id,
    );
    const history = cacheRead<DecisionRequest[]>("list:history") ?? [];
    cacheWrite("list:pending", pending);
    cacheWrite("list:history", [
      resolved,
      ...history.filter((request) => request.id !== resolved.id),
    ]);
  }

  if (auth === "checking") return <LoadingScreen />;
  if (auth === "required" || !session)
    return <SignIn initialError={authError} onReady={acceptSession} />;

  if (requestId) {
    const currentDetail = detail?.id === requestId ? detail : null;
    if (loading && !currentDetail) return <LoadingScreen />;
    if (error && !currentDetail)
      return (
        <main className="center-state">
          <h1>Couldn’t open this request</h1>
          <p>{error}</p>
          <button
            className="button"
            onClick={() => navigate("/")}
            type="button"
          >
            Back to inbox
          </button>
        </main>
      );
    if (currentDetail)
      return (
        <>
          <header className="app-header app-header--detail">
            <div className="brand-lockup">
              <span className="brand-mark brand-mark--small">S</span>
              <span>Decision inbox</span>
            </div>
            <span
              className={`network-dot${online ? "" : " network-dot--offline"}`}
              title={online ? "Online" : "Offline"}
            />
          </header>
          {!online && <OfflineBanner />}
          <DecisionDetail
            request={currentDetail}
            online={online}
            stale={stale}
            onResolved={recordResolution}
          />
        </>
      );
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="brand-lockup">
          <span className="brand-mark brand-mark--small">S</span>
          <span>Decision inbox</span>
        </div>
        <div className="header-actions">
          <span
            className={`network-dot${online ? "" : " network-dot--offline"}`}
            title={online ? "Online" : "Offline"}
          />
          <button
            className="icon-button"
            aria-label="Settings"
            aria-pressed={view === "settings"}
            onClick={() => changeView("settings")}
            type="button"
          >
            <Icon name="settings" />
          </button>
        </div>
      </header>
      {!online && <OfflineBanner />}
      <main className="content">
        {view === "settings" ? (
          <SettingsView
            session={session}
            pushState={pushState}
            setPushState={setPushState}
            online={online}
            onLogout={logout}
          />
        ) : (
          <>
            <div className="section-heading">
              <p className="eyebrow">Private queue</p>
              <h1>{view === "pending" ? "Needs your call" : "History"}</h1>
              <p>
                {view === "pending"
                  ? `${pendingCount === 0 ? "No" : pendingCount} unresolved request${pendingCount === 1 ? "" : "s"}`
                  : "A clear record of what happened"}
              </p>
            </div>
            {error && (
              <div className="error-panel" role="alert">
                <p>{error}</p>
                <button
                  className="button"
                  onClick={() => void load()}
                  type="button"
                >
                  Try again
                </button>
              </div>
            )}
            {loading && requests.length === 0 ? (
              <div className="list-loading" aria-label="Loading" role="status">
                <span />
                <span />
                <span />
              </div>
            ) : (
              <RequestList requests={requests} view={view} stale={stale} />
            )}
          </>
        )}
      </main>
      <nav className="bottom-nav" aria-label="Inbox views">
        <div className="bottom-nav__inner">
          <button
            aria-current={view === "pending" ? "page" : undefined}
            onClick={() => changeView("pending")}
            type="button"
          >
            <span className="nav-icon-wrap">
              <Icon name="inbox" />
              {pendingCount > 0 && (
                <span className="badge">{Math.min(pendingCount, 99)}</span>
              )}
            </span>
            <span>Pending</span>
          </button>
          <button
            aria-current={view === "history" ? "page" : undefined}
            onClick={() => changeView("history")}
            type="button"
          >
            <Icon name="history" />
            <span>History</span>
          </button>
        </div>
      </nav>
    </div>
  );
}
