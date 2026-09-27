import { type BaseWindow, session, WebContentsView } from "electron";
import type { GoalActivity } from "./host.js";

export const CONTROL_HEIGHT = 36;
const STOP_URL = "https://stop.sumi-host.invalid/";

const PAGE = `<!doctype html><meta charset="utf-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'">
<style>
body{margin:0;height:36px;display:flex;align-items:center;gap:10px;padding:0 10px;
font:13px system-ui,sans-serif;background:#eef1f5;color:#222;border-bottom:1px solid #c9ced6;overflow:hidden}
body.running{background:#fff4d6;border-color:#e0b84c}
#status{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
button{font:inherit;padding:3px 14px}
</style>
<span id="status"></span><button id="stop" disabled>Stop</button>
<script>
const stop=document.getElementById('stop');
stop.onclick=()=>{stop.disabled=true;location.href=${JSON.stringify(STOP_URL)}};
window.render=(m)=>{document.body.className=m.running?'running':'';
document.getElementById('status').textContent=m.text;stop.disabled=!m.running;};
</script>`;

/** Host-owned strip above the shared tab: tells the person when the secretary
 * is running a delegated goal and gives them a Stop button. It is a separate
 * WebContents in its own in-memory session with fixed local content, no
 * preload and no network; the website in the tab cannot reach or script it.
 * Its only output is the Stop navigation, which is intercepted here. */
export class GoalControlBar {
  readonly view: WebContentsView;
  private ready: Promise<void>;
  private last = "";

  constructor(
    private readonly window: BaseWindow,
    onStop: () => void,
    private readonly idleText: string,
  ) {
    const partition = session.fromPartition("sumi-host-control");
    partition.webRequest.onBeforeRequest((details, callback) =>
      callback({ cancel: !details.url.startsWith("data:") }),
    );
    partition.setPermissionRequestHandler((_c, _p, callback) =>
      callback(false),
    );
    this.view = new WebContentsView({
      webPreferences: {
        session: partition,
        sandbox: true,
        contextIsolation: true,
        nodeIntegration: false,
        webviewTag: false,
        navigateOnDragDrop: false,
        disableDialogs: true,
      },
    });
    const contents = this.view.webContents;
    contents.setWindowOpenHandler(() => ({ action: "deny" }));
    contents.on("will-navigate", (event, url) => {
      event.preventDefault();
      if (url === STOP_URL) onStop();
    });
    const resize = () => {
      if (window.isDestroyed()) return;
      const [width = 0] = window.getContentSize();
      this.view.setBounds({ x: 0, y: 0, width, height: CONTROL_HEIGHT });
    };
    window.contentView.addChildView(this.view);
    resize();
    window.on("resize", resize);
    this.ready = contents
      .loadURL(`data:text/html;base64,${Buffer.from(PAGE).toString("base64")}`)
      .then(() => this.render({ running: false, text: idleText }));
  }

  /** Point on the Stop button, for native-input acceptance tests. */
  async stopButtonPoint(): Promise<{ x: number; y: number }> {
    await this.ready;
    return this.view.webContents.executeJavaScript(
      "(()=>{const r=document.getElementById('stop').getBoundingClientRect();return {x:Math.round(r.x+r.width/2),y:Math.round(r.y+r.height/2)}})()",
    );
  }

  get text(): string {
    return this.last;
  }

  show(activity: GoalActivity): void {
    if (activity.state === "ended") {
      this.ready = this.ready.then(() =>
        this.render({
          running: false,
          text: `Secretary goal ended (${activity.outcome}). ${this.idleText}`,
        }),
      );
      return;
    }
    const p = activity.progress ?? {};
    const step = typeof p.step === "number" ? p.step : 0;
    const max = typeof p.max_steps === "number" ? p.max_steps : "?";
    const next = p.next as { operation?: string; target?: string } | undefined;
    const doing = next?.operation
      ? ` · next: ${next.operation}${next.target ? ` "${next.target.replace(/^t\d+ /, "")}"` : ""}`
      : "";
    this.ready = this.ready.then(() =>
      this.render({
        running: true,
        text: `Secretary is operating this tab (Jev) · step ${next?.operation ? step + 1 : step}/${max}${doing} · goal: ${activity.goal}`,
      }),
    );
  }

  private async render(model: { running: boolean; text: string }) {
    if (this.window.isDestroyed()) return;
    const text = model.text.replace(/\s+/g, " ").slice(0, 300);
    this.last = text;
    await this.view.webContents
      .executeJavaScript(
        `window.render(${JSON.stringify({ running: model.running, text })})`,
      )
      .catch(() => {});
  }
}
