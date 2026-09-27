import { TooltipProvider } from "@sumi/ui/components/tooltip";
import { useCallback, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { TerminalApiClient } from "../src/terminal/api";
import { TerminalScreen } from "../src/terminal/terminal-screen";
import { localTerminalSession, type LocalTerminalBinding } from "./session";
import "./style.css";

const session = localTerminalSession(() => ({
  persona: localStorage.getItem("fmPersona") ?? "",
  token: localStorage.getItem("fmToken") ?? "",
}));

function LocalTerminal() {
  const [binding, setBinding] = useState<LocalTerminalBinding | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [generation, setGeneration] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const reconnect = useCallback(async () => {
    try {
      const next = await session.renew();
      setBinding(next);
      setGeneration((n) => n + 1);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "ターミナルに接続できません。");
      setBinding(null);
    }
  }, []);
  useEffect(() => { void reconnect(); }, [reconnect]);
  useEffect(() => {
    if (!binding) return;
    const timer = setTimeout(() => void reconnect(), Math.max(1000, (binding.expiresIn - 60) * 1000));
    return () => clearTimeout(timer);
  }, [binding, reconnect]);
  const client = useMemo(() => binding ? new TerminalApiClient(binding, session.fetch) : null, [binding]);
  return <>
    <nav className="local-navigation"><a href="/">会話とファイルに戻る</a></nav>
    <div className="local-content">
      {binding && client ? <TerminalScreen key={generation} {...binding} apiClient={client}
        sessionId={selected ?? undefined} onSelectSession={setSelected} onAuthorizationExpired={reconnect} /> :
        <main className="local-status" aria-live="polite">
          <p>{error ?? "ターミナルに接続しています…"}</p>
          {error ? <button type="button" onClick={() => void reconnect()}>再試行</button> : null}
        </main>}
    </div>
  </>;
}

createRoot(document.getElementById("root")!).render(<TooltipProvider><LocalTerminal /></TooltipProvider>);
