import { createFileRoute } from "@tanstack/react-router";
import { TerminalGate } from "../terminal/terminal-gate";

/**
 * 共有ターミナル = 本人と秘書が同じ永続セッションを操作する画面。
 * `?session=<id>` が選択を保持するので、ビューアを閉じて戻っても
 * 同じセッションへ再接続できる（セッション自体はビューアとは独立）。
 */
export const Route = createFileRoute("/terminal")({
  validateSearch: (search: Record<string, unknown>): { session?: string } =>
    typeof search.session === "string" &&
    /^[0-9a-f-]{36}$/i.test(search.session)
      ? { session: search.session }
      : {},
  component: TerminalRoute,
});

function TerminalRoute() {
  const { session } = Route.useSearch();
  const navigate = Route.useNavigate();
  return (
    <TerminalGate
      sessionId={session}
      onSelectSession={(id) =>
        void navigate({ search: id ? { session: id } : {}, replace: true })
      }
    />
  );
}
