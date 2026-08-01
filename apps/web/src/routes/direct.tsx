import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { DirectChatGate } from "../auth/direct-chat-gate";

/**
 * 直通 = 自分がEmployerである人格agent本人への生の直接回線（direct chat）。
 * ホームのサイドバー「直通」から入る。既存のトーク画面（状態管理・UI）を
 * そのまま使い、Secretary DMとは別surfaceのまま入口だけ同じ家に置く。
 */
export const Route = createFileRoute("/direct")({
  component: () => (
    <AuthGate>
      <DirectChatGate />
    </AuthGate>
  ),
});
