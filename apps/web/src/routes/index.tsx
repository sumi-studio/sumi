import { createFileRoute } from "@tanstack/react-router";
import { MessagingScreen } from "../messaging/components/messaging-screen";

/**
 * ルート = Sumiのホーム面（Workspaceのchannel / DM / 直通）。
 * 現在はモックbackendで動くため認証ゲートを挟んでいない。
 * 実API統合時にAuthGate配下へ移す。
 */
export const Route = createFileRoute("/")({
  component: MessagingScreen,
});
