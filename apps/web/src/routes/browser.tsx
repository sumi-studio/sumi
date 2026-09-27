import { createFileRoute } from "@tanstack/react-router";
import { BrowserWorkspace } from "../browser/browser-workspace";

/**
 * 秘書のブラウザ = 秘書専用の Cloud ブラウザを本人と秘書が同じ画面で使う。
 * `/browser` は SPA のページで、ビューアの WebSocket は `/browser-cloud/viewer`。
 */
export const Route = createFileRoute("/browser")({
  component: BrowserWorkspace,
});
