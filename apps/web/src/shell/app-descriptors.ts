import { MessageCircle } from "lucide-react";
import type { ComponentType } from "react";

/**
 * アプリレールのlocal provider（Codex合意）。ここは実体ではなく記述の置き場で、
 * 将来は同じdescriptor列をserverから受け取り、rendererを
 * builtin / sdui / mcp_app に分ける。MCP AppのHTML/JSはsandboxed iframe内のみ。
 */
export interface AppDescriptor {
  id: string;
  label: string;
  icon: ComponentType<{ className?: string }>;
  route: string;
  renderer: "builtin";
}

export const LOCAL_APP_DESCRIPTORS: AppDescriptor[] = [
  {
    id: "home",
    label: "Sumi",
    icon: MessageCircle,
    route: "/",
    renderer: "builtin",
  },
  // Mail、Calendar、Tasks…は実ルートを持ったときにここへ増える。
];
