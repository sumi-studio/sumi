import { Button } from "@sumi/ui/components/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@sumi/ui/components/tooltip";
import { useNavigate } from "@tanstack/react-router";
import {
  Check,
  ChevronRight,
  LogOut,
  Monitor,
  Moon,
  Palette,
  Settings,
  Sun,
  UserRound,
} from "lucide-react";
import type { ComponentType, ReactElement } from "react";
import { useState } from "react";
import { useAuth } from "../auth/auth-context";
import { ProviderSettings } from "../auth/provider-settings";
import { LOCAL_APP_DESCRIPTORS } from "../shell/app-descriptors";
import { type ThemePreference, useTheme } from "../theme/theme-provider";

const THEME_OPTIONS: Array<{
  id: ThemePreference;
  label: string;
  icon: ComponentType<{ className?: string }>;
}> = [
  { id: "system", label: "システム", icon: Monitor },
  { id: "light", label: "ライト", icon: Sun },
  { id: "dark", label: "ダーク", icon: Moon },
];

/**
 * direct chat（直通）画面のレール。アプリ一覧はshell/app-descriptorsの
 * local providerから描画し、ホーム（メッセージング）へ戻れる。
 */
export function AppNavigation() {
  const navigate = useNavigate();
  return (
    <aside className="app-sidebar flex h-dvh w-12 shrink-0 flex-col overflow-clip">
      <nav className="flex flex-col gap-1 px-1 py-2" aria-label="Sumi">
        {LOCAL_APP_DESCRIPTORS.map((app) => {
          const Icon = app.icon;
          const active = app.id === "direct";
          return (
            <NavigationTooltip key={app.id} label={app.label}>
              <Button
                variant="ghost"
                size="icon"
                aria-label={app.label}
                aria-current={active ? "page" : undefined}
                onClick={
                  active ? undefined : () => void navigate({ to: app.route })
                }
                className={`size-10 ${active ? "bg-interactive-active" : ""}`}
              >
                <Icon className="size-4" />
              </Button>
            </NavigationTooltip>
          );
        })}
      </nav>
      <div className="mt-auto px-2 pb-3">
        <SettingsPopover />
      </div>
    </aside>
  );
}

function SettingsPopover() {
  const { authenticated, user, logout } = useAuth();
  const [logoutError, setLogoutError] = useState<string | null>(null);

  const handleLogout = async () => {
    setLogoutError(null);
    try {
      await logout();
    } catch {
      setLogoutError("ログアウトを完了できませんでした。");
    }
  };

  return (
    <Popover>
      <Tooltip>
        <TooltipTrigger
          render={
            <PopoverTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label="設定"
                  className="size-8 shrink-0"
                />
              }
            />
          }
        >
          <Settings className="size-4" />
        </TooltipTrigger>
        <TooltipContent side="right">設定</TooltipContent>
      </Tooltip>
      <PopoverContent side="top" align="start" aria-label="設定">
        {authenticated && (
          <div className="mb-1 border-border border-b pb-1">
            <div className="flex items-center gap-2 px-2.5 py-2 text-sm">
              <UserRound className="size-4 shrink-0" />
              <span className="max-w-44 truncate">
                {user?.displayName ?? user?.email ?? user?.id ?? "アカウント"}
              </span>
            </div>
            <ProviderSettings humanId={user?.id ?? ""} />
            <Button
              variant="ghost"
              onClick={() => void handleLogout()}
              className="w-full justify-start gap-2 px-2.5 text-popover-foreground hover:text-popover-foreground"
            >
              <LogOut className="size-4" />
              ログアウト
            </Button>
          </div>
        )}
        <ThemePicker />
        {logoutError && (
          <p role="alert" className="mt-1 max-w-56 px-2.5 text-red-600 text-xs">
            {logoutError}
          </p>
        )}
      </PopoverContent>
    </Popover>
  );
}

function NavigationTooltip({
  label,
  children,
}: {
  label: string;
  children: ReactElement;
}) {
  return (
    <Tooltip>
      <TooltipTrigger render={children} />
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}

function ThemePicker() {
  const { theme, setTheme } = useTheme();
  return (
    <Popover>
      <PopoverTrigger
        openOnHover
        delay={0}
        closeDelay={120}
        render={
          <Button
            variant="ghost"
            className="w-full justify-start gap-2 px-2.5 text-popover-foreground hover:text-popover-foreground"
          />
        }
      >
        <Palette className="size-4" />
        <span>テーマ</span>
        <ChevronRight className="ml-auto size-4 text-neutral-400" />
      </PopoverTrigger>
      <PopoverContent
        side="right"
        align="start"
        sideOffset={4}
        aria-label="テーマ"
      >
        {THEME_OPTIONS.map(({ id, label, icon: Icon }) => (
          <Button
            key={id}
            variant="ghost"
            onClick={() => setTheme(id)}
            className="w-full justify-start gap-2 px-2.5 text-popover-foreground hover:text-popover-foreground"
          >
            <Icon className="size-4" />
            {label}
            {theme === id && <Check className="ml-auto size-4" />}
          </Button>
        ))}
      </PopoverContent>
    </Popover>
  );
}
