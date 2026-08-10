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
import { type FormEvent, useEffect, useState } from "react";
import { useAuth } from "../auth/auth-context";
import { ProviderSettings } from "../auth/provider-settings";
import { SumiProfileUpdateIndeterminateError } from "../auth/session-client";
import { refreshMessagingMemberProfiles } from "../messaging/store";
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

export function SettingsPopover() {
  const { authenticated, user, logout, updateDisplayName } = useAuth();
  const [logoutError, setLogoutError] = useState<string | null>(null);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileNotice, setProfileNotice] = useState<string | null>(null);
  const [displayName, setDisplayName] = useState("");
  const [savingProfile, setSavingProfile] = useState(false);

  useEffect(() => {
    setDisplayName(user?.displayName ?? "");
  }, [user?.displayName]);

  const handleLogout = async () => {
    setLogoutError(null);
    try {
      await logout();
    } catch {
      setLogoutError("ログアウトを完了できませんでした。");
    }
  };

  const handleProfileSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextDisplayName = displayName.trim();
    if (
      !nextDisplayName ||
      Array.from(nextDisplayName).length > 80 ||
      nextDisplayName === user?.displayName
    ) {
      return;
    }
    setProfileError(null);
    setProfileNotice(null);
    setSavingProfile(true);
    try {
      await updateDisplayName(nextDisplayName);
      try {
        await refreshMessagingMemberProfiles();
      } catch {
        setProfileNotice("保存済み。トークの表示は再読み込みで反映されます。");
      }
    } catch (error) {
      setProfileError(
        error instanceof SumiProfileUpdateIndeterminateError
          ? "更新結果を確認できませんでした。再読み込みしてください。"
          : "表示名を更新できませんでした。",
      );
    } finally {
      setSavingProfile(false);
    }
  };

  const displayNameCodePoints = Array.from(displayName.trim()).length;
  const displayNameTooLong = displayNameCodePoints > 80;

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
                {user?.displayName ?? "アカウント"}
              </span>
            </div>
            <form
              onSubmit={(event) => void handleProfileSubmit(event)}
              className="px-2.5 pb-2"
            >
              <label
                htmlFor="sumi-settings-display-name"
                className="mb-1 block text-muted-foreground text-xs"
              >
                表示名
              </label>
              <div className="flex gap-1.5">
                <input
                  id="sumi-settings-display-name"
                  value={displayName}
                  onChange={(event) => setDisplayName(event.target.value)}
                  disabled={user?.displayName === null}
                  maxLength={160}
                  aria-invalid={displayNameTooLong || undefined}
                  autoComplete="name"
                  className="min-w-0 flex-1 rounded-md border border-input bg-background px-2 py-1 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                />
                <Button
                  type="submit"
                  size="sm"
                  disabled={
                    savingProfile ||
                    user?.displayName === null ||
                    !displayName.trim() ||
                    displayNameTooLong ||
                    displayName.trim() === user?.displayName
                  }
                >
                  {savingProfile ? "保存中" : "保存"}
                </Button>
              </div>
              {profileError ? (
                <p role="alert" className="mt-1 text-red-600 text-xs">
                  {profileError}
                </p>
              ) : null}
              {displayNameTooLong ? (
                <p role="alert" className="mt-1 text-red-600 text-xs">
                  表示名は1〜80文字で入力してください。
                </p>
              ) : null}
              {profileNotice ? (
                <p role="status" className="mt-1 text-muted-foreground text-xs">
                  {profileNotice}
                </p>
              ) : null}
            </form>
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
