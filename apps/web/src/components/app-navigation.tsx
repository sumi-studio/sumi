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
import type { ComponentType } from "react";
import { type FormEvent, useEffect, useState } from "react";
import { useAuth } from "../auth/auth-context";
import { ProviderSettings } from "../auth/provider-settings";
import {
  canonicalizeSumiDisplayName,
  getSumiProfile,
  SumiProfileUpdateIndeterminateError,
} from "../auth/session-client";
import { isImeComposing } from "../lib/ime";
import {
  clampCodePoints,
  codePointLength,
  hasSafeDisplayCharacters,
} from "../lib/text-length";
import { refreshMessagingMemberProfiles } from "../messaging/store";
import { ParticipantAppsMenu } from "../participant/app-menu";
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

const MAX_DISPLAY_NAME_CODE_POINTS = 80;
const MAX_TAGLINE_CODE_POINTS = 100;

interface ProfileFields {
  displayName: string;
  tagline: string;
}

interface ProfileFormState {
  baseline: ProfileFields | null;
  values: ProfileFields;
}

export function SettingsPopover() {
  const { authenticated, user, logout, updateProfile } = useAuth();
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [logoutError, setLogoutError] = useState<string | null>(null);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileNotice, setProfileNotice] = useState<string | null>(null);
  const [profileLoadError, setProfileLoadError] = useState(false);
  const [profileLoadAttempt, setProfileLoadAttempt] = useState(0);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [profileForm, setProfileForm] = useState<ProfileFormState>({
    baseline: null,
    values: { displayName: "", tagline: "" },
  });
  const [savingProfile, setSavingProfile] = useState(false);
  const humanID = authenticated ? (user?.id ?? null) : null;

  // biome-ignore lint/correctness/useExhaustiveDependencies: changing Human identity must reset Human-owned form state.
  useEffect(() => {
    setProfileForm({
      baseline: null,
      values: { displayName: "", tagline: "" },
    });
    setProfileError(null);
    setProfileNotice(null);
    setProfileLoadError(false);
  }, [humanID]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: profileLoadAttempt intentionally triggers an explicit retry.
  useEffect(() => {
    if (humanID === null || !settingsOpen) {
      setLoadingProfile(false);
      return;
    }
    let cancelled = false;
    setLoadingProfile(true);
    setProfileLoadError(false);
    void getSumiProfile()
      .then((profile) => {
        if (cancelled) return;
        if (profile.participant.humanId !== humanID) {
          setProfileLoadError(true);
          return;
        }
        const canonical = {
          displayName: profile.displayName,
          tagline: profile.tagline,
        };
        setProfileForm((current) => {
          if (current.baseline === null) {
            return { baseline: canonical, values: canonical };
          }
          const displayNameDirty =
            canonicalizeSumiDisplayName(current.values.displayName) !==
            current.baseline.displayName;
          const taglineDirty =
            current.values.tagline.trim() !== current.baseline.tagline;
          return {
            baseline: canonical,
            values: {
              displayName: displayNameDirty
                ? current.values.displayName
                : canonical.displayName,
              tagline: taglineDirty
                ? current.values.tagline
                : canonical.tagline,
            },
          };
        });
      })
      .catch(() => {
        if (!cancelled) setProfileLoadError(true);
      })
      .finally(() => {
        if (!cancelled) setLoadingProfile(false);
      });
    return () => {
      cancelled = true;
    };
  }, [humanID, profileLoadAttempt, settingsOpen]);

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
    const baseline = profileForm.baseline;
    if (baseline === null) return;
    const nextDisplayName = canonicalizeSumiDisplayName(
      profileForm.values.displayName,
    );
    const nextTagline = profileForm.values.tagline.trim();
    if (!nextDisplayName || !hasSafeDisplayCharacters(nextDisplayName)) return;
    if (!hasSafeDisplayCharacters(nextTagline)) return;
    const patch: { displayName?: string; tagline?: string } = {};
    if (nextDisplayName !== baseline.displayName) {
      patch.displayName = nextDisplayName;
    }
    if (nextTagline !== baseline.tagline) {
      patch.tagline = nextTagline;
    }
    if (Object.keys(patch).length === 0) return;
    setProfileError(null);
    setProfileNotice(null);
    setSavingProfile(true);
    try {
      const confirmed = await updateProfile(patch);
      if (confirmed === null) return;
      const canonical = {
        displayName: confirmed.displayName,
        tagline: confirmed.tagline,
      };
      setProfileForm({ baseline: canonical, values: canonical });
      setProfileNotice("保存しました。");
      try {
        await refreshMessagingMemberProfiles();
      } catch {
        setProfileNotice("保存済み。トークの表示は再読み込みで反映されます。");
      }
    } catch (error) {
      setProfileError(
        error instanceof SumiProfileUpdateIndeterminateError
          ? "更新結果を確認できませんでした。再読み込みしてください。"
          : "プロフィールを更新できませんでした。",
      );
    } finally {
      setSavingProfile(false);
    }
  };

  const canonicalDisplayName = canonicalizeSumiDisplayName(
    profileForm.values.displayName,
  );
  const canonicalTagline = profileForm.values.tagline.trim();
  const displayNameValid =
    canonicalDisplayName.length > 0 &&
    hasSafeDisplayCharacters(canonicalDisplayName);
  const taglineValid = hasSafeDisplayCharacters(canonicalTagline);
  const dirty =
    profileForm.baseline !== null &&
    (canonicalDisplayName !== profileForm.baseline.displayName ||
      canonicalTagline !== profileForm.baseline.tagline);
  const profileUnavailable =
    loadingProfile || profileLoadError || profileForm.baseline === null;

  return (
    <Popover open={settingsOpen} onOpenChange={setSettingsOpen}>
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
      <PopoverContent
        side="top"
        align="start"
        aria-label="設定"
        className="w-80"
      >
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
              onKeyDown={(event) => {
                if (event.key === "Enter" && isImeComposing(event)) {
                  event.preventDefault();
                }
              }}
              className="px-2.5 pb-2"
            >
              <label
                htmlFor="sumi-settings-display-name"
                className="mb-1 block text-muted-foreground text-xs"
              >
                表示名
              </label>
              <input
                id="sumi-settings-display-name"
                value={profileForm.values.displayName}
                onChange={(event) => {
                  setProfileForm((current) => ({
                    ...current,
                    values: {
                      ...current.values,
                      displayName: clampCodePoints(
                        event.target.value,
                        MAX_DISPLAY_NAME_CODE_POINTS,
                      ),
                    },
                  }));
                  setProfileError(null);
                  setProfileNotice(null);
                }}
                disabled={profileUnavailable || savingProfile}
                aria-invalid={!displayNameValid || undefined}
                aria-describedby="sumi-settings-display-name-hint"
                autoComplete="name"
                className="w-full rounded-md border border-input bg-background px-2 py-1 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
              />
              <p
                id="sumi-settings-display-name-hint"
                className="mt-1 text-muted-foreground text-xs"
              >
                他の参加者に見える名前です（
                {codePointLength(profileForm.values.displayName)} /{" "}
                {MAX_DISPLAY_NAME_CODE_POINTS}）
              </p>
              <label
                htmlFor="sumi-settings-tagline"
                className="mt-2 mb-1 block text-muted-foreground text-xs"
              >
                ひとこと
              </label>
              <input
                id="sumi-settings-tagline"
                value={profileForm.values.tagline}
                onChange={(event) => {
                  setProfileForm((current) => ({
                    ...current,
                    values: {
                      ...current.values,
                      tagline: clampCodePoints(
                        event.target.value,
                        MAX_TAGLINE_CODE_POINTS,
                      ),
                    },
                  }));
                  setProfileError(null);
                  setProfileNotice(null);
                }}
                disabled={profileUnavailable || savingProfile}
                aria-invalid={!taglineValid || undefined}
                aria-describedby="sumi-settings-tagline-hint"
                placeholder="例: 開発"
                className="w-full rounded-md border border-input bg-background px-2 py-1 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
              />
              <p
                id="sumi-settings-tagline-hint"
                className="mt-1 text-muted-foreground text-xs"
              >
                担っていることを一行で。空でも構いません（
                {codePointLength(profileForm.values.tagline)} /{" "}
                {MAX_TAGLINE_CODE_POINTS}）
              </p>
              <div className="mt-2 flex items-center gap-2">
                <Button
                  type="submit"
                  size="sm"
                  disabled={
                    savingProfile ||
                    profileUnavailable ||
                    !dirty ||
                    !displayNameValid ||
                    !taglineValid
                  }
                >
                  {savingProfile ? "保存中" : "保存"}
                </Button>
                {loadingProfile ? (
                  <span role="status" className="text-muted-foreground text-xs">
                    読み込み中
                  </span>
                ) : null}
                {profileLoadError ? (
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    onClick={() =>
                      setProfileLoadAttempt((attempt) => attempt + 1)
                    }
                  >
                    再試行
                  </Button>
                ) : null}
              </div>
              {profileError ? (
                <p role="alert" className="mt-1 text-red-600 text-xs">
                  {profileError}
                </p>
              ) : null}
              {!displayNameValid && !profileUnavailable ? (
                <p role="alert" className="mt-1 text-red-600 text-xs">
                  表示名は1文字以上で入力してください。
                </p>
              ) : null}
              {!taglineValid && !profileUnavailable ? (
                <p role="alert" className="mt-1 text-red-600 text-xs">
                  ひとことは改行や制御文字を含めず入力してください。
                </p>
              ) : null}
              {profileLoadError ? (
                <p role="alert" className="mt-1 text-red-600 text-xs">
                  プロフィールを読み込めませんでした。
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
        {authenticated ? <ParticipantAppsMenu /> : null}
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
