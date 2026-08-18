import { Check, Copy, Settings, X } from "lucide-react";
import { type ReactNode, useEffect, useState } from "react";
import { create } from "zustand";
import { clampCodePoints, codePointLength } from "../../lib/text-length";
import type { ParticipantRef } from "../model";
import { useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

/**
 * 個人設定の全画面オーバーレイ。左のセクションナビ + 右の内容で、Escで閉じる。
 *
 * ここに置くのは「自分についての申告」だけ。ワークスペースの運営設定は権限の
 * 話なので別の場所が持つ。名乗りはHumanもPersonalityAgentも同じ形で、同じ
 * validationを通って同じ表に載る（AX: 参加者の種別で扱いを変えない）。
 */

export type SettingsSection = "profile" | "account";

interface SettingsOverlayState {
  open: boolean;
  section: SettingsSection;
  openSettings(section?: SettingsSection): void;
  close(): void;
}

export const useSettingsOverlay = create<SettingsOverlayState>((set) => ({
  open: false,
  section: "profile",
  openSettings(section = "profile") {
    set({ open: true, section });
  },
  close() {
    set({ open: false });
  },
}));

const SECTION_LABEL: Record<SettingsSection, string> = {
  profile: "プロフィール",
  account: "アカウント",
};

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50";

/** 表示名の上限。サーバー（戸籍）の MaxHumanDisplayNameRunes と同じ。 */
const MAX_DISPLAY_NAME_CHARS = 80;
/** taglineの上限。サーバーの MaxTaglineChars と同じ。 */
const MAX_TAGLINE_CHARS = 100;

function participantID(ref: ParticipantRef): string {
  return ref.kind === "human" ? ref.humanId : ref.personalityAgentId;
}

function SectionButton({
  section,
  active,
  onSelect,
}: {
  section: SettingsSection;
  active: boolean;
  onSelect: (section: SettingsSection) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onSelect(section)}
      aria-current={active ? "page" : undefined}
      className={`block w-full rounded-md px-2.5 py-1.5 text-left text-[13px] transition-colors ${
        active
          ? "bg-accent font-medium text-foreground"
          : "text-muted-foreground hover:bg-accent/60 hover:text-foreground"
      }`}
    >
      {SECTION_LABEL[section]}
    </button>
  );
}

/** ラベル + 入力 + 補足。入力自体は呼び出し側が id で結び付ける。 */
function Field({
  id,
  label,
  hint,
  children,
}: {
  id: string;
  label: string;
  hint: string;
  children: ReactNode;
}) {
  return (
    <div>
      <label htmlFor={id} className="mb-1 block font-medium text-[12px]">
        {label}
      </label>
      {children}
      <p className="mt-1 text-[11px] text-muted-foreground">{hint}</p>
    </div>
  );
}

/** 名乗り: 表示名とひとこと。 */
function ProfileSection() {
  const selfKey = useMessaging((state) => state.selfKey);
  const member = useMessaging((state) => state.membersByKey[selfKey]);
  const updateProfile = useMessaging((state) => state.updateProfile);

  const canonicalName = member?.displayName ?? "";
  const canonicalTagline = member?.tagline ?? "";
  const [displayName, setDisplayName] = useState(canonicalName);
  const [tagline, setTagline] = useState(canonicalTagline);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState("");
  const [saved, setSaved] = useState(false);

  // 名乗りは別の経路（別のタブ、PA側の道具）でも変わる。正本が動いたら、手を
  // 付けていない欄だけ新しい値へ合わせ、編集中の欄は上書きしない — statusの
  // ひとことと同じ扱い。
  const [seen, setSeen] = useState({
    displayName: canonicalName,
    tagline: canonicalTagline,
  });
  if (seen.displayName !== canonicalName || seen.tagline !== canonicalTagline) {
    if (displayName === seen.displayName) setDisplayName(canonicalName);
    if (tagline === seen.tagline) setTagline(canonicalTagline);
    setSeen({ displayName: canonicalName, tagline: canonicalTagline });
  }
  const dirty = displayName !== canonicalName || tagline !== canonicalTagline;

  const save = async (event: React.FormEvent) => {
    event.preventDefault();
    if (busy || !dirty) return;
    setBusy(true);
    setFailed("");
    setSaved(false);
    try {
      await updateProfile({
        displayName: clampCodePoints(
          displayName.trim(),
          MAX_DISPLAY_NAME_CHARS,
        ),
        tagline: clampCodePoints(tagline.trim(), MAX_TAGLINE_CHARS),
      });
      setSaved(true);
    } catch {
      setFailed("保存できませんでした。表示名は1文字以上必要です");
    } finally {
      setBusy(false);
    }
  };

  return (
    <form onSubmit={save} className="space-y-5">
      <div className="flex items-center gap-3">
        <ParticipantAvatar
          participantKey={selfKey}
          name={displayName || "?"}
          size={56}
          src={member?.avatarUrl}
        />
        <p className="text-[11px] text-muted-foreground">
          顔写真はまだ差し替えられません
        </p>
      </div>
      <Field
        id="settings-display-name"
        label="表示名"
        hint={`他の参加者に見える名前です（${codePointLength(displayName)} / ${MAX_DISPLAY_NAME_CHARS}）`}
      >
        <input
          id="settings-display-name"
          value={displayName}
          onChange={(event) =>
            setDisplayName(
              clampCodePoints(event.target.value, MAX_DISPLAY_NAME_CHARS),
            )
          }
          disabled={busy}
          className={INPUT_CLASS}
        />
      </Field>
      <Field
        id="settings-tagline"
        label="ひとこと"
        hint={`担っていることを一行で（例: 秘書、開発）。空でも構いません（${codePointLength(tagline)} / ${MAX_TAGLINE_CHARS}）`}
      >
        <input
          id="settings-tagline"
          value={tagline}
          onChange={(event) =>
            setTagline(clampCodePoints(event.target.value, MAX_TAGLINE_CHARS))
          }
          disabled={busy}
          placeholder="例: 開発"
          className={INPUT_CLASS}
        />
      </Field>
      <div className="flex items-center gap-3">
        <button
          type="submit"
          disabled={busy || !dirty || displayName.trim() === ""}
          className="rounded-md bg-primary px-3 py-1.5 font-medium text-[12.5px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
        >
          保存
        </button>
        {failed ? (
          <span className="text-[12px] text-rose-500">{failed}</span>
        ) : saved && !dirty ? (
          <span className="flex items-center gap-1 text-[12px] text-muted-foreground">
            <Check className="size-3.5" />
            保存しました
          </span>
        ) : null}
      </div>
      <p className="text-[11px] text-muted-foreground/80">
        ここで名乗ったことは、人格agentが同じ道具で名乗るのと同じ扱いになります。
        参加者の種別で見た目が変わることはありません。
      </p>
    </form>
  );
}

/** 自分が誰として繋がっているかの確認。IDは表示名の代わりには使わない。 */
function AccountSection({ self }: { self: ParticipantRef }) {
  const id = participantID(self);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const timer = window.setTimeout(() => setCopied(false), 1_500);
    return () => window.clearTimeout(timer);
  }, [copied]);

  return (
    <div className="space-y-4">
      <div>
        <p className="mb-1 font-medium text-[12px]">参加者ID</p>
        <div className="flex items-center gap-2">
          <code className="min-w-0 flex-1 truncate rounded-md border border-border bg-muted/40 px-2.5 py-1.5 text-[12.5px]">
            {id}
          </code>
          <button
            type="button"
            onClick={() => {
              void navigator.clipboard?.writeText(id).then(() => {
                setCopied(true);
              });
            }}
            className="flex items-center gap-1.5 rounded-md border border-border px-2.5 py-1.5 text-[12.5px] hover:bg-accent"
          >
            {copied ? (
              <Check className="size-3.5" />
            ) : (
              <Copy className="size-3.5" />
            )}
            {copied ? "コピーしました" : "コピー"}
          </button>
        </div>
        <p className="mt-1 text-[11px] text-muted-foreground">
          問い合わせのときに使います。IDが名前の代わりになることはありません
        </p>
      </div>
    </div>
  );
}

/** サイドバー下部に置く導線。設定の中身はこのファイルに閉じている。 */
export function SettingsTrigger() {
  const openSettings = useSettingsOverlay((state) => state.openSettings);
  return (
    <button
      type="button"
      title="個人設定"
      aria-label="個人設定"
      onClick={() => openSettings("profile")}
      className="flex size-7 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
    >
      <Settings className="size-3.5" />
    </button>
  );
}

export function SettingsOverlay() {
  const open = useSettingsOverlay((state) => state.open);
  const section = useSettingsOverlay((state) => state.section);
  const close = useSettingsOverlay((state) => state.close);
  const openSettings = useSettingsOverlay((state) => state.openSettings);
  const self = useMessaging((state) => state.self);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, close]);

  if (!open || !self) return null;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="個人設定"
      className="fixed inset-0 z-50 flex bg-background"
    >
      <nav className="hidden w-56 shrink-0 flex-col overflow-y-auto border-border/70 border-r bg-muted/20 p-3 sm:flex">
        <p className="px-2.5 pt-2 pb-1 font-medium text-[11px] text-muted-foreground/80">
          ユーザー設定
        </p>
        {(["profile", "account"] as SettingsSection[]).map((candidate) => (
          <SectionButton
            key={candidate}
            section={candidate}
            active={section === candidate}
            onSelect={openSettings}
          />
        ))}
      </nav>
      <div className="scrollbar-ui min-w-0 flex-1 overflow-y-auto">
        <div className="mx-auto w-full max-w-2xl px-6 py-8">
          <div className="mb-6 flex items-start justify-between gap-4">
            <h1 className="font-semibold text-[18px]">
              {SECTION_LABEL[section]}
            </h1>
            <button
              type="button"
              aria-label="設定を閉じる"
              onClick={close}
              className="flex flex-col items-center gap-1 rounded-md p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
            >
              <X className="size-4" />
              <span className="text-[10px]">ESC</span>
            </button>
          </div>
          {section === "profile" ? <ProfileSection /> : null}
          {section === "account" ? <AccountSection self={self} /> : null}
        </div>
      </div>
    </div>
  );
}
