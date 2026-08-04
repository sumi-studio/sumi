import { Check, Copy, Image as ImageIcon, Settings, X } from "lucide-react";
import { type ReactNode, useEffect, useRef, useState } from "react";
import { create } from "zustand";
import { MAX_ATTACHMENT_BYTES, type ParticipantRef } from "../model";
import { useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

/**
 * 設定の全画面オーバーレイ。左のセクションナビ + 右の内容で、Escで閉じる。
 *
 * 個人設定（自分の名乗り）とワークスペース管理は別のグループとして並べる。
 * 一般の利用者に見えるのは前者だけで、後者は権限を持つ人にだけ現れる。
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

/** サイドバー下部のメニューに置く導線。詳細はこのファイルに閉じている。 */
export function SettingsMenuItem({ onOpened }: { onOpened?: () => void }) {
  const openSettings = useSettingsOverlay((state) => state.openSettings);
  return (
    <button
      type="button"
      onClick={() => {
        openSettings("profile");
        onOpened?.();
      }}
      className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] hover:bg-accent"
    >
      <Settings className="size-3.5 text-muted-foreground" />
      個人設定
    </button>
  );
}

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

/**
 * 画像の差し替え。送信前の添付と同じ経路（uploadAttachment）で預け、
 * 返ってきたidを保存時にプロフィールへ結びつける。
 */
function ImagePicker({
  label,
  hint,
  preview,
  round,
  disabled,
  onPicked,
  onCleared,
}: {
  label: string;
  hint: string;
  preview: ReactNode;
  round: boolean;
  disabled: boolean;
  onPicked: (attachmentId: string, url: string) => void;
  onCleared: () => void;
}) {
  const uploadAttachment = useMessaging((state) => state.uploadAttachment);
  const inputRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState("");

  const pick = async (file: File | undefined) => {
    if (!file) return;
    setFailed("");
    if (!file.type.startsWith("image/")) {
      setFailed("画像ファイルを選んでください");
      return;
    }
    if (file.size > MAX_ATTACHMENT_BYTES) {
      setFailed("画像が大きすぎます");
      return;
    }
    setBusy(true);
    try {
      const attachment = await uploadAttachment(file);
      onPicked(attachment.attachmentId, attachment.url);
    } catch {
      setFailed("画像を預けられませんでした");
    } finally {
      setBusy(false);
      if (inputRef.current) inputRef.current.value = "";
    }
  };

  return (
    <div>
      <p className="mb-1 font-medium text-[12px]">{label}</p>
      <div className="flex items-center gap-3">
        <span className={round ? "" : "min-w-0 flex-1"}>{preview}</span>
        <span className="flex flex-col items-start gap-1">
          <button
            type="button"
            disabled={disabled || busy}
            onClick={() => inputRef.current?.click()}
            className="flex items-center gap-1.5 rounded-md border border-border px-2.5 py-1 text-[12.5px] hover:bg-accent disabled:opacity-50"
          >
            <ImageIcon className="size-3.5" />
            {busy ? "アップロード中…" : "画像を選ぶ"}
          </button>
          <button
            type="button"
            disabled={disabled || busy}
            onClick={() => {
              setFailed("");
              onCleared();
            }}
            className="rounded-md px-2.5 py-1 text-[12px] text-muted-foreground hover:bg-accent disabled:opacity-50"
          >
            外す
          </button>
        </span>
      </div>
      <input
        ref={inputRef}
        type="file"
        accept="image/png,image/jpeg,image/gif,image/webp"
        className="hidden"
        aria-label={label}
        onChange={(event) => void pick(event.target.files?.[0])}
      />
      <p className="mt-1 text-[11px] text-muted-foreground">{failed || hint}</p>
    </div>
  );
}

/** 名乗り: 表示名・tagline・アバター・ヘッダー画像。 */
function ProfileSection() {
  const selfKey = useMessaging((state) => state.selfKey);
  const member = useMessaging((state) => state.membersByKey[selfKey]);
  const updateProfile = useMessaging((state) => state.updateProfile);

  const [displayName, setDisplayName] = useState(member?.displayName ?? "");
  const [tagline, setTagline] = useState(member?.tagline ?? "");
  const [avatar, setAvatar] = useState<{
    attachmentId: string;
    url: string | undefined;
  }>({
    attachmentId: member?.avatarAttachmentId ?? "",
    url: member?.avatarUrl,
  });
  const [banner, setBanner] = useState<{
    attachmentId: string;
    url: string | undefined;
  }>({
    attachmentId: member?.bannerAttachmentId ?? "",
    url: member?.bannerUrl,
  });
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState("");
  const [saved, setSaved] = useState(false);

  const dirty =
    displayName !== (member?.displayName ?? "") ||
    tagline !== (member?.tagline ?? "") ||
    avatar.attachmentId !== (member?.avatarAttachmentId ?? "") ||
    banner.attachmentId !== (member?.bannerAttachmentId ?? "");

  const save = async (event: React.FormEvent) => {
    event.preventDefault();
    if (busy || !dirty) return;
    setBusy(true);
    setFailed("");
    setSaved(false);
    try {
      await updateProfile({
        displayName: displayName.trim(),
        tagline: tagline.trim(),
        avatarAttachmentId: avatar.attachmentId,
        bannerAttachmentId: banner.attachmentId,
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
      <ImagePicker
        label="ヘッダー画像"
        hint="プロフィールカードの上に出ます"
        round={false}
        disabled={busy}
        preview={
          banner.url ? (
            <img
              src={banner.url}
              alt=""
              className="h-16 w-full rounded-md border border-border/70 object-cover"
            />
          ) : (
            <span className="block h-16 w-full rounded-md border border-border/70 border-dashed bg-muted/40" />
          )
        }
        onPicked={(attachmentId, url) => setBanner({ attachmentId, url })}
        onCleared={() => setBanner({ attachmentId: "", url: undefined })}
      />
      <ImagePicker
        label="アバター"
        hint="メンバーリストと発言の横に出ます"
        round
        disabled={busy}
        preview={
          <ParticipantAvatar
            participantKey={selfKey}
            name={displayName || "?"}
            size={56}
            src={avatar.url}
          />
        }
        onPicked={(attachmentId, url) => setAvatar({ attachmentId, url })}
        onCleared={() => setAvatar({ attachmentId: "", url: undefined })}
      />
      <Field
        id="settings-display-name"
        label="表示名"
        hint="他の参加者に見える名前です"
      >
        <input
          id="settings-display-name"
          value={displayName}
          onChange={(event) => setDisplayName(event.target.value)}
          disabled={busy}
          maxLength={80}
          className={INPUT_CLASS}
        />
      </Field>
      <Field
        id="settings-tagline"
        label="ひとこと"
        hint="担っていることを一行で（例: 秘書、開発）。空でも構いません"
      >
        <input
          id="settings-tagline"
          value={tagline}
          onChange={(event) => setTagline(event.target.value)}
          disabled={busy}
          maxLength={100}
          placeholder="例: 開発"
          className={INPUT_CLASS}
        />
      </Field>
      <div className="flex items-center gap-3">
        <button
          type="submit"
          disabled={busy || !dirty || !displayName.trim()}
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
      aria-label="設定"
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
