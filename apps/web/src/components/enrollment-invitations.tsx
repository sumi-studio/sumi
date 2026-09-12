import { Button } from "@sumi/ui/components/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@sumi/ui/components/sheet";
import { Check, Copy, LoaderCircle } from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import {
  createEnrollmentInvitation,
  type EnrollmentInvitation,
  listEnrollmentInvitations,
  revokeEnrollmentInvitation,
} from "../auth/enrollment-invitations";
import { AuthAPIError } from "../auth/session-client";

export function EnrollmentInvitations({
  open,
  onOpenChange,
  onCapabilityChange,
}: {
  open: boolean;
  onOpenChange(open: boolean): void;
  onCapabilityChange(allowed: boolean): void;
}) {
  const [invitations, setInvitations] = useState<EnrollmentInvitation[]>([]);
  const [email, setEmail] = useState("");
  const [days, setDays] = useState("7");
  const [link, setLink] = useState("");
  const [copied, setCopied] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const linkInput = useRef<HTMLInputElement>(null);
  const mounted = useRef(true);
  const visibleGeneration = useRef(0);
  const visible = useRef(open);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Reopening and explicit retry refresh invitation metadata.
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    void listEnrollmentInvitations(controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        onCapabilityChange(true);
        setInvitations(result.invitations);
        setError(null);
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted) return;
        if (
          reason instanceof AuthAPIError &&
          (reason.status === 401 || reason.status === 403)
        )
          onCapabilityChange(false);
        else setError("招待を読み込めませんでした。");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [open, attempt, onCapabilityChange]);
  useEffect(() => {
    visible.current = open;
    visibleGeneration.current += 1;
    if (!open) {
      setLink("");
      setCopied(false);
    }
  }, [open]);

  async function create(event: FormEvent) {
    event.preventDefault();
    if (busy) return;
    const generation = visibleGeneration.current;
    setBusy("create");
    setError(null);
    try {
      const result = await createEnrollmentInvitation({
        ...(email.trim() ? { email: email.trim() } : {}),
        expiresInSeconds: Number(days) * 86400,
      });
      if (!mounted.current) return;
      setInvitations((rows) => [result.invitation, ...rows]);
      if (visible.current && generation === visibleGeneration.current)
        setLink(`${location.origin}/#invite=${result.token}`);
      setCopied(false);
      setEmail("");
    } catch {
      if (mounted.current)
        setError(
          "作成結果を確認できませんでした。招待一覧を再読み込みして確認してください。",
        );
    } finally {
      if (mounted.current) setBusy(null);
    }
  }
  async function revoke(id: string) {
    if (busy) return;
    setBusy(id);
    setError(null);
    try {
      await revokeEnrollmentInvitation(id);
      if (mounted.current) setAttempt((value) => value + 1);
    } catch {
      if (mounted.current)
        setError(
          "取消結果を確認できませんでした。招待一覧を再読み込みしてください。",
        );
    } finally {
      if (mounted.current) setBusy(null);
    }
  }
  async function copy() {
    try {
      await navigator.clipboard.writeText(link);
      setCopied(true);
    } catch {
      linkInput.current?.focus();
      linkInput.current?.select();
      setError("リンクを選択しました。コピーして相手に送ってください。");
    }
  }
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="flex w-full flex-col gap-0 overflow-y-auto sm:max-w-xl">
        <SheetHeader className="px-6 pt-8 pb-7">
          <SheetTitle className="text-xl">Sumiに招待する</SheetTitle>
          <SheetDescription className="leading-6">
            リンクを渡した相手が、Sumiを使い始められます。Workspaceへの参加は、別途相手が確認します。
          </SheetDescription>
        </SheetHeader>
        <div className="space-y-8 px-6 pb-8">
          <form onSubmit={(event) => void create(event)} className="space-y-5">
            <div className="space-y-2">
              <label htmlFor="enrollment-email" className="text-sm">
                相手のメールアドレス{" "}
                <span className="text-muted-foreground">任意</span>
              </label>
              <input
                id="enrollment-email"
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="指定すると、このアドレスの本人だけが使えます"
                className="h-11 w-full border-border border-b bg-transparent text-sm outline-none placeholder:text-muted-foreground/60 focus-visible:border-foreground"
              />
            </div>
            <div className="flex items-center justify-between gap-4">
              <label className="flex items-center gap-2 text-sm">
                有効期限
                <select
                  value={days}
                  onChange={(event) => setDays(event.target.value)}
                  className="rounded-md border border-border bg-background px-2 py-1.5"
                >
                  <option value="1">1日</option>
                  <option value="7">7日</option>
                  <option value="30">30日</option>
                </select>
              </label>
              <Button type="submit" disabled={busy !== null || loading}>
                {busy === "create" && (
                  <LoaderCircle className="size-4 animate-spin" />
                )}
                招待リンクを作る
              </Button>
            </div>
            <p className="text-muted-foreground text-xs">
              1人が受け取ると使用済みになります。受け取る前なら取り消せます。
            </p>
          </form>
          {link && (
            <section className="space-y-2" aria-label="作成した招待リンク">
              <label htmlFor="enrollment-link" className="font-medium text-sm">
                リンクをコピーして送ってください
              </label>
              <div className="flex gap-2">
                <input
                  ref={linkInput}
                  id="enrollment-link"
                  readOnly
                  value={link}
                  onFocus={(event) => event.target.select()}
                  className="min-w-0 flex-1 rounded-md border border-border bg-background px-3 text-sm"
                />
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => void copy()}
                  aria-label="招待リンクをコピー"
                >
                  {copied ? (
                    <Check className="size-4" />
                  ) : (
                    <Copy className="size-4" />
                  )}
                </Button>
              </div>
              <p className="text-muted-foreground text-xs" role="status">
                {copied
                  ? "コピーしました。"
                  : "このリンクは画面を閉じると再表示できません。"}
              </p>
            </section>
          )}
          {error && (
            <div role="alert" className="text-sm">
              <p>{error}</p>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setAttempt((value) => value + 1)}
              >
                一覧を再読み込み
              </Button>
            </div>
          )}
          <section aria-label="発行した招待">
            <h3 className="mb-3 font-medium text-sm">発行した招待</h3>
            {loading ? (
              <p role="status" className="text-muted-foreground text-sm">
                読み込んでいます…
              </p>
            ) : invitations.length === 0 ? (
              <p className="text-muted-foreground text-sm">
                まだ招待を発行していません。
              </p>
            ) : (
              <ul className="divide-y divide-border">
                {invitations.map((invitation) => {
                  const status = invitation.revokedAt
                    ? "取消済み"
                    : invitation.consumedAt
                      ? "受取済み"
                      : Date.parse(invitation.expiresAt) <= Date.now()
                        ? "期限切れ"
                        : "未受取";
                  return (
                    <li
                      key={invitation.id}
                      className="flex items-center justify-between gap-4 py-4"
                    >
                      <div className="min-w-0">
                        <p className="truncate text-sm">
                          {invitation.email ?? "リンクを知っている相手"}
                        </p>
                        <p className="mt-1 text-muted-foreground text-xs">
                          {status} ·{" "}
                          {new Date(invitation.expiresAt).toLocaleDateString(
                            "ja-JP",
                          )}
                          まで
                        </p>
                      </div>
                      {status === "未受取" && (
                        <Button
                          variant="ghost"
                          size="sm"
                          disabled={busy !== null}
                          onClick={() => void revoke(invitation.id)}
                        >
                          取り消す
                        </Button>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
        </div>
      </SheetContent>
    </Sheet>
  );
}
