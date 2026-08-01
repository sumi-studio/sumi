import { useNavigate } from "@tanstack/react-router";
import { DoorOpen, Hash } from "lucide-react";
import { useState } from "react";
import type { PlaceKey, StatusKind } from "../model";
import { participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

const STATUS_LABEL: Record<StatusKind, string> = {
  available: "対応可能",
  busy: "取り込み中",
  away: "離席中",
};

function Badge({ count, urgent }: { count: number; urgent: boolean }) {
  if (count <= 0) return null;
  return (
    <span
      className={`ml-auto rounded-full px-1.5 py-px font-semibold text-[10px] tabular-nums ${
        urgent
          ? "bg-rose-500 text-white"
          : "bg-muted-foreground/20 text-foreground"
      }`}
    >
      {count > 99 ? "99+" : count}
    </span>
  );
}

function PlaceRow({
  placeKey: key,
  label,
  icon,
  unread,
  mentions,
}: {
  placeKey: PlaceKey;
  label: React.ReactNode;
  icon: React.ReactNode;
  unread: number;
  mentions: number;
}) {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const placeNavigate = usePlaceNavigate();
  const active = activePlaceKey === key;
  return (
    <button
      type="button"
      onClick={() => placeNavigate(key)}
      className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors ${
        active
          ? "bg-accent text-foreground"
          : unread > 0
            ? "font-medium text-foreground hover:bg-accent/60"
            : "text-muted-foreground hover:bg-accent/60 hover:text-foreground"
      }`}
    >
      {icon}
      <span className="min-w-0 flex-1 truncate">{label}</span>
      <Badge count={mentions > 0 ? mentions : unread} urgent={mentions > 0} />
    </button>
  );
}

export function Sidebar() {
  const workspaces = useMessaging((state) => state.workspaces);
  const channels = useMessaging((state) => state.channels);
  const dms = useMessaging((state) => state.dms);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const unreadCountByPlace = useMessaging((state) => state.unreadCountByPlace);
  const mentionCountByPlace = useMessaging(
    (state) => state.mentionCountByPlace,
  );
  const selfKey = useMessaging((state) => state.selfKey);
  const self = useMessaging((state) => state.self);
  const setStatus = useMessaging((state) => state.setStatus);
  const employedAgents = useMessaging((state) => state.employedAgents);
  const navigate = useNavigate();
  const [statusMenuOpen, setStatusMenuOpen] = useState(false);

  const selfProfile = self ? membersByKey[selfKey] : undefined;
  const selfStatus = statusByKey[selfKey];

  return (
    <aside className="flex w-60 shrink-0 flex-col border-border/70 border-r bg-muted/20">
      <div className="flex h-12 shrink-0 items-center border-border/70 border-b px-4">
        <span className="truncate font-semibold text-[14px]">
          {workspaces[0]?.name ?? "Sumi"}
        </span>
      </div>
      <nav className="scrollbar-ui min-h-0 flex-1 overflow-y-auto p-2">
        <p className="px-2 pt-2 pb-1 font-medium text-[11px] text-muted-foreground/80">
          チャンネル
        </p>
        {channels.map((channel) => {
          const key = `channel:${channel.channelId}`;
          const unread = unreadCountByPlace[key] ?? 0;
          const mentions = mentionCountByPlace[key] ?? 0;
          return (
            <PlaceRow
              key={key}
              placeKey={key}
              label={channel.name}
              icon={<Hash className="size-3.5 shrink-0 opacity-60" />}
              unread={unread}
              mentions={mentions}
            />
          );
        })}
        <p className="px-2 pt-4 pb-1 font-medium text-[11px] text-muted-foreground/80">
          ダイレクトメッセージ
        </p>
        {dms.map((dm) => {
          const key = `${dm.kind}:${dm.dmId}`;
          const others = dm.participants.filter(
            (ref) => participantKey(ref) !== selfKey,
          );
          const first = others[0];
          const firstKey = first ? participantKey(first) : "";
          const name = others
            .map(
              (ref) => membersByKey[participantKey(ref)]?.displayName ?? "不明",
            )
            .join("、");
          const unread = unreadCountByPlace[key] ?? 0;
          return (
            <PlaceRow
              key={key}
              placeKey={key}
              label={name}
              icon={
                <ParticipantAvatar
                  participantKey={firstKey}
                  name={membersByKey[firstKey]?.displayName ?? "?"}
                  size={18}
                  status={statusByKey[firstKey]?.status}
                />
              }
              unread={unread}
              mentions={unread}
            />
          );
        })}
        {employedAgents.length > 0 ? (
          <>
            <p className="flex items-center gap-1 px-2 pt-4 pb-1 font-medium text-[11px] text-muted-foreground/80">
              直通
              <span className="text-[10px] text-muted-foreground/50">
                — 生の回線
              </span>
            </p>
            {employedAgents.map((agent) => {
              const key = participantKey(agent);
              const member = membersByKey[key];
              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => void navigate({ to: "/direct" })}
                  className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] text-muted-foreground transition-colors hover:bg-accent/60 hover:text-foreground"
                >
                  <DoorOpen className="size-3.5 shrink-0 opacity-60" />
                  <span className="min-w-0 flex-1 truncate">
                    {member?.displayName ?? "不明"}
                  </span>
                </button>
              );
            })}
          </>
        ) : null}
      </nav>
      <div className="relative shrink-0 border-border/70 border-t p-2">
        {statusMenuOpen ? (
          <div className="absolute bottom-full left-2 z-10 mb-1 w-52 rounded-lg border border-border bg-background p-1 shadow-md">
            {(Object.keys(STATUS_LABEL) as StatusKind[]).map((kind) => (
              <button
                key={kind}
                type="button"
                onClick={() => {
                  setStatus(kind, kind === "busy" ? "取り込み中" : "");
                  setStatusMenuOpen(false);
                }}
                className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] hover:bg-accent ${
                  selfStatus?.status === kind ? "font-medium" : ""
                }`}
              >
                <span
                  className={`size-2 rounded-full ${
                    kind === "available"
                      ? "bg-emerald-500"
                      : kind === "busy"
                        ? "bg-rose-500"
                        : "bg-amber-400"
                  }`}
                />
                {STATUS_LABEL[kind]}
              </button>
            ))}
            <p className="px-2 pt-1 pb-0.5 text-[10px] text-muted-foreground/70">
              ステータスは自己申告。誰かが勝手に晒すことはありません
            </p>
          </div>
        ) : null}
        <button
          type="button"
          onClick={() => setStatusMenuOpen((open) => !open)}
          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-accent/60"
        >
          <ParticipantAvatar
            participantKey={selfKey}
            name={selfProfile?.displayName ?? "?"}
            size={26}
            status={selfStatus?.status ?? "available"}
          />
          <span className="min-w-0 flex-1">
            <span className="block truncate font-medium text-[13px]">
              {selfProfile?.displayName ?? "…"}
            </span>
            <span className="block truncate text-[11px] text-muted-foreground">
              {selfStatus ? STATUS_LABEL[selfStatus.status] : "対応可能"}
              {selfStatus?.note ? ` — ${selfStatus.note}` : ""}
            </span>
          </span>
        </button>
      </div>
    </aside>
  );
}
