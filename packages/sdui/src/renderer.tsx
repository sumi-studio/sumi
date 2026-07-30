import { Button } from "@sumi/ui/components/button";
import { cn } from "@sumi/ui/lib/utils";
import { AlertCircle, CalendarClock, Check, ListChecks } from "lucide-react";
import {
  type ConfirmCardProps,
  cardSchemas,
  type ListCardProps,
  parseCatalogSduiNode,
  type ReminderCardProps,
} from "./catalog";
import type { SduiNode } from "./schema";

export interface SduiViewProps {
  node: SduiNode;
  /** カード上のボタン押下。action 文字列をエージェントへ送り返す */
  onAction?: (action: string, label: string) => void;
  className?: string;
}

/**
 * 宣言データ → カタログ参照でレンダー。未知の type や不正な props は
 * エラーにせずフォールバックカードとして可視化する (会話を壊さない)。
 */
export function SduiView({ node, onAction, className }: SduiViewProps) {
  const parsedNode = parseCatalogSduiNode(node);
  const card = parsedNode ? (
    renderCard(parsedNode, onAction)
  ) : (
    <UnknownCard reason="invalid declaration" />
  );
  return (
    <div
      className={cn(
        "max-w-md rounded-2xl border border-neutral-200 bg-white p-4 shadow-[0_1px_4px_rgba(0,0,0,0.03)]",
        className,
      )}
    >
      {card}
    </div>
  );
}

function renderCard(node: SduiNode, onAction: SduiViewProps["onAction"]) {
  switch (node.type) {
    case "reminder": {
      const parsed = cardSchemas.reminder.safeParse(node.props);
      return parsed.success ? (
        <ReminderCard props={parsed.data} onAction={onAction} />
      ) : (
        <UnknownCard
          reason={`reminder: ${parsed.error.issues[0]?.message ?? "invalid"}`}
        />
      );
    }
    case "confirm": {
      const parsed = cardSchemas.confirm.safeParse(node.props);
      return parsed.success ? (
        <ConfirmCard props={parsed.data} onAction={onAction} />
      ) : (
        <UnknownCard
          reason={`confirm: ${parsed.error.issues[0]?.message ?? "invalid"}`}
        />
      );
    }
    case "list": {
      const parsed = cardSchemas.list.safeParse(node.props);
      return parsed.success ? (
        <ListCard props={parsed.data} />
      ) : (
        <UnknownCard
          reason={`list: ${parsed.error.issues[0]?.message ?? "invalid"}`}
        />
      );
    }
    default:
      return <UnknownCard reason={`未対応のカード種別: ${node.type}`} />;
  }
}

function CardButton({
  label,
  action,
  primary = false,
  onAction,
}: {
  label: string;
  action: string;
  primary?: boolean;
  onAction: SduiViewProps["onAction"];
}) {
  return (
    <Button
      variant={primary ? "default" : "outline"}
      size="sm"
      onClick={() => onAction?.(action, label)}
      disabled={!onAction}
      className="rounded-full px-3.5"
    >
      {label}
    </Button>
  );
}

function formatDateTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return iso;
  }
  return date.toLocaleString("ja-JP", {
    month: "numeric",
    day: "numeric",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function ReminderCard({
  props,
  onAction,
}: {
  props: ReminderCardProps;
  onAction: SduiViewProps["onAction"];
}) {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-start gap-3">
        <span className="flex size-9 shrink-0 items-center justify-center rounded-xl bg-neutral-100">
          <CalendarClock className="size-4.5 text-neutral-600" />
        </span>
        <div className="min-w-0">
          <p className="font-medium text-[15px] text-neutral-900">
            {props.title}
          </p>
          <p className="text-neutral-500 text-sm">{formatDateTime(props.at)}</p>
          {props.note && (
            <p className="mt-1 text-neutral-500 text-sm">{props.note}</p>
          )}
        </div>
      </div>
      {props.actions && props.actions.length > 0 && (
        <div className="flex gap-2">
          {props.actions.map((a, i) => (
            <CardButton
              key={a.action}
              label={a.label}
              action={a.action}
              primary={i === 0}
              onAction={onAction}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function ConfirmCard({
  props,
  onAction,
}: {
  props: ConfirmCardProps;
  onAction: SduiViewProps["onAction"];
}) {
  return (
    <div className="flex flex-col gap-3">
      <p className="font-medium text-[15px] text-neutral-900">{props.title}</p>
      {props.message && (
        <p className="text-neutral-600 text-sm leading-6">{props.message}</p>
      )}
      <div className="flex gap-2">
        <CardButton
          label={props.confirm.label}
          action={props.confirm.action}
          primary
          onAction={onAction}
        />
        <CardButton
          label={props.cancel.label}
          action={props.cancel.action}
          onAction={onAction}
        />
      </div>
    </div>
  );
}

function ListCard({ props }: { props: ListCardProps }) {
  return (
    <div className="flex flex-col gap-2">
      {props.title && (
        <div className="flex items-center gap-2">
          <ListChecks className="size-4 text-neutral-500" />
          <p className="font-medium text-[15px] text-neutral-900">
            {props.title}
          </p>
        </div>
      )}
      <ul className="flex flex-col gap-1.5">
        {props.items.map((item) => (
          <li
            key={item.text}
            className="flex items-start gap-2 text-[15px] text-neutral-700"
          >
            <span
              className={cn(
                "mt-1 flex size-4 shrink-0 items-center justify-center rounded-full border",
                item.done
                  ? "border-neutral-900 bg-neutral-900 text-white"
                  : "border-neutral-300 bg-white",
              )}
            >
              {item.done && <Check className="size-3" />}
            </span>
            <span className={cn(item.done && "text-neutral-400 line-through")}>
              {item.text}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function UnknownCard({ reason }: { reason: string }) {
  return (
    <div className="flex items-center gap-2 text-neutral-500 text-sm">
      <AlertCircle className="size-4 shrink-0" />
      <span>このカードを表示できません ({reason})</span>
    </div>
  );
}
