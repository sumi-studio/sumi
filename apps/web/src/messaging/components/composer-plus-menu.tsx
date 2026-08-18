import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import { Plus } from "lucide-react";
import type { ComponentType, RefObject } from "react";
import { useState } from "react";

/**
 * composerの「＋」メニュー。添付・挿入・作成の入口をここ1つに集める。
 *
 * 項目は呼び出し側が配列で渡すだけにしてある。入口が増えたときに
 * ツールバーへボタンを1つずつ生やさずに済ませるのが目的。
 * 開閉はこのコンポーネントの中だけで完結させ、後でメニュー基盤を差し替えても
 * 呼び出し側に波及しないようにする。
 */
export interface ComposerPlusMenuItem {
  id: string;
  label: string;
  /** 行の右端に出す短い説明。使えない項目にはその理由を書く。 */
  hint: string;
  icon: ComponentType<{ className?: string }>;
  disabled?: boolean;
  onSelect?: () => void;
}

export function ComposerPlusMenu({
  items,
  /** 閉じた後にフォーカスを戻す先。入力の続きを妨げないため通常は入力欄。 */
  finalFocusRef,
}: {
  items: ComposerPlusMenuItem[];
  finalFocusRef?: RefObject<HTMLElement | null>;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <button
            type="button"
            title="添付・挿入"
            aria-label="作成メニューを開く"
            className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          />
        }
      >
        <Plus className="size-4" />
      </PopoverTrigger>
      <PopoverContent
        side="top"
        align="start"
        finalFocus={finalFocusRef}
        className="w-64 p-1"
      >
        <ul className="flex flex-col">
          {items.map((item) => {
            const Icon = item.icon;
            return (
              <li key={item.id}>
                <button
                  type="button"
                  disabled={item.disabled}
                  onClick={() => {
                    item.onSelect?.();
                    setOpen(false);
                  }}
                  className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors enabled:hover:bg-accent disabled:cursor-default disabled:opacity-50"
                >
                  <Icon className="size-3.5 shrink-0 text-muted-foreground" />
                  <span className="shrink-0 font-medium">{item.label}</span>
                  <span className="ml-auto truncate text-[11px] text-muted-foreground">
                    {item.hint}
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      </PopoverContent>
    </Popover>
  );
}
