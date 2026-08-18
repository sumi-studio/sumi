import { Search } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import {
  EMOJI_CATEGORIES,
  type EmojiEntry,
  emojiName,
  searchEmojis,
} from "../emoji-data";
import { useRecentEmojis } from "../recent-emoji";

/**
 * リアクション用の絵文字ピッカー。
 *
 * 固定8個のパレットでは「その気持ちの絵文字が無い」で行き止まりになる。
 * 検索・最近使ったもの・カテゴリ一覧の3つの入り口を持たせ、どこからでも
 * 目的の絵文字に届くようにする。
 *
 * 開閉そのものはこのコンポーネントの責務ではない（呼び出し側が持つ）。
 */

const RECENT_SECTION_ID = "__recent__";

export function EmojiPicker({
  onSelect,
}: {
  onSelect: (emoji: string) => void;
}) {
  const recent = useRecentEmojis();
  const [query, setQuery] = useState("");
  const [preview, setPreview] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const sectionRefs = useRef(new Map<string, HTMLDivElement>());

  // ピッカーを開く操作の続きは検索。開いた時点で打ち始められるようにする。
  useEffect(() => {
    searchRef.current?.focus();
  }, []);

  const results = useMemo(() => searchEmojis(query), [query]);
  const searching = query.trim().length > 0;

  const jumpTo = (id: string) => {
    const section = sectionRefs.current.get(id);
    const container = scrollRef.current;
    if (!section || !container) return;
    container.scrollTop = section.offsetTop - container.offsetTop;
  };

  const choose = (emoji: string) => {
    onSelect(emoji);
  };

  const previewName = preview ? emojiName(preview) : null;

  return (
    <div className="flex h-[19rem] w-[19rem] flex-col">
      <div className="flex items-center gap-1.5 rounded-md border border-border bg-muted/40 px-2 py-1">
        <Search className="size-3.5 shrink-0 text-muted-foreground" />
        <input
          ref={searchRef}
          type="text"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            // IME変換確定のEnterは「決定」の意味を持たせない。
            if (isImeComposing(event)) return;
            if (event.key === "Enter" && results.length > 0) {
              event.preventDefault();
              choose(results[0].emoji);
            }
          }}
          placeholder="絵文字を検索"
          aria-label="絵文字を検索"
          className="w-full bg-transparent text-[13px] outline-none placeholder:text-muted-foreground/70"
        />
      </div>
      <div className="mt-1.5 flex min-h-0 flex-1 gap-1">
        {searching ? null : (
          <div className="flex w-8 shrink-0 flex-col gap-0.5 overflow-y-auto">
            {recent.length > 0 ? (
              <CategoryButton
                icon="🕘"
                label="最近使った絵文字"
                onClick={() => jumpTo(RECENT_SECTION_ID)}
              />
            ) : null}
            {EMOJI_CATEGORIES.map((category) => (
              <CategoryButton
                key={category.id}
                icon={category.icon}
                label={category.label}
                onClick={() => jumpTo(category.id)}
              />
            ))}
          </div>
        )}
        <div
          ref={scrollRef}
          className="scrollbar-ui min-h-0 flex-1 overflow-y-auto overscroll-contain pr-0.5"
        >
          {searching ? (
            results.length === 0 ? (
              <p className="px-1 py-6 text-center text-[12px] text-muted-foreground">
                見つかりませんでした
              </p>
            ) : (
              <Section
                id="__search__"
                label="検索結果"
                entries={results}
                onChoose={choose}
                onPreview={setPreview}
                sectionRefs={sectionRefs}
              />
            )
          ) : (
            <>
              {recent.length > 0 ? (
                <Section
                  id={RECENT_SECTION_ID}
                  label="最近使った絵文字"
                  entries={recent.map((emoji) => ({
                    emoji,
                    name: emojiName(emoji),
                    keywords: [],
                  }))}
                  onChoose={choose}
                  onPreview={setPreview}
                  sectionRefs={sectionRefs}
                />
              ) : null}
              {EMOJI_CATEGORIES.map((category) => (
                <Section
                  key={category.id}
                  id={category.id}
                  label={category.label}
                  entries={category.entries}
                  onChoose={choose}
                  onPreview={setPreview}
                  sectionRefs={sectionRefs}
                />
              ))}
            </>
          )}
        </div>
      </div>
      <div className="mt-1 flex h-6 items-center gap-1.5 border-border/60 border-t pt-1 text-[11px] text-muted-foreground">
        {preview ? (
          <>
            <span className="text-[15px] leading-none">{preview}</span>
            <span className="truncate">{previewName}</span>
          </>
        ) : (
          <span className="truncate">絵文字を選ぶ</span>
        )}
      </div>
    </div>
  );
}

function CategoryButton({
  icon,
  label,
  onClick,
}: {
  icon: string;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      className="flex size-7 shrink-0 items-center justify-center rounded-md text-[14px] transition-colors hover:bg-accent"
    >
      {icon}
    </button>
  );
}

function Section({
  id,
  label,
  entries,
  onChoose,
  onPreview,
  sectionRefs,
}: {
  id: string;
  label: string;
  entries: EmojiEntry[];
  onChoose: (emoji: string) => void;
  onPreview: (emoji: string | null) => void;
  sectionRefs: React.RefObject<Map<string, HTMLDivElement>>;
}) {
  return (
    <div
      ref={(node) => {
        if (node) sectionRefs.current.set(id, node);
        else sectionRefs.current.delete(id);
      }}
    >
      <p className="sticky top-0 z-1 bg-popover px-1 py-1 font-medium text-[10.5px] text-muted-foreground">
        {label}
      </p>
      <div className="grid grid-cols-7 gap-0.5 px-0.5 pb-1">
        {entries.map((item) => (
          <button
            key={`${id}:${item.emoji}`}
            type="button"
            title={item.name}
            aria-label={item.name}
            onClick={() => onChoose(item.emoji)}
            onMouseEnter={() => onPreview(item.emoji)}
            onFocus={() => onPreview(item.emoji)}
            className="flex size-8 items-center justify-center rounded-md text-[17px] transition-colors hover:bg-accent"
          >
            {item.emoji}
          </button>
        ))}
      </div>
    </div>
  );
}
