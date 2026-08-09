import { Loader2, Search } from "lucide-react";
import {
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import { isImeComposing } from "../../lib/ime";
import type {
  ChannelSummary,
  DmSummary,
  MemberProfile,
  MessageSearchResult,
  ParticipantKey,
  Place,
  PlaceKey,
} from "../model";
import { participantKey, placeKey } from "../model";
import { useMessaging } from "../store";
import { useOverlayPanel } from "./overlay";

const SEARCH_DEBOUNCE_MS = 300;

const RESULT_TIME_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "numeric",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

/** 検索結果のplace表示名。usePlaceDisplayと同じ解決規則（DMは相手の名前）。 */
function placeLabel(
  place: Place,
  channels: ChannelSummary[],
  dms: DmSummary[],
  membersByKey: Record<ParticipantKey, MemberProfile>,
  selfKey: ParticipantKey,
): string {
  if (place.kind === "channel") {
    const channel = channels.find(
      (entry) => entry.channelId === place.channelId,
    );
    return `# ${channel?.name ?? "不明"}`;
  }
  const dm = dms.find(
    (entry) => entry.kind === place.kind && entry.dmId === place.dmId,
  );
  if (!dm) return "DM";
  return dm.participants
    .filter((ref) => participantKey(ref) !== selfKey)
    .map((ref) => membersByKey[participantKey(ref)]?.displayName ?? "不明")
    .join("、");
}

/**
 * snippet内の最初の一致を強調する。lowercase時に長さが変わる文字を含む場合は
 * 位置がずれるため強調をあきらめてそのまま表示する（表示だけの問題）。
 */
function highlightSnippet(snippet: string, query: string): ReactNode {
  const folded = snippet.toLowerCase();
  const foldedQuery = query.toLowerCase();
  if (
    !foldedQuery ||
    folded.length !== snippet.length ||
    foldedQuery.length !== query.length
  ) {
    return snippet;
  }
  const index = folded.indexOf(foldedQuery);
  if (index < 0) return snippet;
  return (
    <>
      {snippet.slice(0, index)}
      <span className="rounded-[2px] bg-primary/15 font-medium text-foreground">
        {snippet.slice(index, index + query.length)}
      </span>
      {snippet.slice(index + query.length)}
    </>
  );
}

/**
 * ヘッダーの検索ボックス。300msデバウンスで可視なplace全体を検索し、
 * 結果クリックで該当placeへ遷移+該当メッセージへジャンプする。
 * Escで閉じる。IME変換確定のEnterは検索を発火しない（isComposing）。
 */
export function MessageSearch({
  onJump,
}: {
  onJump: (jump: {
    placeKey: PlaceKey;
    seq: number;
    messageId: string;
  }) => void;
}) {
  const searchMessages = useMessaging((state) => state.searchMessages);
  const channels = useMessaging((state) => state.channels);
  const dms = useMessaging((state) => state.dms);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const selfKey = useMessaging((state) => state.selfKey);

  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [searching, setSearching] = useState(false);
  const [results, setResults] = useState<MessageSearchResult[] | null>(null);
  const timerRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);

  const runSearch = useCallback(
    async (raw: string) => {
      const trimmed = raw.trim();
      const requestId = ++requestIdRef.current;
      if (!trimmed) {
        setResults(null);
        setSearching(false);
        return;
      }
      setSearching(true);
      try {
        const found = await searchMessages(trimmed);
        if (requestIdRef.current === requestId) setResults(found);
      } catch {
        if (requestIdRef.current === requestId) setResults([]);
      } finally {
        if (requestIdRef.current === requestId) setSearching(false);
      }
    },
    [searchMessages],
  );

  const scheduleSearch = useCallback(
    (raw: string) => {
      if (timerRef.current) window.clearTimeout(timerRef.current);
      timerRef.current = window.setTimeout(() => {
        void runSearch(raw);
      }, SEARCH_DEBOUNCE_MS);
    },
    [runSearch],
  );

  useEffect(() => {
    return () => {
      if (timerRef.current) window.clearTimeout(timerRef.current);
    };
  }, []);

  const close = useCallback(() => {
    if (timerRef.current) window.clearTimeout(timerRef.current);
    requestIdRef.current += 1;
    setOpen(false);
    setSearching(false);
  }, []);

  const trimmed = query.trim();
  const showPanel = open && trimmed !== "";

  // 外側クリック・Escape・排他・ホイール透過は共通のオーバーレイ規律に任せる。
  const overlay = useOverlayPanel<HTMLInputElement>({
    open: showPanel,
    onOpenChange: (next) => {
      if (!next) close();
    },
  });

  return (
    <div className="relative">
      <div className="relative">
        <Search className="-translate-y-1/2 pointer-events-none absolute top-1/2 left-2 size-3.5 text-muted-foreground" />
        <input
          ref={overlay.triggerProps.ref}
          value={query}
          placeholder="検索"
          maxLength={100}
          onChange={(event) => {
            setQuery(event.target.value);
            setOpen(true);
            scheduleSearch(event.target.value);
          }}
          onFocus={() => {
            if (query.trim()) setOpen(true);
          }}
          onKeyDown={(event) => {
            if (isImeComposing(event)) return;
            if (event.key === "Enter") {
              event.preventDefault();
              if (timerRef.current) window.clearTimeout(timerRef.current);
              setOpen(true);
              void runSearch(query);
              return;
            }
            if (event.key === "Escape") {
              // 閉じるのはオーバーレイ規律側（フォーカスは検索欄に残す）。
              event.preventDefault();
              close();
            }
          }}
          // フォーカスで幅を変えない。右隣のアイコンが動くと押し損ねる。
          className="h-7 w-36 rounded-md border border-border bg-background pr-2 pl-7 text-[12px] outline-none focus-visible:border-ring/60 sm:w-44"
        />
      </div>
      {showPanel ? (
        <div
          {...overlay.panelProps}
          className="absolute top-full right-0 z-20 mt-1 max-h-[60vh] w-96 overflow-y-auto rounded-lg border border-border bg-background p-1 shadow-md"
        >
          <p className="flex items-center gap-1.5 px-2 pt-1.5 pb-1 font-medium text-[11px] text-muted-foreground">
            「{trimmed}」の検索結果
            {searching ? (
              <Loader2 className="size-3 animate-spin text-muted-foreground" />
            ) : null}
          </p>
          {results === null ? null : results.length === 0 && !searching ? (
            <p className="px-2 pb-2 text-[12px] text-muted-foreground/70">
              一致するメッセージはありません
            </p>
          ) : (
            results.map((result) => {
              const key = placeKey(result.place);
              const authorName =
                membersByKey[participantKey(result.author)]?.displayName ??
                "不明";
              return (
                <button
                  key={result.messageId}
                  type="button"
                  onClick={() => {
                    close();
                    onJump({
                      placeKey: key,
                      seq: result.seq,
                      messageId: result.messageId,
                    });
                  }}
                  className="block w-full rounded-md px-2 py-1.5 text-left hover:bg-accent/60"
                >
                  <span className="flex items-baseline gap-1.5">
                    <span className="max-w-[45%] truncate font-medium text-[11px] text-muted-foreground">
                      {placeLabel(
                        result.place,
                        channels,
                        dms,
                        membersByKey,
                        selfKey,
                      )}
                    </span>
                    <span className="truncate text-[11px] text-muted-foreground/80">
                      {authorName}
                    </span>
                    <span className="ml-auto shrink-0 text-[10px] text-muted-foreground/60">
                      {RESULT_TIME_FORMAT.format(result.createdAt)}
                    </span>
                  </span>
                  <span className="mt-0.5 line-clamp-2 block text-[12.5px] text-foreground/90">
                    {highlightSnippet(result.snippet, trimmed)}
                  </span>
                </button>
              );
            })
          )}
        </div>
      ) : null}
    </div>
  );
}
