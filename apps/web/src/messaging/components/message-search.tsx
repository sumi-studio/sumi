import { Loader2, Search } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
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

const SEARCH_DEBOUNCE_MS = 300;
const MAX_SEARCH_QUERY_BYTES = 200;
const encoder = new TextEncoder();
const resultTime = new Intl.DateTimeFormat("ja-JP", {
  month: "numeric",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

function limitQuery(value: string): string {
  if (encoder.encode(value).length <= MAX_SEARCH_QUERY_BYTES) return value;
  let bytes = 0;
  let result = "";
  for (const character of value) {
    const size = encoder.encode(character).length;
    if (bytes + size > MAX_SEARCH_QUERY_BYTES) break;
    result += character;
    bytes += size;
  }
  return result;
}

function labelForPlace(
  place: Place,
  channels: ChannelSummary[],
  dms: DmSummary[],
  members: Record<ParticipantKey, MemberProfile>,
  selfKey: ParticipantKey,
): string {
  if (place.kind === "channel") {
    return `# ${channels.find((entry) => entry.channelId === place.channelId)?.name ?? "不明"}`;
  }
  const dm = dms.find(
    (entry) => entry.kind === place.kind && entry.dmId === place.dmId,
  );
  if (!dm) return "DM";
  return dm.participants
    .filter((entry) => participantKey(entry) !== selfKey)
    .map((entry) => members[participantKey(entry)]?.displayName ?? "不明")
    .join("、");
}

function highlightedSnippet(snippet: string, query: string) {
  const index = snippet.toLowerCase().indexOf(query.toLowerCase());
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
  const members = useMessaging((state) => state.membersByKey);
  const selfKey = useMessaging((state) => state.selfKey);
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [searching, setSearching] = useState(false);
  const [failed, setFailed] = useState(false);
  const [results, setResults] = useState<MessageSearchResult[] | null>(null);
  const container = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const timer = useRef<number | null>(null);
  const requestID = useRef(0);
  const composing = useRef(false);

  const runSearch = useCallback(
    async (value: string) => {
      const trimmed = value.trim();
      const id = ++requestID.current;
      if (!trimmed) {
        setResults(null);
        setSearching(false);
        setFailed(false);
        return;
      }
      setSearching(true);
      setFailed(false);
      try {
        const found = await searchMessages(trimmed);
        if (requestID.current === id) setResults(found);
      } catch {
        if (requestID.current === id) {
          setResults(null);
          setFailed(true);
        }
      } finally {
        if (requestID.current === id) setSearching(false);
      }
    },
    [searchMessages],
  );

  const scheduleSearch = useCallback(
    (value: string) => {
      if (timer.current !== null) window.clearTimeout(timer.current);
      requestID.current += 1;
      setResults(null);
      setFailed(false);
      timer.current = window.setTimeout(
        () => void runSearch(value),
        SEARCH_DEBOUNCE_MS,
      );
    },
    [runSearch],
  );

  const close = useCallback(() => {
    if (timer.current !== null) window.clearTimeout(timer.current);
    requestID.current += 1;
    setOpen(false);
    setSearching(false);
  }, []);

  useEffect(
    () => () => {
      if (timer.current !== null) window.clearTimeout(timer.current);
    },
    [],
  );

  useEffect(() => {
    if (!open) return;
    const outside = (event: PointerEvent) => {
      if (
        event.target instanceof Node &&
        container.current?.contains(event.target)
      )
        return;
      close();
    };
    document.addEventListener("pointerdown", outside);
    return () => document.removeEventListener("pointerdown", outside);
  }, [close, open]);

  const trimmed = query.trim();
  return (
    <div ref={container} className="relative">
      <div className="relative">
        <Search className="-translate-y-1/2 pointer-events-none absolute top-1/2 left-2 size-3.5 text-muted-foreground" />
        <input
          ref={input}
          value={query}
          placeholder="検索"
          onChange={(event) => {
            const next = limitQuery(event.target.value);
            setQuery(next);
            setOpen(true);
            if (!composing.current) scheduleSearch(next);
          }}
          onCompositionStart={() => {
            composing.current = true;
            if (timer.current !== null) window.clearTimeout(timer.current);
          }}
          onCompositionEnd={(event) => {
            composing.current = false;
            scheduleSearch(limitQuery(event.currentTarget.value));
          }}
          onFocus={() => {
            if (query.trim()) setOpen(true);
          }}
          onKeyDown={(event) => {
            if (event.nativeEvent.isComposing) return;
            if (event.key === "Enter") {
              event.preventDefault();
              if (timer.current !== null) window.clearTimeout(timer.current);
              setOpen(true);
              void runSearch(query);
            }
            if (event.key === "Escape") {
              event.preventDefault();
              close();
              input.current?.blur();
            }
          }}
          className="h-7 w-36 rounded-md border border-border bg-background pr-2 pl-7 text-[12px] outline-none transition-[width] duration-150 focus:w-64 focus-visible:border-ring/60 sm:w-44"
        />
      </div>
      {open && trimmed ? (
        <div className="absolute top-full right-0 z-20 mt-1 max-h-[60vh] w-96 overflow-y-auto rounded-lg border border-border bg-background p-1 shadow-md">
          <p className="flex items-center gap-1.5 px-2 pt-1.5 pb-1 font-medium text-[11px] text-muted-foreground">
            「{trimmed}」の検索結果
            {searching ? <Loader2 className="size-3 animate-spin" /> : null}
          </p>
          {failed && !searching ? (
            <p className="px-2 pb-2 text-[12px] text-rose-500">
              検索に失敗しました
            </p>
          ) : null}
          {results?.length === 0 && !searching ? (
            <p className="px-2 pb-2 text-[12px] text-muted-foreground/70">
              一致するメッセージはありません
            </p>
          ) : null}
          {results?.map((result) => (
            <button
              key={result.messageId}
              type="button"
              onClick={() => {
                close();
                onJump({
                  placeKey: placeKey(result.place),
                  seq: result.seq,
                  messageId: result.messageId,
                });
              }}
              className="block w-full rounded-md px-2 py-1.5 text-left hover:bg-accent/60"
            >
              <span className="flex items-baseline gap-1.5">
                <span className="max-w-[45%] truncate font-medium text-[11px] text-muted-foreground">
                  {labelForPlace(result.place, channels, dms, members, selfKey)}
                </span>
                <span className="truncate text-[11px] text-muted-foreground/80">
                  {members[participantKey(result.author)]?.displayName ??
                    "不明"}
                </span>
                <span className="ml-auto shrink-0 text-[10px] text-muted-foreground/60">
                  {resultTime.format(result.createdAt)}
                </span>
              </span>
              <span className="mt-0.5 line-clamp-2 block text-[12.5px] text-foreground/90">
                {highlightedSnippet(result.snippet, trimmed)}
              </span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
