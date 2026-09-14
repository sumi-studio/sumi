import type {
  AgentRun,
  ChatItem,
  ConversationEntry,
  ConversationModel,
} from "./model";
import { createConversationChanges } from "./model";
import {
  collectAgentCopyText,
  collectProjectionGlobals,
  type ProjectionGlobals,
  projectEntry,
  projectionKey,
} from "./projection";

export interface TimelineExchange {
  /** Index of the exchange's first row (the user item) in `items`. */
  startIndex: number;
  /** Index of the exchange's last row in `items`. */
  endIndex: number;
  tick: { id: string; title: string; preview?: string };
}

export interface ExchangeState extends TimelineExchange {
  /** Item whose text currently supplies the tick preview. */
  previewSourceId?: string;
}

interface EntryMeta {
  kind: ConversationEntry["kind"];
  runId: string | null;
  messageId?: string;
}

/**
 * Incremental view over a `ConversationModel` driven by the model's write
 * journal. The canonical projection rescans the whole lifetime transcript on
 * every render, so a 6,000-entry conversation paid O(history) for every
 * streamed token; this projector instead recomputes only the rows the journal
 * names while producing the same rows as `projectConversation`.
 *
 * Consumption protocol: `update` reads `model.changes`, applies it, then
 * replaces the field with a fresh journal so subsequent writes accumulate for
 * the next update. A model literal without a journal, a structural journal,
 * a removal, or a non-tail order edit falls back to a full rebuild —
 * infrequent events (history merges, replayed-content inserts, deletions)
 * keep canonical cost.
 *
 * A single projector must own each model's journal stream; two projectors
 * consuming one model would each disarm the journal the other needed.
 */
export class ConversationProjector {
  items: ChatItem[] = [];
  copyTextByRunId = new Map<string, string>();
  exchanges: TimelineExchange[] = [];
  itemIndexById = new Map<string, number>();

  private initialized = false;
  private lastConsumed: object | null = null;
  private orderIds: string[] = [];
  private runIds: string[] = [];
  private itemByEntryId = new Map<string, ChatItem>();
  private runById = new Map<string, AgentRun>();
  private entryIdsByRun = new Map<string, string[]>();
  private copyProseIdsByRun = new Map<string, string[]>();
  private entryMeta = new Map<string, EntryMeta>();
  private globals: ProjectionGlobals = {
    finalProseByRun: new Map(),
    toolEntries: new Map(),
    approvals: new Map(),
  };
  private toolSlots = new Map<string, { activity?: string; result?: string }>();
  private exchangeStates: ExchangeState[] = [];
  private exchangeIndexByItemId = new Map<string, number>();
  /**
   * Internal mutable row list. The public `items` is a fresh copy per publish
   * so React memoization sees a changed identity; `itemList` is never exposed.
   */
  private itemList: ChatItem[] = [];
  /** Runs whose row is placed at their first entry's position. */
  private insertedRuns = new Set<string>();
  /** Ids of entry-less runs whose rows sit after every entry row. */
  private trailingRuns = new Set<string>();

  update(model: ConversationModel): void {
    const journal = model.changes;
    if (
      journal &&
      !journal.structural &&
      (journal === this.lastConsumed ||
        (journal.addedEntryIds.size === 0 &&
          journal.removedEntryIds.size === 0 &&
          journal.changedEntryIds.size === 0 &&
          journal.changedRunIds.size === 0 &&
          journal.orderOps.length === 0))
    )
      return;

    if (!this.initialized || !journal || journal.structural) {
      model.changes = createConversationChanges();
      this.lastConsumed = journal ?? null;
      this.rebuild(model);
      this.initialized = true;
      return;
    }

    // The incremental path only applies tail appends and in-place writes.
    // Anything else (removals, replayed mid-order inserts) rebuilds.
    let incremental = journal.removedEntryIds.size === 0;
    const appendedEntryIds: string[] = [];
    if (incremental) {
      for (const op of journal.orderOps) {
        if (op.op !== "insert" || op.index !== this.orderIds.length) {
          incremental = false;
          break;
        }
        this.orderIds.push(op.id);
        appendedEntryIds.push(op.id);
      }
    }
    model.changes = createConversationChanges();
    this.lastConsumed = journal;
    if (!incremental) {
      this.rebuild(model);
      return;
    }
    this.apply(model, journal, appendedEntryIds);
  }

  private rebuild(model: ConversationModel): void {
    this.orderIds = [...model.entryOrder];
    this.runIds = [...model.runOrder];
    this.itemByEntryId.clear();
    this.runById.clear();
    this.entryIdsByRun.clear();
    this.copyProseIdsByRun.clear();
    this.entryMeta.clear();
    this.toolSlots.clear();
    this.globals = collectProjectionGlobals(model);
    this.copyTextByRunId = collectAgentCopyText(model);

    for (const [id, run] of Object.entries(model.runs)) {
      this.runById.set(id, run);
    }
    for (const id of model.entryOrder) {
      const entry = model.entries[id];
      if (!entry) continue;
      this.registerMeta(entry);
      if (entry.kind === "trace") {
        const trace = model.runs[entry.runId]?.trace.find(
          (trace) => trace.id === entry.traceId,
        );
        if (trace?.type === "tool") {
          const key = projectionKey([entry.runId, entry.traceId]);
          const slot = this.toolSlots.get(key) ?? {};
          slot[entry.phase] = id;
          this.toolSlots.set(key, slot);
        }
      }
      const item = projectEntry(model, entry, this.globals);
      if (item) this.itemByEntryId.set(id, item);
    }
    this.assemble(model);
  }

  private registerMeta(entry: ConversationEntry): void {
    const runId = "runId" in entry ? entry.runId : null;
    this.entryMeta.set(entry.id, {
      kind: entry.kind,
      runId,
      messageId: "messageId" in entry ? entry.messageId : undefined,
    });
    if (runId) {
      const list = this.entryIdsByRun.get(runId);
      if (list) list.push(entry.id);
      else this.entryIdsByRun.set(runId, [entry.id]);
    }
    if (entry.kind === "prose" && runId) {
      const list = this.copyProseIdsByRun.get(runId);
      if (list) list.push(entry.id);
      else this.copyProseIdsByRun.set(runId, [entry.id]);
    }
  }

  private apply(
    model: ConversationModel,
    journal: NonNullable<ConversationModel["changes"]>,
    appendedEntryIds: string[],
  ): void {
    const affected = new Set<string>();
    const copyDirtyRuns = new Set<string>();
    const changedRuns: AgentRun[] = [];

    for (const id of journal.addedEntryIds) {
      const entry = model.entries[id];
      if (!entry) continue;
      this.registerMeta(entry);
      affected.add(id);
      if (entry.kind === "prose") {
        const key = entry.runId ?? `message:${entry.messageId}`;
        const previousFinal = this.globals.finalProseByRun.get(key);
        this.globals.finalProseByRun.set(key, id);
        if (previousFinal) affected.add(previousFinal);
        if (entry.runId) copyDirtyRuns.add(entry.runId);
      } else if (entry.kind === "trace") {
        const trace = model.runs[entry.runId]?.trace.find(
          (trace) => trace.id === entry.traceId,
        );
        if (trace?.type === "tool") {
          const key = projectionKey([entry.runId, entry.traceId]);
          const slot = this.toolSlots.get(key) ?? {};
          slot[entry.phase] = id;
          this.toolSlots.set(key, slot);
          this.updateToolChoice(key, slot, affected);
        }
      } else if (entry.kind === "approval") {
        this.setApproval(entry, affected);
      }
    }

    for (const id of journal.changedEntryIds) {
      const entry = model.entries[id];
      const meta = this.entryMeta.get(id);
      if (!entry || !meta) continue;
      const runId = "runId" in entry ? entry.runId : null;
      const messageId = "messageId" in entry ? entry.messageId : undefined;
      if (
        meta.kind !== entry.kind ||
        meta.runId !== runId ||
        meta.messageId !== messageId
      ) {
        // An entry changing classification invalidates every table it sits
        // in; rebuild rather than chase each one.
        this.rebuild(model);
        return;
      }
      affected.add(id);
      if (entry.kind === "prose" && runId) copyDirtyRuns.add(runId);
      if (entry.kind === "approval") this.setApproval(entry, affected);
    }

    for (const runId of journal.changedRunIds) {
      const run = model.runs[runId];
      if (!run) continue;
      if (!this.runById.has(runId)) this.runIds.push(runId);
      this.runById.set(runId, run);
      changedRuns.push(run);
      for (const entryId of this.entryIdsByRun.get(runId) ?? []) {
        const entry = model.entries[entryId];
        if (entry?.kind !== "trace") continue;
        affected.add(entryId);
        const key = projectionKey([entry.runId, entry.traceId]);
        const trace = run.trace.find((trace) => trace.id === entry.traceId);
        const slot = this.toolSlots.get(key) ?? {};
        if (trace?.type === "tool") {
          slot[entry.phase] = entryId;
          this.toolSlots.set(key, slot);
        } else {
          if (slot.activity === entryId) delete slot.activity;
          if (slot.result === entryId) delete slot.result;
          if (!slot.activity && !slot.result) this.toolSlots.delete(key);
        }
        this.updateToolChoice(key, slot, affected);
      }
    }

    const appended = new Set(appendedEntryIds);
    const replacedItems: { previous: ChatItem; item: ChatItem }[] = [];
    const changedItems = new Map<string, ChatItem>();
    for (const id of affected) {
      const entry = model.entries[id];
      const item = entry ? projectEntry(model, entry, this.globals) : null;
      const previous = this.itemByEntryId.get(id);
      if (item) this.itemByEntryId.set(id, item);
      else this.itemByEntryId.delete(id);
      if (item) changedItems.set(id, item);
      if (appended.has(id) || previous === item) continue;
      if (previous && item) {
        replacedItems.push({ previous, item });
      } else {
        // A row appearing or vanishing mid-list shifts every later row;
        // the incremental index math is not worth it — rebuild.
        this.rebuild(model);
        return;
      }
    }

    for (const runId of copyDirtyRuns) {
      // Same falsy-accumulate semantics as collectAgentCopyText: an empty
      // first segment is swallowed rather than producing a leading "\n\n".
      let text: string | undefined;
      for (const proseId of this.copyProseIdsByRun.get(runId) ?? []) {
        const entry = model.entries[proseId];
        if (entry?.kind !== "prose") continue;
        text = text ? `${text}\n\n${entry.text}` : entry.text;
      }
      if (text === undefined) this.copyTextByRunId.delete(runId);
      else this.copyTextByRunId.set(runId, text);
    }

    this.assembleIncremental(
      model,
      appendedEntryIds,
      replacedItems,
      changedRuns,
      changedItems,
    );
  }

  /** The approval key is also the tool-call key: [runId, tool_call_id]. */
  private setApproval(
    entry: Extract<ConversationEntry, { kind: "approval" }>,
    affected: Set<string>,
  ): void {
    const key = projectionKey([entry.runId, entry.request.tool_call_id]);
    this.globals.approvals.set(key, entry);
    const slot = this.toolSlots.get(key);
    const toolEntryId = slot?.activity ?? slot?.result;
    if (toolEntryId) affected.add(toolEntryId);
  }

  private updateToolChoice(
    key: string,
    slot: { activity?: string; result?: string },
    affected: Set<string>,
  ): void {
    const chosen = slot.activity ?? slot.result;
    const previous = this.globals.toolEntries.get(key);
    if (previous === chosen) return;
    if (chosen) this.globals.toolEntries.set(key, chosen);
    else this.globals.toolEntries.delete(key);
    if (previous) affected.add(previous);
    if (chosen) affected.add(chosen);
  }

  /** Full canonical assembly; also resets the incremental bookkeeping. */
  private assemble(model: ConversationModel): void {
    const items: ChatItem[] = [];
    this.insertedRuns.clear();
    this.trailingRuns.clear();
    for (const id of this.orderIds) {
      const entry = model.entries[id];
      if (!entry) continue;
      const runId = "runId" in entry ? entry.runId : null;
      if (runId && !this.insertedRuns.has(runId)) {
        const run = this.runById.get(runId);
        if (run) {
          items.push(run);
          this.insertedRuns.add(runId);
        }
      }
      const item = this.itemByEntryId.get(id);
      if (item) items.push(item);
    }
    for (const runId of this.runIds) {
      if (this.insertedRuns.has(runId)) continue;
      const run = this.runById.get(runId);
      if (run) {
        items.push(run);
        this.insertedRuns.add(runId);
        this.trailingRuns.add(runId);
      }
    }

    this.itemIndexById.clear();
    items.forEach((item, index) => {
      this.itemIndexById.set(item.id, index);
    });
    this.itemList = items;
    this.items = items.slice();
    this.rebuildExchanges(this.items);
  }

  /**
   * Bounded assembly for journaled writes: in-place row replacement, tail
   * appends, and new entry-less run rows. Placement that would not be a tail
   * append (entry rows landing ahead of trailing run rows, rows appearing or
   * vanishing mid-list) falls back to the canonical rebuild.
   */
  private assembleIncremental(
    model: ConversationModel,
    appendedEntryIds: string[],
    replacedItems: { previous: ChatItem; item: ChatItem }[],
    changedRuns: AgentRun[],
    changedItems: Map<string, ChatItem>,
  ): void {
    // Canonical order puts every entry row ahead of entry-less run rows, so
    // an entry append while such rows trail is not a tail append.
    if (appendedEntryIds.length > 0 && this.trailingRuns.size > 0) {
      this.rebuild(model);
      return;
    }
    const items = this.itemList;
    const previousLength = items.length;

    for (const { previous, item } of replacedItems) {
      const index = this.itemIndexById.get(previous.id);
      if (index === undefined || items[index] !== previous) {
        // The row index disagrees with the journal's view — rebuild rather
        // than trust drifting bookkeeping.
        this.rebuild(model);
        return;
      }
      items[index] = item;
      if (item.id !== previous.id) {
        this.itemIndexById.delete(previous.id);
        this.itemIndexById.set(item.id, index);
      }
    }

    for (const id of appendedEntryIds) {
      const entry = model.entries[id];
      if (!entry) continue;
      const runId = "runId" in entry ? entry.runId : null;
      if (runId && !this.insertedRuns.has(runId)) {
        const run = this.runById.get(runId);
        if (run) {
          items.push(run);
          this.insertedRuns.add(runId);
          this.itemIndexById.set(run.id, items.length - 1);
        }
      }
      const item = this.itemByEntryId.get(id);
      if (item) {
        items.push(item);
        this.itemIndexById.set(item.id, items.length - 1);
      }
    }

    // Changed runs carry a new object; their row must take it. Runs seen for
    // the first time join the trailing entry-less block (entry rows above
    // would already have inserted them).
    for (const run of changedRuns) {
      const index = this.itemIndexById.get(run.id);
      if (index !== undefined) {
        items[index] = run;
        continue;
      }
      if (this.insertedRuns.has(run.id)) {
        this.rebuild(model);
        return;
      }
      items.push(run);
      this.insertedRuns.add(run.id);
      this.trailingRuns.add(run.id);
      this.itemIndexById.set(run.id, items.length - 1);
    }

    this.items = items.slice();
    this.updateExchanges(this.items, previousLength, changedItems);
  }

  private rebuildExchanges(items: ChatItem[]): void {
    const { exchanges, exchangeIndexByItemId } =
      collectTimelineExchanges(items);
    this.exchangeStates = exchanges;
    this.exchangeIndexByItemId = exchangeIndexByItemId;
    this.publishExchanges();
  }

  private publishExchanges(): void {
    // Internal bookkeeping (previewSourceId) must not leak to consumers.
    this.exchanges = this.exchangeStates.map(
      ({ previewSourceId: _source, ...exchange }) => exchange,
    );
  }

  private updateExchanges(
    items: ChatItem[],
    previousItemCount: number,
    changedItems: Map<string, ChatItem>,
  ): void {
    const exchanges = this.exchangeStates;
    let touched = false;
    // Rows appended since the last assembly extend or open exchanges.
    for (let index = previousItemCount; index < items.length; index++) {
      const item = items[index];
      if (item.kind === "user") {
        const last = exchanges.at(-1);
        if (last) last.endIndex = index - 1;
        exchanges.push({
          startIndex: index,
          endIndex: index,
          tick: { id: item.id, title: item.text },
        });
        this.exchangeIndexByItemId.set(item.id, exchanges.length - 1);
        touched = true;
        continue;
      }
      const last = exchanges.at(-1);
      if (last === undefined) continue;
      last.endIndex = index;
      this.exchangeIndexByItemId.set(item.id, exchanges.length - 1);
      if (item.kind === "prose" && !last.tick.preview) {
        last.tick = { ...last.tick, preview: toExcerpt(item.text) };
        last.previewSourceId = item.id;
        touched = true;
      }
    }
    // Content changes on existing rows: titles and preview sources only.
    for (const item of changedItems.values()) {
      const exchangeIndex = this.exchangeIndexByItemId.get(item.id);
      if (exchangeIndex === undefined) continue;
      const exchange = exchanges[exchangeIndex];
      if (item.kind === "user" && exchange.tick.title !== item.text) {
        exchange.tick = { ...exchange.tick, title: item.text };
        touched = true;
      } else if (
        item.kind === "prose" &&
        (exchange.previewSourceId === item.id || !exchange.tick.preview)
      ) {
        const preview = toExcerpt(item.text);
        if (preview !== exchange.tick.preview) {
          exchange.tick = { ...exchange.tick, preview };
          exchange.previewSourceId = item.id;
          touched = true;
        }
      }
    }
    if (touched || items.length !== previousItemCount) this.publishExchanges();
  }
}

/**
 * Canonical row list → timeline exchanges plus row indexes. The projector
 * keeps this result updated incrementally; tests and rebuilds use this scan.
 */
export function collectTimelineExchanges(items: ChatItem[]): {
  exchanges: ExchangeState[];
  itemIndexById: Map<string, number>;
  exchangeIndexByItemId: Map<string, number>;
} {
  const exchanges: ExchangeState[] = [];
  const itemIndexById = new Map<string, number>();
  const exchangeIndexByItemId = new Map<string, number>();
  items.forEach((item, index) => {
    itemIndexById.set(item.id, index);
    const current = exchanges.at(-1);
    if (item.kind === "user") {
      if (current) current.endIndex = index - 1;
      exchanges.push({
        startIndex: index,
        endIndex: items.length - 1,
        tick: { id: item.id, title: item.text },
      });
      exchangeIndexByItemId.set(item.id, exchanges.length - 1);
      return;
    }
    if (current) exchangeIndexByItemId.set(item.id, exchanges.length - 1);
    if (item.kind === "prose" && current && !current.tick.preview) {
      current.tick = { ...current.tick, preview: toExcerpt(item.text) };
      current.previewSourceId = item.id;
    }
  });
  return { exchanges, itemIndexById, exchangeIndexByItemId };
}

function toExcerpt(text: string): string {
  return text
    .replace(/```[\s\S]*?```/g, " (コード) ")
    .replace(/\$\$[\s\S]*?\$\$/g, " (数式) ")
    .replace(/[#*`>|$_-]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 140);
}
