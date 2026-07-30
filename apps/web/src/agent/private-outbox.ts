import type { RecoverableDraft } from "./model";

export const PrivateOutboxStorageKey = "sumi.direct-chat.private-outbox";
export const PrivateOutboxVersion = 1;
export const MaxPrivateOutboxEntries = 32;
export const MaxPrivateOutboxTextLength = 256 * 1024;

const MaxIdempotencyKeyLength = 1024;
const MaxRecoveryReasonLength = 128;
const UUIDPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export interface PrivateOutboxStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export type PrivateOutboxEntry =
  | {
      state: "pending";
      idempotencyKey: string;
      text: string;
    }
  | {
      state: "admitted";
      idempotencyKey: string;
      text: string;
      commandId: string;
      commandSeq: number;
    }
  | {
      state: "recoverable";
      idempotencyKey: string;
      text: string;
      reason: string;
      commandId?: string;
      commandSeq?: number;
    };

export type AdmitPrivateOutboxResult =
  | {
      kind: "admitted";
      entry: Extract<PrivateOutboxEntry, { state: "admitted" }>;
    }
  | { kind: "missing" }
  | {
      kind: "already_recoverable";
      entry: Extract<PrivateOutboxEntry, { state: "recoverable" }>;
    }
  | { kind: "conflict"; entry: PrivateOutboxEntry };

/**
 * Bounded session-private recovery state for user text. It never contains or
 * reconstructs the canonical life log; durable events own that projection.
 */
export class PrivateOutbox {
  private entriesValue: PrivateOutboxEntry[];
  private readonly storage: PrivateOutboxStorage | undefined;

  constructor(
    storage: PrivateOutboxStorage | undefined = browserSessionStorage(),
  ) {
    this.storage = storage;
    this.entriesValue = this.load();
  }

  entries(): readonly PrivateOutboxEntry[] {
    return [...this.entriesValue];
  }

  recoverableDrafts(): RecoverableDraft[] {
    return this.entriesValue.flatMap((entry) =>
      entry.state === "recoverable"
        ? [
            {
              idempotencyKey: entry.idempotencyKey,
              text: entry.text,
              reason: entry.reason,
              ...(entry.commandId ? { commandId: entry.commandId } : {}),
            },
          ]
        : [],
    );
  }

  putPending(idempotencyKey: string, text: string): boolean {
    if (!isValidKey(idempotencyKey) || !isValidText(text)) return false;
    const existing = this.findByIdempotencyKey(idempotencyKey);
    if (existing) {
      return existing.state === "pending" && existing.text === text;
    }
    if (
      this.entriesValue.length >= MaxPrivateOutboxEntries ||
      totalTextLength(this.entriesValue) + text.length >
        MaxPrivateOutboxTextLength
    ) {
      return false;
    }
    this.entriesValue = [
      ...this.entriesValue,
      { state: "pending", idempotencyKey, text },
    ];
    if (this.persist()) return true;
    // A pending command must be durable before its composer can be cleared:
    // an in-memory-only row disappears on reload and cannot safely be retried.
    this.entriesValue = this.entriesValue.filter(
      (entry) => entry.idempotencyKey !== idempotencyKey,
    );
    return false;
  }

  admit(
    idempotencyKey: string,
    commandId: string,
    commandSeq: number,
  ): AdmitPrivateOutboxResult {
    const index = this.entriesValue.findIndex(
      (entry) => entry.idempotencyKey === idempotencyKey,
    );
    if (index < 0) return { kind: "missing" };
    const existing = this.entriesValue[index];
    if (existing.state === "recoverable") {
      if (
        (existing.commandId !== undefined &&
          existing.commandId !== commandId) ||
        (existing.commandSeq !== undefined &&
          existing.commandSeq !== commandSeq)
      ) {
        return { kind: "conflict", entry: existing };
      }
      return { kind: "already_recoverable", entry: existing };
    }
    if (existing.state === "admitted") {
      return existing.commandId === commandId &&
        existing.commandSeq === commandSeq
        ? { kind: "admitted", entry: existing }
        : { kind: "conflict", entry: existing };
    }
    if (!isUUID(commandId) || !isSafeSequence(commandSeq)) {
      return { kind: "conflict", entry: existing };
    }
    const admitted: Extract<PrivateOutboxEntry, { state: "admitted" }> = {
      ...existing,
      state: "admitted",
      commandId,
      commandSeq,
    };
    this.replace(index, admitted);
    return { kind: "admitted", entry: admitted };
  }

  recoverByIdempotencyKey(
    idempotencyKey: string,
    reason: string,
  ): Extract<PrivateOutboxEntry, { state: "recoverable" }> | undefined {
    const index = this.entriesValue.findIndex(
      (entry) => entry.idempotencyKey === idempotencyKey,
    );
    return index < 0 ? undefined : this.recover(index, reason);
  }

  recoverByCommand(
    commandId: string,
    commandSeq: number,
    reason: string,
  ): Extract<PrivateOutboxEntry, { state: "recoverable" }> | undefined {
    const index = this.entriesValue.findIndex(
      (entry) =>
        entry.state === "admitted" &&
        entry.commandId === commandId &&
        entry.commandSeq === commandSeq,
    );
    return index < 0 ? undefined : this.recover(index, reason);
  }

  findByIdempotencyKey(idempotencyKey: string): PrivateOutboxEntry | undefined {
    return this.entriesValue.find(
      (entry) => entry.idempotencyKey === idempotencyKey,
    );
  }

  findByCommand(
    commandId: string,
    commandSeq: number,
  ): Extract<PrivateOutboxEntry, { state: "admitted" }> | undefined {
    const entry = this.entriesValue.find(
      (candidate) =>
        candidate.state === "admitted" &&
        candidate.commandId === commandId &&
        candidate.commandSeq === commandSeq,
    );
    return entry?.state === "admitted" ? entry : undefined;
  }

  removeByIdempotencyKey(idempotencyKey: string): boolean {
    const next = this.entriesValue.filter(
      (entry) => entry.idempotencyKey !== idempotencyKey,
    );
    if (next.length === this.entriesValue.length) return false;
    this.entriesValue = next;
    this.persist();
    return true;
  }

  consumeRecoverable(idempotencyKey: string): string | undefined {
    const entry = this.findByIdempotencyKey(idempotencyKey);
    if (entry?.state !== "recoverable") return undefined;
    this.removeByIdempotencyKey(idempotencyKey);
    return entry.text;
  }

  clear(): void {
    if (this.entriesValue.length === 0) {
      this.persist();
      return;
    }
    this.entriesValue = [];
    this.persist();
  }

  private recover(
    index: number,
    reason: string,
  ): Extract<PrivateOutboxEntry, { state: "recoverable" }> | undefined {
    if (!isValidReason(reason)) return undefined;
    const existing = this.entriesValue[index];
    if (existing.state === "recoverable") return existing;
    const recoverable: Extract<PrivateOutboxEntry, { state: "recoverable" }> = {
      state: "recoverable",
      idempotencyKey: existing.idempotencyKey,
      text: existing.text,
      reason,
      ...(existing.state === "admitted"
        ? {
            commandId: existing.commandId,
            commandSeq: existing.commandSeq,
          }
        : {}),
    };
    this.replace(index, recoverable);
    return recoverable;
  }

  private replace(index: number, entry: PrivateOutboxEntry) {
    this.entriesValue = this.entriesValue.map((candidate, candidateIndex) =>
      candidateIndex === index ? entry : candidate,
    );
    this.persist();
  }

  private load(): PrivateOutboxEntry[] {
    if (!this.storage) return [];
    let raw: string | null;
    try {
      raw = this.storage.getItem(PrivateOutboxStorageKey);
    } catch {
      return [];
    }
    if (raw === null) return [];
    try {
      const value: unknown = JSON.parse(raw);
      if (!isStoredOutbox(value)) throw new Error("invalid private outbox");
      return value.entries;
    } catch {
      try {
        this.storage.removeItem(PrivateOutboxStorageKey);
      } catch {
        // Storage can disappear between reads and cleanup.
      }
      return [];
    }
  }

  private persist(): boolean {
    // No browser storage is normal in non-browser runtimes. A configured
    // storage backend that throws is different: it means durable recovery
    // was explicitly attempted and failed.
    if (!this.storage) return true;
    try {
      if (this.entriesValue.length === 0) {
        this.storage.removeItem(PrivateOutboxStorageKey);
        return true;
      }
      this.storage.setItem(
        PrivateOutboxStorageKey,
        JSON.stringify({
          version: PrivateOutboxVersion,
          entries: this.entriesValue,
        }),
      );
      return true;
    } catch {
      return false;
    }
  }
}

function browserSessionStorage(): PrivateOutboxStorage | undefined {
  try {
    return globalThis.sessionStorage;
  } catch {
    return undefined;
  }
}

function isStoredOutbox(
  value: unknown,
): value is { version: 1; entries: PrivateOutboxEntry[] } {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ["version", "entries"]) ||
    value.version !== PrivateOutboxVersion ||
    !Array.isArray(value.entries) ||
    value.entries.length > MaxPrivateOutboxEntries
  ) {
    return false;
  }
  const keys = new Set<string>();
  let textLength = 0;
  for (const entry of value.entries) {
    if (!isStoredEntry(entry) || keys.has(entry.idempotencyKey)) return false;
    keys.add(entry.idempotencyKey);
    textLength += entry.text.length;
    if (textLength > MaxPrivateOutboxTextLength) return false;
  }
  return true;
}

function isStoredEntry(value: unknown): value is PrivateOutboxEntry {
  if (
    !isRecord(value) ||
    !isValidKey(value.idempotencyKey) ||
    !isValidText(value.text)
  ) {
    return false;
  }
  if (value.state === "pending") {
    return hasExactKeys(value, ["state", "idempotencyKey", "text"]);
  }
  if (value.state === "admitted") {
    return (
      hasExactKeys(value, [
        "state",
        "idempotencyKey",
        "text",
        "commandId",
        "commandSeq",
      ]) &&
      isUUID(value.commandId) &&
      isSafeSequence(value.commandSeq)
    );
  }
  if (value.state !== "recoverable" || !isValidReason(value.reason)) {
    return false;
  }
  const hasCommandId = "commandId" in value;
  const hasCommandSeq = "commandSeq" in value;
  return (
    hasCommandId === hasCommandSeq &&
    hasExactKeys(
      value,
      hasCommandId
        ? [
            "state",
            "idempotencyKey",
            "text",
            "reason",
            "commandId",
            "commandSeq",
          ]
        : ["state", "idempotencyKey", "text", "reason"],
    ) &&
    (!hasCommandId ||
      (isUUID(value.commandId) && isSafeSequence(value.commandSeq)))
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  const actual = Object.keys(value);
  return (
    actual.length === keys.length && actual.every((key) => keys.includes(key))
  );
}

function isValidKey(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MaxIdempotencyKeyLength
  );
}

function isValidText(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MaxPrivateOutboxTextLength
  );
}

function isValidReason(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MaxRecoveryReasonLength
  );
}

function isUUID(value: unknown): value is string {
  return (
    typeof value === "string" && value.length === 36 && UUIDPattern.test(value)
  );
}

function isSafeSequence(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function totalTextLength(entries: readonly PrivateOutboxEntry[]): number {
  return entries.reduce((total, entry) => total + entry.text.length, 0);
}
