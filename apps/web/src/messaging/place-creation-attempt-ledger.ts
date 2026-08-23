import { secureRandomUUID } from "../lib/random-uuid";

const STORAGE_PREFIX = "sumi.messaging.place-creation-attempts/v1/";

export class PlaceCreationAttemptCapacityError extends Error {
  constructor() {
    super("Too many unresolved place creation attempts");
    this.name = "PlaceCreationAttemptCapacityError";
  }
}

/** Owns unresolved logical gestures for one exact authenticated app scope. */
export class PlaceCreationAttemptLedger {
  private readonly attempts = new Map<string, string>();
  private ownerKey: string | null = null;
  private persistent = false;
  private readonly storage: Storage | null;
  private readonly capacity: number;
  private readonly nonceFactory: () => string;

  constructor(
    storage: Storage | null,
    capacity = 32,
    nonceFactory: () => string = secureRandomUUID,
  ) {
    this.storage = storage;
    this.capacity = capacity;
    this.nonceFactory = nonceFactory;
  }

  activate(ownerKey: string, persistent: boolean): void {
    if (this.ownerKey === ownerKey && this.persistent === persistent) return;
    this.attempts.clear();
    this.ownerKey = ownerKey;
    this.persistent = persistent;
    if (!persistent || !this.storage) return;

    const storageKey = this.storageKey(ownerKey);
    // sessionStorage is tab-scoped. Any other exact authority is obsolete in
    // this tab and must not retain capability-like reconciliation state.
    for (let index = this.storage.length - 1; index >= 0; index -= 1) {
      const key = this.storage.key(index);
      if (key?.startsWith(STORAGE_PREFIX) && key !== storageKey) {
        this.storage.removeItem(key);
      }
    }
    const encoded = this.storage.getItem(storageKey);
    if (encoded === null) return;
    const entries: unknown = JSON.parse(encoded);
    if (!Array.isArray(entries) || entries.length > this.capacity) {
      throw new Error("Invalid persisted place creation attempts");
    }
    for (const entry of entries) {
      if (
        !Array.isArray(entry) ||
        entry.length !== 2 ||
        typeof entry[0] !== "string" ||
        typeof entry[1] !== "string" ||
        entry[1].length === 0 ||
        entry[1].length > 128 ||
        this.attempts.has(entry[0])
      ) {
        this.attempts.clear();
        throw new Error("Invalid persisted place creation attempts");
      }
      this.attempts.set(entry[0], entry[1]);
    }
  }

  authorityReplaced(): void {
    if (this.persistent && this.ownerKey && this.storage) {
      this.storage.removeItem(this.storageKey(this.ownerKey));
    }
    this.attempts.clear();
    this.ownerKey = null;
    this.persistent = false;
  }

  nonceFor(declaration: string): string {
    const retained = this.attempts.get(declaration);
    if (retained) return retained;
    if (this.attempts.size >= this.capacity) {
      throw new PlaceCreationAttemptCapacityError();
    }
    const nonce = this.nonceFactory();
    this.attempts.set(declaration, nonce);
    this.persist();
    return nonce;
  }

  complete(declaration: string, nonce: string): void {
    if (this.attempts.get(declaration) !== nonce) return;
    if (this.persistent && this.ownerKey && this.storage) {
      const remaining = [...this.attempts].filter(
        ([candidate]) => candidate !== declaration,
      );
      const key = this.storageKey(this.ownerKey);
      if (remaining.length === 0) {
        this.storage.removeItem(key);
      } else {
        this.storage.setItem(key, JSON.stringify(remaining));
      }
    }
    this.attempts.delete(declaration);
  }

  private persist(): void {
    if (!this.persistent || !this.ownerKey || !this.storage) return;
    const key = this.storageKey(this.ownerKey);
    if (this.attempts.size === 0) {
      this.storage.removeItem(key);
      return;
    }
    this.storage.setItem(key, JSON.stringify([...this.attempts]));
  }

  private storageKey(ownerKey: string): string {
    return `${STORAGE_PREFIX}${encodeURIComponent(ownerKey)}`;
  }
}
