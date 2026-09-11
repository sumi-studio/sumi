import { secureRandomUUID } from "../lib/random-uuid";
import type { FeedbackAttachment } from "./api";
import type { FeedbackDiagnostics } from "./diagnostics";
export interface Draft {
  title: string;
  body: string;
  requestId: string;
  diagnostics?: FeedbackDiagnostics;
  submitted?: boolean;
  attachments?: FeedbackAttachment[];
}
const memory = new Map<string, Draft>();
export function draftKey(actor: string, thread: string): string {
  return `sumi:feedback:draft:${encodeURIComponent(actor)}:${encodeURIComponent(thread)}`;
}
export function loadDraft(key: string): Draft {
  const unsaved = memory.get(key);
  if (unsaved) return unsaved;
  try {
    const raw = JSON.parse(localStorage.getItem(key) || "null");
    if (
      raw &&
      typeof raw.title === "string" &&
      typeof raw.body === "string" &&
      typeof raw.requestId === "string"
    )
      return raw;
  } catch {
    const cached = memory.get(key);
    if (cached) return cached;
    /* Storage may be unavailable. Editing still works. */
  }
  return { title: "", body: "", requestId: secureRandomUUID() };
}
export function saveDraft(key: string, draft: Draft) {
  try {
    localStorage.setItem(key, JSON.stringify(draft));
    memory.delete(key);
  } catch {
    memory.set(key, draft);
    /* Keep the in-memory draft. */
  }
}
export function clearDraft(key: string, requestId: string) {
  if (loadDraft(key).requestId !== requestId) return;
  memory.delete(key);
  try {
    localStorage.removeItem(key);
  } catch {
    memory.set(key, { title: "", body: "", requestId: secureRandomUUID() });
    /* Successful send is authoritative even if an old stored value cannot be removed. */
  }
}
