import type { DraftAttachment } from "./draft-attachments";

export interface DraftSelection {
  start: number;
  end: number;
  direction: "forward" | "backward" | "none";
  scrollTop: number;
}

/** The reference the Human chose, including enough context to show it before history reloads. */
export interface DraftReplyTarget {
  messageId: string;
  authorLabel: string;
  preview: string;
}

export interface ComposerDraft {
  text: string;
  replyTarget: DraftReplyTarget | null;
  attachments: DraftAttachment[];
  attachmentOverflow: number;
  selection: DraftSelection | null;
}

export const EMPTY_COMPOSER_DRAFT: ComposerDraft = {
  text: "",
  replyTarget: null,
  attachments: [],
  attachmentOverflow: 0,
  selection: null,
};
