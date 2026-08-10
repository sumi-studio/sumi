import {
  CompactMessageResponse,
  type CompactMessageTrailer,
} from "@sumi/ui/ai-elements/compact-message-response";
import { memo, useMemo } from "react";
import { displayMentionPattern } from "../mention";
import type { MemberProfile, ParticipantKey } from "../model";
import { participantKey } from "../model";

/**
 * メッセージ本文のMarkdown描画。
 *
 * 契約上contentはmarkdown（docs/messaging-contracts-draft.md）。開発チームの
 * 会話はコード片だらけなので、コードブロック・インラインコード・リスト・
 * 引用・強調をDiscord相当の控えめな組版で描く。
 *
 * parser・標準Markdown plugin・安全性ポリシーは@sumi/uiの共有rendererが
 * 所有する。本文由来の生HTMLは描画せず、リンクはopenerを渡さず、
 * ![alt](url)は<img>にせず明示的に開くリンクとして描く。
 *
 * mention装飾はremarkプラグインとしてAST上のtextノードを分割する。
 * code / inlineCode は値がtextノードにならないため、コードの内側は
 * Discordと同様に装飾されない。link内も装飾しない（URLを壊さない）。
 */

const MENTION_SELF_CLASS =
  "rounded bg-amber-500/15 px-0.5 font-medium text-amber-700 dark:text-amber-400";
const MENTION_OTHER_CLASS =
  "rounded bg-primary/10 px-0.5 font-medium text-primary";

/** remark/mdastノードの最小型。@types/mdastへ依存せず必要な形だけを持つ。 */
interface MdNode {
  type: string;
  value?: string;
  children?: MdNode[];
  data?: {
    hName?: string;
    hProperties?: Record<string, unknown>;
  };
}

interface MentionTarget {
  displayName: string;
  isSelf: boolean;
}

/** この中ではmention装飾をしない（コードは値がtextにならないため対象外）。 */
const MENTION_SKIP_PARENTS = new Set(["link", "linkReference"]);

function mentionNode(label: string, isSelf: boolean): MdNode {
  return {
    type: "strong",
    data: {
      hName: "span",
      hProperties: {
        className: isSelf ? MENTION_SELF_CLASS : MENTION_OTHER_CLASS,
        "data-mention": isSelf ? "self" : "other",
      },
    },
    children: [{ type: "text", value: label }],
  };
}

function splitMentions(
  text: string,
  targets: MentionTarget[],
): MdNode[] | null {
  const pattern = displayMentionPattern(targets.map((t) => t.displayName));
  const parts: MdNode[] = [];
  let cursor = 0;
  for (const match of text.matchAll(pattern)) {
    const start = match.index ?? 0;
    if (start > cursor) {
      parts.push({ type: "text", value: text.slice(cursor, start) });
    }
    const target = targets.find((t) => t.displayName === match[1]);
    parts.push(mentionNode(match[0], target?.isSelf ?? false));
    cursor = start + match[0].length;
  }
  if (parts.length === 0) return null;
  if (cursor < text.length) {
    parts.push({ type: "text", value: text.slice(cursor) });
  }
  return parts;
}

/** mdastを歩き、textノード中の @表示名 をハイライト用spanノードへ置き換える。 */
function remarkMentions(targets: MentionTarget[]) {
  return () => (tree: MdNode) => {
    if (targets.length === 0) return;
    const walk = (node: MdNode) => {
      if (!node.children) return;
      if (MENTION_SKIP_PARENTS.has(node.type)) return;
      const next: MdNode[] = [];
      let changed = false;
      for (const child of node.children) {
        if (child.type === "text" && typeof child.value === "string") {
          const replaced = splitMentions(child.value, targets);
          if (replaced) {
            next.push(...replaced);
            changed = true;
            continue;
          }
        }
        walk(child);
        next.push(child);
      }
      if (changed) node.children = next;
    };
    walk(tree);
  };
}

export interface MessageContentProps {
  content: string;
  members: Record<ParticipantKey, MemberProfile>;
  selfKey: ParticipantKey;
  /** 本文末尾へインラインで続ける後置ラベル（例:「(編集済み)」）。 */
  trailer?: CompactMessageTrailer;
}

export const MessageContent = memo(function MessageContent({
  content,
  members,
  selfKey,
  trailer,
}: MessageContentProps) {
  const remarkPlugins = useMemo(() => {
    const targets: MentionTarget[] = Object.values(members).map((member) => ({
      displayName: member.displayName,
      isSelf: participantKey(member.participant) === selfKey,
    }));
    return [remarkMentions(targets)];
  }, [members, selfKey]);

  return (
    <CompactMessageResponse
      extraRemarkPlugins={remarkPlugins}
      trailer={trailer}
    >
      {content}
    </CompactMessageResponse>
  );
});
