import type { ComponentProps } from "react";
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
  useMessageScroller,
  useMessageScrollerScrollable,
  useMessageScrollerVisibility,
} from "../components/message-scroller";

/**
 * AI Elements Conversation APIを、現行shadcnのMessageScrollerへ接続する。
 * 自動追従と可視メッセージ管理は新しいprimitiveを使いながら、利用側は
 * Conversationというチャット領域の語彙だけを参照する。
 */
export function ConversationProvider(
  props: ComponentProps<typeof MessageScrollerProvider>,
) {
  return <MessageScrollerProvider {...props} />;
}

export function Conversation(props: ComponentProps<typeof MessageScroller>) {
  return <MessageScroller {...props} />;
}

export function ConversationViewport(
  props: ComponentProps<typeof MessageScrollerViewport>,
) {
  return <MessageScrollerViewport {...props} />;
}

export function ConversationContent(
  props: ComponentProps<typeof MessageScrollerContent>,
) {
  return <MessageScrollerContent {...props} />;
}

export function ConversationItem(
  props: ComponentProps<typeof MessageScrollerItem>,
) {
  return <MessageScrollerItem {...props} />;
}

export function ConversationScrollButton(
  props: ComponentProps<typeof MessageScrollerButton>,
) {
  return <MessageScrollerButton {...props} />;
}

export {
  useMessageScroller as useConversationScroll,
  useMessageScrollerScrollable as useConversationScrollable,
  useMessageScrollerVisibility as useConversationVisibility,
};
