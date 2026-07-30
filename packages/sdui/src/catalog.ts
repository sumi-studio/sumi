import { z } from "zod";
import { parseSduiNode, type SduiNode } from "./schema";

/**
 * 最小カタログ (画面構成書・実装順 2): リマインダー・確認ダイアログ・リスト。
 * props はカード種別ごとに zod で検証し、不正な宣言データは
 * フォールバック表示に落とす (実行コードは決して流れない)。
 */

export const MAX_SDUI_TITLE_LENGTH = 256;
export const MAX_SDUI_BODY_LENGTH = 2_048;
export const MAX_SDUI_LABEL_LENGTH = 128;
export const MAX_SDUI_ACTION_LENGTH = 512;
export const MAX_SDUI_ACTIONS = 8;
export const MAX_SDUI_LIST_ITEMS = 100;

const labelSchema = z.string().max(MAX_SDUI_LABEL_LENGTH);
const actionSchema = z.string().max(MAX_SDUI_ACTION_LENGTH);
const cardActionSchema = z
  .object({ label: labelSchema, action: actionSchema })
  .strict();

export const reminderCardSchema = z
  .object({
    title: z.string().max(MAX_SDUI_TITLE_LENGTH),
    /** ISO 8601 */
    at: z.string().max(128),
    note: z.string().max(MAX_SDUI_BODY_LENGTH).optional(),
    /** ボタン。action はエージェントへ送り返す文字列 (意味の解釈はエージェント側) */
    actions: z.array(cardActionSchema).max(MAX_SDUI_ACTIONS).optional(),
  })
  .strict();
export type ReminderCardProps = z.infer<typeof reminderCardSchema>;

export const confirmCardSchema = z
  .object({
    title: z.string().max(MAX_SDUI_TITLE_LENGTH),
    message: z.string().max(MAX_SDUI_BODY_LENGTH).optional(),
    confirm: cardActionSchema,
    cancel: cardActionSchema,
  })
  .strict();
export type ConfirmCardProps = z.infer<typeof confirmCardSchema>;

export const listCardSchema = z
  .object({
    title: z.string().max(MAX_SDUI_TITLE_LENGTH).optional(),
    items: z
      .array(
        z
          .object({
            text: z.string().max(MAX_SDUI_BODY_LENGTH),
            done: z.boolean().optional(),
          })
          .strict(),
      )
      .max(MAX_SDUI_LIST_ITEMS),
  })
  .strict();
export type ListCardProps = z.infer<typeof listCardSchema>;

export const cardSchemas = {
  reminder: reminderCardSchema,
  confirm: confirmCardSchema,
  list: listCardSchema,
} as const;

export type CardType = keyof typeof cardSchemas;

export function parseCatalogSduiNode(value: unknown): SduiNode | null {
  const node = parseSduiNode(value);
  if (!node || !(node.type in cardSchemas)) {
    return node;
  }
  const schema = cardSchemas[node.type as CardType];
  const props = schema.safeParse(node.props);
  return props.success ? { ...node, props: props.data } : null;
}
