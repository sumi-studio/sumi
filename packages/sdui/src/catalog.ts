import { z } from "zod";

/**
 * 最小カタログ (画面構成書・実装順 2): リマインダー・確認ダイアログ・リスト。
 * props はカード種別ごとに zod で検証し、不正な宣言データは
 * フォールバック表示に落とす (実行コードは決して流れない)。
 */

export const reminderCardSchema = z.object({
  title: z.string(),
  /** ISO 8601 */
  at: z.string(),
  note: z.string().optional(),
  /** ボタン。action はエージェントへ送り返す文字列 (意味の解釈はエージェント側) */
  actions: z
    .array(z.object({ label: z.string(), action: z.string() }))
    .optional(),
});
export type ReminderCardProps = z.infer<typeof reminderCardSchema>;

export const confirmCardSchema = z.object({
  title: z.string(),
  message: z.string().optional(),
  confirm: z.object({ label: z.string(), action: z.string() }),
  cancel: z.object({ label: z.string(), action: z.string() }),
});
export type ConfirmCardProps = z.infer<typeof confirmCardSchema>;

export const listCardSchema = z.object({
  title: z.string().optional(),
  items: z.array(z.object({ text: z.string(), done: z.boolean().optional() })),
});
export type ListCardProps = z.infer<typeof listCardSchema>;

export const cardSchemas = {
  reminder: reminderCardSchema,
  confirm: confirmCardSchema,
  list: listCardSchema,
} as const;

export type CardType = keyof typeof cardSchemas;
