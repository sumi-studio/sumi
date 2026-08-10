import { z } from "zod";

export const choiceToneSchema = z.enum(["neutral", "positive", "destructive"]);

export const choiceSchema = z.object({
  id: z
    .string()
    .trim()
    .min(1)
    .max(40)
    .regex(/^[a-zA-Z0-9_-]+$/),
  label: z.string().trim().min(1).max(80),
  tone: choiceToneSchema,
});

const callbackUrlSchema = z
  .string()
  .trim()
  .max(512)
  .refine((value) => {
    try {
      const url = new URL(value);
      const host = url.hostname.toLowerCase().replace(/\.$/u, "");
      return (
        url.protocol === "https:" &&
        !url.username &&
        !url.password &&
        host !== "localhost" &&
        !host.endsWith(".local") &&
        !/^\d{1,3}(?:\.\d{1,3}){3}$/.test(host) &&
        !host.includes(":")
      );
    } catch {
      return false;
    }
  }, "Callback URL must be a public HTTPS hostname");

export const createDecisionSchema = z
  .object({
    title: z.string().trim().min(1).max(120),
    body: z.string().trim().min(1).max(2_000),
    source: z.string().trim().min(1).max(64),
    choices: z.array(choiceSchema).min(2).max(5),
    allowFreeText: z.boolean().default(false),
    expiresAt: z.string().datetime({ offset: true }),
    callback: z
      .object({
        url: callbackUrlSchema.optional(),
        correlationId: z.string().trim().min(1).max(128).optional(),
      })
      .strict()
      .optional(),
  })
  .strict()
  .superRefine((value, context) => {
    const ids = new Set(value.choices.map((choice) => choice.id));
    if (ids.size !== value.choices.length) {
      context.addIssue({
        code: "custom",
        path: ["choices"],
        message: "Choice IDs must be unique",
      });
    }
  });

export const responseSchema = z
  .object({
    choiceId: z.string().trim().min(1).max(40).optional(),
    reply: z.string().trim().min(1).max(500).optional(),
    idempotencyKey: z.string().trim().min(8).max(128),
  })
  .strict()
  .refine((value) => Boolean(value.choiceId || value.reply), {
    message: "Choose an option or write a reply",
  });

export const bootstrapSchema = z.object({
  token: z.string().min(16).max(512),
});

export const mintBootstrapSchema = z.object({
  expiresInSeconds: z.number().int().min(300).max(86_400).default(3_600),
});

export const pushSubscriptionSchema = z.object({
  endpoint: z
    .string()
    .url()
    .max(2_048)
    .refine((value) => value.startsWith("https://")),
  expirationTime: z.number().int().positive().nullable().optional(),
  keys: z.object({
    p256dh: z.string().min(16).max(256),
    auth: z.string().min(8).max(128),
  }),
});

export type Choice = z.infer<typeof choiceSchema>;
export type ChoiceTone = z.infer<typeof choiceToneSchema>;
export type CreateDecisionInput = z.infer<typeof createDecisionSchema>;
export type DecisionStatus = "pending" | "resolved" | "cancelled" | "expired";

export interface DecisionResponse {
  id: string;
  choiceId: string | null;
  reply: string | null;
  createdAt: string;
}

export interface DecisionRequest {
  id: string;
  title: string;
  body: string;
  source: string;
  choices: Choice[];
  allowFreeText: boolean;
  status: DecisionStatus;
  expiresAt: string;
  createdAt: string;
  updatedAt: string;
  correlationId: string | null;
  response: DecisionResponse | null;
}

export interface ApiError {
  error: {
    code: string;
    message: string;
    fields?: Record<string, string[]>;
  };
}
