import type {
  Choice,
  DecisionRequest,
  DecisionResponse,
  DecisionStatus,
} from "./contracts";

export interface DecisionRow {
  id: string;
  publisher_fingerprint: string;
  idempotency_key: string;
  payload_hash: string;
  title: string;
  body: string;
  source_label: string;
  choices_json: string;
  allow_free_text: number;
  callback_url: string | null;
  correlation_id: string | null;
  status: DecisionStatus;
  expires_at: number;
  created_at: number;
  updated_at: number;
  resolved_at: number | null;
  cancelled_at: number | null;
  resolution_key: string | null;
  callback_attempted_at: number | null;
  callback_status: number | null;
  callback_delivery_id: string | null;
  callback_delivery_created_at: number | null;
  response_id: string | null;
  response_choice_id: string | null;
  response_reply: string | null;
  response_created_at: number | null;
}

export const DECISION_SELECT = `
  SELECT
    r.*,
    p.id AS response_id,
    p.choice_id AS response_choice_id,
    p.reply AS response_reply,
    p.created_at AS response_created_at
  FROM decision_requests r
  LEFT JOIN decision_responses p ON p.request_id = r.id
`;

function iso(timestamp: number): string {
  return new Date(timestamp).toISOString();
}

export function decisionFromRow(row: DecisionRow): DecisionRequest {
  const response: DecisionResponse | null = row.response_id
    ? {
        id: row.response_id,
        choiceId: row.response_choice_id,
        reply: row.response_reply,
        createdAt: iso(row.response_created_at ?? row.updated_at),
      }
    : null;

  return {
    id: row.id,
    title: row.title,
    body: row.body,
    source: row.source_label,
    choices: JSON.parse(row.choices_json) as Choice[],
    allowFreeText: row.allow_free_text === 1,
    status: row.status,
    expiresAt: iso(row.expires_at),
    createdAt: iso(row.created_at),
    updatedAt: iso(row.updated_at),
    correlationId: row.correlation_id,
    response,
  };
}

export async function expireRequests(
  db: D1Database,
  now: number,
  id?: string,
): Promise<void> {
  const query = id
    ? "UPDATE decision_requests SET status = 'expired', updated_at = ? WHERE id = ? AND status = 'pending' AND expires_at <= ?"
    : "UPDATE decision_requests SET status = 'expired', updated_at = ? WHERE status = 'pending' AND expires_at <= ?";
  const values = id ? [now, id, now] : [now, now];
  await db
    .prepare(query)
    .bind(...values)
    .run();
}

export async function getDecision(
  db: D1Database,
  id: string,
): Promise<DecisionRow | null> {
  return db
    .prepare(`${DECISION_SELECT} WHERE r.id = ?`)
    .bind(id)
    .first<DecisionRow>();
}
