// Human-facing approval reads: the authenticated owner's inbox view over
// core_tool_approvals. These projections join the parked input's provenance
// (who/where/when the secretary was working) so the decision surface can
// explain what will run without exposing internal turn/operation plumbing.
package agentstate

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// HumanApprovalResolvedLimit bounds how many decided approvals the inbox
// returns alongside the pending queue — enough for "what just resolved"
// without shipping the persona's whole history.
const HumanApprovalResolvedLimit = 25

// approvalInputTextRunes bounds the originating-input excerpt carried to the
// browser: context for the decision, never the full conversation record.
const approvalInputTextRunes = 400

// HumanApproval is one durable approval projected for the bound human's
// decision surface: the grant itself plus the secretary's name and where
// the suspended work came from.
type HumanApproval struct {
	ToolApproval
	SecretaryName string                `json:"secretary_name"`
	Input         *ApprovalInputContext `json:"input,omitempty"`
}

// ApprovalInputContext describes the input the parked call belongs to —
// the conversation and actor the secretary was answering — from the
// durable input row's own provenance fields.
type ApprovalInputContext struct {
	InputID     string                `json:"input_id"`
	Kind        string                `json:"kind"`
	Surface     string                `json:"surface"`
	ThreadID    string                `json:"thread_id,omitempty"`
	WorkspaceID string                `json:"workspace_id,omitempty"`
	ActorKind   string                `json:"actor_kind,omitempty"`
	ActorID     string                `json:"actor_id,omitempty"`
	ActorName   string                `json:"actor_name,omitempty"`
	Place       *ApprovalPlaceContext `json:"place,omitempty"`
	Text        string                `json:"text,omitempty"`
	OccurredAt  *time.Time            `json:"occurred_at,omitempty"`
}

// ApprovalPlaceContext is the conversation the suspended work belongs to,
// when the input carries one (Messaging attention inputs freeze
// place/workspace provenance into the input payload).
type ApprovalPlaceContext struct {
	ID   string `json:"id"`
	Kind string `json:"kind,omitempty"`
	Name string `json:"name,omitempty"`
}

func inputContext(in *Input) *ApprovalInputContext {
	if in == nil {
		return nil
	}
	ctx := &ApprovalInputContext{
		InputID:    in.InputID,
		Kind:       in.Kind,
		Surface:    in.SourceSurface,
		ThreadID:   in.ThreadID,
		ActorKind:  in.ActorKind,
		ActorID:    in.ActorID,
		OccurredAt: in.OccurredAt,
	}
	if workspaceID, _ := in.Payload["workspace_id"].(string); workspaceID != "" {
		ctx.WorkspaceID = workspaceID
	}
	if actor, ok := in.Payload["actor"].(map[string]any); ok {
		ctx.ActorName, _ = actor["display_name"].(string)
	}
	if place, ok := in.Payload["place"].(map[string]any); ok {
		ctx.Place = &ApprovalPlaceContext{}
		ctx.Place.ID, _ = place["id"].(string)
		ctx.Place.Kind, _ = place["kind"].(string)
		ctx.Place.Name, _ = place["name"].(string)
		if ctx.Place.ID == "" {
			ctx.Place = nil
		}
	}
	if text, _ := in.Payload["text"].(string); text != "" {
		runes := []rune(text)
		if len(runes) > approvalInputTextRunes {
			text = string(runes[:approvalInputTextRunes]) + "…"
		}
		if utf8.ValidString(text) {
			ctx.Text = text
		}
	}
	return ctx
}

type humanApprovalRow struct {
	approval      ToolApproval
	secretaryName string
	input         *Input
}

// approvalColsA is approvalCols qualified for the human-inbox join, where
// persona_id/input_id also exist on the joined tables.
const approvalColsA = `a.approval_id, a.persona_id, a.input_id, a.call_index, a.operation_id, a.turn_id,
	a.tool, a.route, a.required_by, a.request, a.action_digest, a.status, a.decision, a.decision_id,
	a.decided_by_kind, a.decided_by_id, a.provenance, a.decided_at, a.consumed_at,
	a.prior_decision, a.prior_decided_by_kind, a.prior_decided_by_id, a.prior_decided_at, a.created_at`

func (s *Store) humanApprovals(ctx context.Context, humanID string, resolved bool, limit int) ([]humanApprovalRow, error) {
	q := `SELECT ` + approvalColsA + `,
			p.display_name,
			i.kind, i.payload, i.actor_kind, i.actor_id, i.source_surface,
			i.thread_id, i.occurred_at
		FROM core_tool_approvals a
		JOIN core_personas p ON p.persona_id = a.persona_id
		LEFT JOIN core_inputs i ON i.persona_id = a.persona_id AND i.input_id = a.input_id
		WHERE p.human_id = $1 AND `
	args := []any{humanID}
	if resolved {
		q += `a.status <> 'pending' ORDER BY a.decided_at DESC NULLS LAST, a.approval_id`
		if limit > 0 {
			q += fmt.Sprintf(" LIMIT %d", limit)
		}
	} else {
		q += `a.status = 'pending' ORDER BY a.created_at, a.approval_id`
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []humanApprovalRow{}
	for rows.Next() {
		var row humanApprovalRow
		var kind, actorKind, actorID, surface, threadID *string
		var payload map[string]any
		var occurredAt *time.Time
		err := rows.Scan(
			&row.approval.ApprovalID, &row.approval.PersonaID, &row.approval.InputID,
			&row.approval.CallIndex, &row.approval.OperationID, &row.approval.TurnID,
			&row.approval.Tool, &row.approval.Route, &row.approval.RequiredBy,
			&row.approval.Request, &row.approval.ActionDigest, &row.approval.Status,
			&row.approval.Decision, &row.approval.DecisionID,
			&row.approval.DecidedByKind, &row.approval.DecidedByID,
			&row.approval.Provenance, &row.approval.DecidedAt, &row.approval.ConsumedAt,
			&row.approval.PriorDecision, &row.approval.PriorDecidedByKind,
			&row.approval.PriorDecidedByID, &row.approval.PriorDecidedAt,
			&row.approval.CreatedAt,
			&row.secretaryName,
			&kind, &payload, &actorKind, &actorID, &surface, &threadID, &occurredAt,
		)
		if err != nil {
			return nil, err
		}
		if kind != nil {
			in := &Input{
				InputID:       row.approval.InputID,
				Kind:          *kind,
				Payload:       payload,
				ActorKind:     strv(actorKind),
				ActorID:       strv(actorID),
				SourceSurface: strv(surface),
				ThreadID:      strv(threadID),
				OccurredAt:    occurredAt,
			}
			row.input = in
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func strv(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ListHumanApprovals returns the pending queue plus the most recent decided
// approvals for every persona bound to humanID — the human's own inbox over
// their secretaries, with the originating input's provenance attached.
func (s *Store) ListHumanApprovals(ctx context.Context, humanID string) ([]HumanApproval, error) {
	if humanID == "" {
		return nil, fmt.Errorf("%w: human id required", ErrBadRequest)
	}
	pending, err := s.humanApprovals(ctx, humanID, false, 0)
	if err != nil {
		return nil, err
	}
	resolved, err := s.humanApprovals(ctx, humanID, true, HumanApprovalResolvedLimit)
	if err != nil {
		return nil, err
	}
	out := make([]HumanApproval, 0, len(pending)+len(resolved))
	for _, row := range append(pending, resolved...) {
		out = append(out, HumanApproval{
			ToolApproval:  row.approval,
			SecretaryName: row.secretaryName,
			Input:         inputContext(row.input),
		})
	}
	return out, nil
}

// ApprovalByID returns one approval record addressed by its globally unique
// approval id alone (the id hashes the persona into it). The caller still
// goes through ResolveApproval for the decision — this lookup only finds
// which persona owns the record.
func (s *Store) ApprovalByID(ctx context.Context, approvalID string) (*ToolApproval, error) {
	a, err := scanApproval(s.pool.QueryRow(ctx,
		`SELECT `+approvalCols+` FROM core_tool_approvals WHERE approval_id = $1
		 ORDER BY persona_id LIMIT 1`, approvalID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// PersonaHumanID returns the persona's bound human, or "" when the persona
// is unbound or absent. The notification path treats both the same way:
// with no bound human there is no one to tell.
func (s *Store) PersonaHumanID(ctx context.Context, personaID string) (string, error) {
	var humanID *string
	err := s.pool.QueryRow(ctx,
		`SELECT human_id::text FROM core_personas WHERE persona_id = $1`,
		personaID).Scan(&humanID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if humanID == nil {
		return "", nil
	}
	return *humanID, nil
}
