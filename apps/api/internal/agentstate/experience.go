package agentstate

// Experience journaling at the operation boundary.
//
// What the secretary did with a tool is part of the persona's life log the
// moment it happens, not only when the turn that did it commits: the claim
// transaction that applies an effect and records its receipt also journals
// the call and the result, so a completed effect is never known only to the
// operation ledger. A host that stops mid-turn, another input served before
// the interrupted one resumes, or a turn that ends in a terminal failure
// all leave the journal telling what actually ran. The records are the ones
// the Core itself commits (secretary.ts executeCall), keyed by
// experienceKey, so the turn's own commit — and every later attempt of the
// same input — adds each of them at most once.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// experienceKey is a journaled experience's identity within one input's
// resolution lineage (every turn serving that input): a round's deciding
// text by its round, a call and its result by the call's flat plan index —
// the same position the operation ledger keys the effect on — and an
// approval request by its approval. Records without such an identity
// (receipts, notes, failure and pause markers) have none and are never
// merged.
func experienceKey(kind string, p map[string]any) (string, bool) {
	index := func(v any) (string, bool) {
		switch n := v.(type) {
		case float64:
			if n == math.Trunc(n) && n >= 0 && n <= 1<<53 {
				return strconv.FormatInt(int64(n), 10), true
			}
		case int:
			return strconv.Itoa(n), n >= 0
		case int64:
			return strconv.FormatInt(n, 10), n >= 0
		case json.Number:
			if i, err := n.Int64(); err == nil && i >= 0 {
				return strconv.FormatInt(i, 10), true
			}
		}
		return "", false
	}
	switch kind {
	case "assistant_message":
		if k, ok := index(p["round"]); ok {
			return kind + ":" + k, true
		}
	case "tool_call", "tool_result":
		if k, ok := index(p["call_index"]); ok {
			return kind + ":" + k, true
		}
	case "approval_requested":
		if id, ok := p["approval_id"].(string); ok && id != "" {
			return kind + ":" + id, true
		}
	}
	return "", false
}

// withoutJournaledExperience drops the request's experiences (experienceKey)
// that an earlier commit of the same input's lineage already journaled,
// and a second copy inside the request itself. Experience records are only
// journaled after their input's receipt (ensureInputReceived, and every
// commit leads with the receipt), so the scan starts at the input's
// received_seq instead of walking the persona's whole journal; an input
// with no receipt yet has no journaled experience to repeat. commit_request
// keeps the request as sent, so replays still compare.
func withoutJournaledExperience(ctx context.Context, tx pgx.Tx, personaID, inputID string, events []EventInput) ([]EventInput, error) {
	keyed := false
	for _, e := range events {
		if _, ok := experienceKey(e.Kind, e.Payload); ok {
			keyed = true
			break
		}
	}
	if !keyed {
		return events, nil
	}
	journaled := map[string]bool{}
	rows, err := tx.Query(ctx, `
		SELECT e.kind, e.payload
		FROM core_inputs i
		JOIN core_events e ON e.persona_id = i.persona_id AND e.seq >= i.received_seq
		JOIN core_turns t ON t.persona_id = e.persona_id AND t.turn_id = e.turn_id
		WHERE i.persona_id = $1 AND i.input_id = $2 AND t.input_id = $2
			AND e.kind IN ('assistant_message','tool_call','tool_result','approval_requested')`,
		personaID, inputID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var kind string
		var payload map[string]any
		if err := rows.Scan(&kind, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		if k, ok := experienceKey(kind, payload); ok {
			journaled[k] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	out := make([]EventInput, 0, len(events))
	for _, e := range events {
		if k, ok := experienceKey(e.Kind, e.Payload); ok {
			if journaled[k] {
				continue
			}
			journaled[k] = true
		}
		out = append(out, e)
	}
	return out, nil
}

// callExperience is one planned call's place in its input's plan: the round
// that decided it, that round's deciding text, the call, and its flat index.
type callExperience struct {
	round int
	text  string
	call  PlanCall
	index int
}

// planPosition resolves a flat call index against the recorded plan.
func planPosition(plan *TurnPlan, callIndex int) (callExperience, bool) {
	if plan == nil || callIndex < 0 {
		return callExperience{}, false
	}
	n := callIndex
	for r, d := range plan.Plan {
		if n < len(d.Calls) {
			return callExperience{round: r, text: d.Text, call: d.Calls[n], index: callIndex}, true
		}
		n -= len(d.Calls)
	}
	return callExperience{}, false
}

// decided is the round's deciding text (when it has any) and the call as
// the model made it — journaled before the effect runs, so anything the
// effect itself records follows its cause.
func (c callExperience) decided() []EventInput {
	var out []EventInput
	if c.text != "" {
		out = append(out, EventInput{Kind: "assistant_message",
			Payload: map[string]any{"text": c.text, "round": c.round}})
	}
	return append(out, EventInput{Kind: "tool_call", Payload: map[string]any{
		"tool": c.call.Tool, "call_id": c.call.CallID, "request": c.call.Request,
		"route": c.call.Route, "round": c.round, "call_index": c.index,
	}})
}

// result is the finalized outcome exactly as the Core feeds it back to the
// model: the stored response of a done operation, or the error a failed
// one recorded (a denial marked as such). A running operation has no
// outcome yet and never gets one invented.
func (c callExperience) result(op Operation, approval *ToolApproval) (EventInput, bool) {
	p := map[string]any{"tool": c.call.Tool, "call_id": c.call.CallID, "call_index": c.index}
	switch op.Status {
	case "done":
		p["response"] = op.Response
	case "failed":
		p["error"] = receiptError(op.Response)
		if approval != nil && approval.Status == "denied" {
			p["denied"] = true
		}
	default:
		return EventInput{}, false
	}
	return EventInput{Kind: "tool_result", Payload: p}, true
}

// awaiting is a parked call: decided, planned, not run, waiting on a human.
func (c callExperience) awaiting(a *ToolApproval) []EventInput {
	var out []EventInput
	if c.text != "" {
		out = append(out, EventInput{Kind: "assistant_message",
			Payload: map[string]any{"text": c.text, "round": c.round}})
	}
	return append(out, EventInput{Kind: "approval_requested", Payload: map[string]any{
		"tool": c.call.Tool, "call_id": c.call.CallID, "route": c.call.Route,
		"round": c.round, "call_index": c.index, "approval_id": a.ApprovalID,
		"required_by": a.RequiredBy, "request": c.call.Request,
	}})
}

// receiptError is the error text a failed operation's receipt carries.
func receiptError(resp map[string]any) string {
	v, ok := resp["error"]
	if !ok {
		return "operation failed"
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// journalExperienceTx journals experiences of the turn's input inside the
// caller's transaction: the input first (once), then each record its
// lineage has not journaled yet. Rolled back with the transaction — an
// effect that did not commit leaves no record either.
func (s *Store) journalExperienceTx(ctx context.Context, tx pgx.Tx, personaID, turnID, inputID string, events ...EventInput) error {
	if err := ensureInputReceived(ctx, tx, personaID, inputID, turnID); err != nil {
		return err
	}
	events, err := withoutJournaledExperience(ctx, tx, personaID, inputID, events)
	if err != nil {
		return err
	}
	return s.appendEventsTx(ctx, tx, personaID, turnID, events)
}

// journalOutcomeTx journals a planned call together with its finalized
// outcome (decided records already journaled are kept once).
func (s *Store) journalOutcomeTx(ctx context.Context, tx pgx.Tx, personaID, turnID, inputID string, pos callExperience, op Operation, approval *ToolApproval) error {
	res, ok := pos.result(op, approval)
	if !ok {
		return nil
	}
	return s.journalExperienceTx(ctx, tx, personaID, turnID, inputID, append(pos.decided(), res)...)
}

// claimRejected reports whether a claim failed with a deterministic
// rejection the Core records as the call's result (HTTP 400).
func claimRejected(err error) bool {
	return errors.Is(err, ErrBadRequest) || errors.Is(err, ErrUnknownTool) || isDataError(err)
}

// journalRejectedClaim records a rejected claim — the call and the
// rejection the Core is told — in its own transaction, since the claim's
// transaction rolled back with nothing applied. It is fenced exactly like
// the claim: only the live turn's recorded plan position is journaled; a
// rejection that is not one (a fenced writer, an off-plan call) records
// nothing here.
func (s *Store) journalRejectedClaim(ctx context.Context, personaID, turnID string, generation int64, callIndex int, tool string, request map[string]any, cause error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := requireGeneration(ctx, tx, personaID, generation); err != nil {
		return err
	}
	var inputID, status string
	var turnGen int64
	err = tx.QueryRow(ctx,
		`SELECT input_id, generation, status FROM core_turns WHERE persona_id = $1 AND turn_id = $2`,
		personaID, turnID).Scan(&inputID, &turnGen, &status)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (turnGen != generation || status != "running")) {
		return nil
	}
	if err != nil {
		return err
	}
	plan, err := s.planForInput(ctx, tx, personaID, inputID)
	if err != nil {
		return dataErr(err)
	}
	if request == nil {
		request = map[string]any{}
	}
	pos, ok := planPosition(plan, callIndex)
	if !ok || pos.call.Tool != tool || !jsonbEqual(pos.call.Request, request) {
		return nil
	}
	rejection := EventInput{Kind: "tool_result", Payload: map[string]any{
		"tool": tool, "call_id": pos.call.CallID, "call_index": callIndex,
		"error": strings.ReplaceAll(cause.Error(), "\x00", ""),
	}}
	if err := s.journalExperienceTx(ctx, tx, personaID, turnID, inputID, append(pos.decided(), rejection)...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// operationCallIndex is the plan position an operation's server-derived
// idempotency key names (<input_id>:tool:<call_index>).
func operationCallIndex(idemKey string) (int, bool) {
	i := strings.LastIndex(idemKey, ":tool:")
	if i < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(idemKey[i+len(":tool:"):])
	return n, err == nil && n >= 0
}

// journalCompletedTx journals an operation a turn finalized after claiming
// it running (CompleteOperation): the call and the receipt it was given,
// attributed to the finalizing turn.
func (s *Store) journalCompletedTx(ctx context.Context, tx pgx.Tx, personaID string, op Operation) error {
	callIndex, ok := operationCallIndex(op.IdempotencyKey)
	if !ok {
		return nil
	}
	var inputID string
	if err := tx.QueryRow(ctx,
		`SELECT input_id FROM core_turns WHERE persona_id = $1 AND turn_id = $2`,
		personaID, op.TurnID).Scan(&inputID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	plan, err := s.planForInput(ctx, tx, personaID, inputID)
	if err != nil {
		return dataErr(err)
	}
	pos, ok := planPosition(plan, callIndex)
	if !ok || pos.call.Tool != op.Tool {
		return nil
	}
	return s.journalOutcomeTx(ctx, tx, personaID, op.TurnID, inputID, pos, op, nil)
}

// markInterruptedTx journals, for a turn Recover interrupted that had
// already journaled experience, that its request stopped there and resumes
// later — so a context rendered before it resumes does not read the
// input's recorded calls as a finished exchange.
func markInterruptedTx(ctx context.Context, tx pgx.Tx, s *Store, personaID, turnID, inputID string) error {
	var has bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM core_inputs i
			JOIN core_events e ON e.persona_id = i.persona_id AND e.seq >= i.received_seq
			WHERE i.persona_id = $1 AND i.input_id = $2 AND e.turn_id = $3)`,
		personaID, inputID, turnID).Scan(&has); err != nil {
		return err
	}
	if !has {
		return nil
	}
	return s.appendEventsTx(ctx, tx, personaID, turnID, []EventInput{
		{Kind: "turn_paused", Payload: map[string]any{"reason": "interrupted"}},
	})
}
