package agentevents

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// CoreDirectChat connects the existing browser Direct Chat surface to the
// accepted TypeScript secretary core, keeping every established contract:
//
//   - POST /direct-chat/commands still lands the verified Human's command in
//     the durable per-persona command log (same idempotency, seq, receipts);
//     a user_message additionally becomes a durable core input
//     (input_id "direct-chat:<command_id>", source_surface 'direct_chat') so
//     a stopped core host finds it queued on restart. approval_decision
//     commands resolve the core's pending approval directly under the
//     deciding Human's identity.
//   - a per-persona projector translates the committed core journal into the
//     same validated durable browser events (message_end, tool_execution_*,
//     approval_*, command_disposition) appended to the existing event log —
//     history, live WebSocket replay, and reconnect cursors keep working
//     unchanged.
//   - the core host itself is woken by the ordinary runtime wake sweep; no
//     legacy runtime generation, connection lease, or Rust process is
//     involved, and the projection refuses to write while one is live.
//
// Projection is content-deduped: every generated event is a deterministic
// function of durable core state, so a restart replays the journal and
// re-emits nothing that already reached the event log.
type CoreDirectChat struct {
	Core    *agentstate.Store
	Gateway *DurableGateway
	// Pool resolves the persona's owning Human and display name from the
	// agents table, the same lookup Messaging attention delivery uses.
	Pool *pgxpool.Pool
	// PollInterval bounds how often journals and command logs are swept.
	PollInterval time.Duration

	mu       sync.Mutex
	personas map[string]*coreDirectChatPersona
	// idleReadHook is test-only synchronization invoked inside
	// closeRunIfIdle after the idleness read resolves and before the
	// conditional END append — a regression parks the sweep in exactly the
	// window a stale idle observation races, without sleeps.
	idleReadHook func()
}

type coreDirectChatPersona struct {
	loaded     bool
	seen       map[[sha256.Size]byte]struct{}
	journalSeq int64
	commandSeq uint64
	// disposedCommands holds the command_id of every committed
	// command_disposition. A command's first terminal receipt is final:
	// the reconciler must never emit a second disposition for it, even
	// after a restart or an authority change that would classify the same
	// failure differently.
	disposedCommands map[string]struct{}
	turnInput        map[string]string // turn_id -> input_id, for surface attribution
	inputKind        map[string]string // input_id -> source_surface
	// The wire's run model allows one active run per session (agent_end
	// clears the pending approval prompt), while the core may genuinely
	// interleave inputs — e.g. a second message processed while the first
	// waits on an approval. The projection therefore asks the gateway to
	// open one run for a contiguous busy period and to close it only when
	// no direct-chat input is live (queued/claimed/waiting): agent_end is
	// then always the truth "the secretary finished", never emitted
	// mid-approval where it would hide the actionable prompt. Marker
	// need/identity is decided under the event-file lock from committed
	// log state — this projector keeps no run counters of its own, so a
	// stale or overlapping projector cannot mint a marker for an index
	// that already closed or orphan content after agent_end.
	failures  int
	nextRetry time.Time
}

var errCoreDirectChatUnavailable = errors.New("core direct chat is unavailable")

// Append implements CommandAppender.
func (c *CoreDirectChat) Append(
	ctx context.Context,
	provenance DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (CommandEnvelope, error) {
	env, _, err := c.AppendWithIdempotencyStatus(ctx, provenance, idempotencyKey, command)
	return env, err
}

// AppendWithIdempotencyStatus durably admits the command into the existing
// command log, then dispatches it to the core: a user_message becomes a
// durable input (idempotent on its command id), an approval_decision applies
// the deciding Human's one-shot verdict to the pending core approval. An
// idempotent replay still re-dispatches so a crash between the command
// commit and the core write can never strand an admitted command.
func (c *CoreDirectChat) AppendWithIdempotencyStatus(
	ctx context.Context,
	provenance DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (CommandEnvelope, bool, error) {
	if c == nil || c.Core == nil || c.Gateway == nil {
		return CommandEnvelope{}, false, errCoreDirectChatUnavailable
	}
	env, existing, err := c.Gateway.commands.appendWithIdempotencyStatus(
		ctx, provenance, idempotencyKey, command)
	if err != nil {
		return CommandEnvelope{}, false, err
	}
	if err := c.dispatch(ctx, provenance, env); err != nil {
		return env, existing, err
	}
	c.notePersona(provenance.PersonalityAgentID)
	return env, existing, nil
}

func (c *CoreDirectChat) dispatch(
	ctx context.Context,
	provenance DirectChatProvenance,
	env CommandEnvelope,
) error {
	var head browserCommandHead
	if err := json.Unmarshal(env.Command, &head); err != nil {
		return err
	}
	switch head.Type {
	case "user_message":
		return c.ensureMessageInput(ctx, provenance, env)
	case "approval_decision":
		// A permanent decision failure (already resolved elsewhere, unknown
		// request) is not an admission failure: the command is durable and
		// the reconciler closes it with the matching disposition. Transient
		// store errors still propagate so the caller can retry.
		err := c.applyApprovalDecision(ctx, provenance, env)
		switch {
		case err == nil,
			errors.Is(err, agentstate.ErrApprovalNotFound),
			errors.Is(err, agentstate.ErrApprovalForbidden),
			errors.Is(err, agentstate.ErrApprovalConflict):
			return nil
		default:
			return err
		}
	default:
		// Other command shapes (e.g. abort) have no core effect today. They
		// stay durable in the command log; the reconciler terminates them
		// with a rejected disposition the browser can render.
		return nil
	}
}

// ensureMessageInput submits the durable core input for an admitted
// user_message. Submission is idempotent on input_id, so admission retries
// and reconciler passes can never queue the command twice.
func (c *CoreDirectChat) ensureMessageInput(
	ctx context.Context,
	provenance DirectChatProvenance,
	env CommandEnvelope,
) error {
	var msg userMessageWire
	if err := unmarshalStrict(env.Command, &msg); err != nil {
		return err
	}
	if _, err := c.ensurePersona(ctx, provenance.PersonalityAgentID); err != nil {
		return err
	}
	// Every field must be a deterministic function of the durable command:
	// an idempotent replay re-submits the identical input, and SubmitInput
	// refuses a replay whose fields differ from the stored row.
	_, _, err := c.Core.SubmitInput(ctx, &agentstate.Input{
		PersonaID: provenance.PersonalityAgentID,
		InputID:   "direct-chat:" + env.CommandID,
		Kind:      "message",
		Payload: map[string]any{
			"text":        stringOrEmpty(msg.Text),
			"command_id":  env.CommandID,
			"command_seq": env.Seq,
		},
		ActorKind:     "human",
		ActorID:       provenance.Actor.PrincipalID,
		SourceSurface: "direct_chat",
		Attention:     "reply",
	})
	return err
}

// applyApprovalDecision turns a browser approval_decision command into the
// core's durable one-shot verdict. decision_id is the command id, so a
// replay of the same admitted command is a no-op, never a second effect.
func (c *CoreDirectChat) applyApprovalDecision(
	ctx context.Context,
	provenance DirectChatProvenance,
	env CommandEnvelope,
) error {
	var cmd struct {
		Type      string          `json:"type"`
		RequestID string          `json:"request_id"`
		Decision  json.RawMessage `json:"decision"`
	}
	if err := unmarshalStrict(env.Command, &cmd); err != nil {
		return err
	}
	var decision struct {
		Type string `json:"type"`
	}
	if err := unmarshalStrict(cmd.Decision, &decision); err != nil {
		return err
	}
	_, err := c.Core.ResolveApproval(ctx, provenance.PersonalityAgentID, cmd.RequestID,
		agentstate.ApprovalDecision{
			Decision:      decision.Type,
			DecisionID:    env.CommandID,
			DecidedByKind: "human",
			DecidedByID:   provenance.Actor.PrincipalID,
		})
	return err
}

// ensurePersona creates the secretary's core persona on first use, bound to
// its owning Human — the same provisioning Messaging attention performs, so
// an admitted command always has a durable queue to land in.
func (c *CoreDirectChat) ensurePersona(ctx context.Context, personaID string) (agentstate.Persona, error) {
	var humanID *string
	var displayName string
	if c.Pool != nil {
		var hid string
		err := c.Pool.QueryRow(ctx,
			"SELECT human_id::text, display_name FROM agents WHERE personality_agent_id = $1",
			personaID).Scan(&hid, &displayName)
		switch {
		case err == nil:
			humanID = &hid
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return agentstate.Persona{}, fmt.Errorf("resolve agent for persona: %w", err)
		}
	}
	persona, _, err := c.Core.EnsurePersona(ctx, personaID, humanID, displayName)
	return persona, err
}

// DirectChatReadiness reports the browser status for a core-backed persona:
// ready whenever the core state service is reachable. The executor is the
// wake-swept pool, so a stopped host is queueing, not unavailability.
func (c *CoreDirectChat) DirectChatReadiness(
	ctx context.Context,
	personalityAgentID string,
) (directChatReadiness, error) {
	if c == nil || c.Core == nil {
		return directChatReadiness{reason: directChatUnavailableUnknown}, nil
	}
	_, err := c.Core.PersonaState(ctx, personalityAgentID)
	switch {
	case err == nil || errors.Is(err, agentstate.ErrPersonaNotFound):
		return directChatReadiness{ready: true}, nil
	default:
		return directChatReadiness{reason: directChatUnavailableUnknown}, nil
	}
}

func (c *CoreDirectChat) notePersona(personalityAgentID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.personas == nil {
		c.personas = make(map[string]*coreDirectChatPersona)
	}
	if _, ok := c.personas[personalityAgentID]; !ok {
		c.personas[personalityAgentID] = newCoreDirectChatPersona()
	}
}

func newCoreDirectChatPersona() *coreDirectChatPersona {
	return &coreDirectChatPersona{
		seen:             make(map[[sha256.Size]byte]struct{}),
		disposedCommands: make(map[string]struct{}),
		turnInput:        make(map[string]string),
		inputKind:        make(map[string]string),
	}
}

func (c *CoreDirectChat) personaState(personalityAgentID string) *coreDirectChatPersona {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.personas == nil {
		c.personas = make(map[string]*coreDirectChatPersona)
	}
	st, ok := c.personas[personalityAgentID]
	if !ok {
		st = newCoreDirectChatPersona()
		c.personas[personalityAgentID] = st
	}
	return st
}

func (c *CoreDirectChat) pollInterval() time.Duration {
	if c.PollInterval > 0 {
		return c.PollInterval
	}
	return 500 * time.Millisecond
}

// Run projects committed core journal events into each served persona's
// durable browser event log until ctx ends. Personas are rediscovered every
// sweep from durable state — direct-chat inputs and command logs — so an API
// restart loses no work and no persona needs a live browser to make progress.
func (c *CoreDirectChat) Run(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval())
	defer ticker.Stop()
	for {
		c.sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// maxSyncBackoff caps the per-persona retry backoff so a persistently failing
// projection still recovers promptly once the cause clears — a lost index
// write, a restarted database — rather than waiting out a long penalty.
const maxSyncBackoff = 30 * time.Second

func (c *CoreDirectChat) sweep(ctx context.Context) {
	for _, id := range c.personaIDs(ctx) {
		st := c.personaState(id)
		if time.Now().Before(st.nextRetry) {
			continue
		}
		err := c.syncPersona(ctx, id)
		switch {
		case err == nil:
			if st.failures > 0 {
				log.Printf("core direct-chat projection for %s recovered after %d consecutive failure(s)", id, st.failures)
			}
			st.failures = 0
			st.nextRetry = time.Time{}
		case ctx.Err() != nil:
			// Shutdown raced a sync: not a persona failure.
		default:
			st.failures++
			// Double the delay up to the cap. Iterating instead of shifting
			// keeps large failure counts from overflowing time.Duration
			// before the cap can apply — failures is unbounded.
			delay := c.pollInterval()
			for i := 1; i < st.failures && delay < maxSyncBackoff; i++ {
				delay *= 2
			}
			if delay > maxSyncBackoff {
				delay = maxSyncBackoff
			}
			st.nextRetry = time.Now().Add(delay)
			// Every failure is logged, but the backoff bounds the rate: a
			// stuck persona costs at most one line per delay, not one per
			// poll, and the recovery above is observable the moment it lands.
			log.Printf("core direct-chat projection for %s failed (%d consecutive; retry in %s): %v",
				id, st.failures, delay, err)
		}
	}
}

// personaIDs unions the personas this process has admitted commands for with
// every persona holding durable direct-chat evidence (inputs or commands), so
// reconciliation survives restarts without a live connection.
func (c *CoreDirectChat) personaIDs(ctx context.Context) []string {
	seen := make(map[string]bool)
	var ids []string
	c.mu.Lock()
	for id := range c.personas {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	if surfaced, err := c.Core.PersonaIDsByInputSurface(ctx, "direct_chat"); err == nil {
		for _, id := range surfaced {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	if commanded, err := c.Gateway.commands.PersonaIDs(); err == nil {
		for _, id := range commanded {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	return ids
}

// syncPersona advances one persona's projection: journal events, failed-turn
// error messages, then command reconciliation. Each pass is idempotent —
// generated events are deterministic functions of durable state and the
// append is deduped by content against what the log already holds.
func (c *CoreDirectChat) syncPersona(ctx context.Context, personaID string) error {
	st := c.personaState(personaID)
	if !st.loaded {
		if err := c.loadProjectionState(ctx, personaID, st); err != nil {
			return err
		}
	}
	if err := c.projectJournal(ctx, personaID, st); err != nil {
		return err
	}
	if err := c.projectPendingApprovals(ctx, personaID, st); err != nil {
		return err
	}
	if err := c.projectFailedTurns(ctx, personaID, st); err != nil {
		return err
	}
	if err := c.reconcileCommands(ctx, personaID, st); err != nil {
		return err
	}
	return c.closeRunIfIdle(ctx, personaID)
}

// loadProjectionState rebuilds this persona's content cursors from the
// durable event log so a restarted projector re-emits nothing already
// committed. Run state is deliberately not loaded here: marker decisions are
// made under the event-file lock against committed log state, so neither a
// restart nor an overlapping projector can act on a stale cached view.
func (c *CoreDirectChat) loadProjectionState(ctx context.Context, personaID string, st *coreDirectChatPersona) error {
	seen := make(map[[sha256.Size]byte]struct{})
	disposed := make(map[string]struct{})
	envelopes, err := c.Gateway.EventCatchUp(ctx, personaID, 0)
	if err != nil {
		return err
	}
	for _, env := range envelopes {
		seen[sha256.Sum256(env.Event)] = struct{}{}
		if eventType(env.Event) != "command_disposition" {
			continue
		}
		var disposition struct {
			CommandID string `json:"command_id"`
		}
		if err := json.Unmarshal(env.Event, &disposition); err == nil && disposition.CommandID != "" {
			disposed[disposition.CommandID] = struct{}{}
		}
	}
	st.seen = seen
	st.disposedCommands = disposed
	st.loaded = true
	return nil
}

// emit durably appends run content — messages, tool events, approvals —
// asking the gateway to open a run marker when the committed log has none.
// Dedup keys make the content idempotent and the marker is conditional on
// committed state under the event-file lock, so a replayed projection — in
// this process or an overlapping one — neither double-commits content nor
// strands it behind a stale run boundary.
func (c *CoreDirectChat) emit(ctx context.Context, personaID string, st *coreDirectChatPersona, events []json.RawMessage) error {
	if len(events) == 0 {
		return nil
	}
	out := make([]ProjectedEvent, 0, len(events)+1)
	out = append(out, ProjectedEvent{RunMarker: RunMarkerStart})
	for _, raw := range events {
		out = append(out, ProjectedEvent{Event: raw})
	}
	if err := c.Gateway.AppendProjectedEvents(ctx, personaID, out); err != nil {
		return err
	}
	for _, raw := range events {
		st.seen[sha256.Sum256(raw)] = struct{}{}
	}
	return nil
}

// closeRunIfIdle asks the gateway to end the open run when the persona has
// no live direct-chat input left. The liveness check runs after every other
// projection pass, so queued inputs reconciled this sweep keep the run open
// and a pending approval — whose input sits in 'waiting' — never sees its
// prompt closed early. Whether a run is actually open is decided from
// committed log state under the event-file lock: a stale projector that
// thinks a run is open commits nothing, and a stale projector that thinks
// none is open still closes one another writer left behind.
//
// The idleness read itself can go stale: a second projector may commit a new
// busy period between LiveDirectChatInputs and this append. The END request
// therefore carries the event tail observed BEFORE the PG read; under the
// append lock the gateway refuses it if the committed tail moved — an input
// admitted with nothing projected yet may legitimately follow this END and
// open its own run, but a committed live approval (or any other new event)
// must not lose its run. A refused END is re-evaluated on the next sweep
// with a fresh mark.
func (c *CoreDirectChat) closeRunIfIdle(ctx context.Context, personaID string) error {
	mark := c.Gateway.observedEventTail(personaID)
	live, err := c.Core.LiveDirectChatInputs(ctx, personaID)
	if c.idleReadHook != nil {
		c.idleReadHook()
	}
	if err != nil {
		return err
	}
	if live > 0 {
		return nil
	}
	return c.Gateway.AppendProjectedEvents(ctx, personaID,
		[]ProjectedEvent{{RunMarker: RunMarkerEnd, IfTail: &mark}})
}

func (c *CoreDirectChat) projectJournal(ctx context.Context, personaID string, st *coreDirectChatPersona) error {
	for {
		events, err := c.Core.Events(ctx, personaID, st.journalSeq, 256)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		var out []json.RawMessage
		for i, ev := range events {
			block, err := c.translate(ctx, personaID, ev, events[i+1:], st)
			if err != nil {
				return fmt.Errorf("translate journal event %d: %w", ev.Seq, err)
			}
			for _, raw := range block {
				if _, ok := st.seen[sha256.Sum256(raw)]; ok {
					continue
				}
				out = append(out, raw)
			}
		}
		if err := c.emit(ctx, personaID, st, out); err != nil {
			return err
		}
		st.journalSeq = events[len(events)-1].Seq
		if len(events) < 256 {
			return nil
		}
	}
}

// projectPendingApprovals projects approval_requested events from the durable
// pending approvals of this persona's direct-chat inputs. Approval requests
// live in core_tool_approvals (the core's 'approval_requested' outbox record
// is a delivery nudge, not the journal), so the projection reads the table
// directly; content hashing makes the one-time emission idempotent and the
// later approval_resolved event closes it out.
func (c *CoreDirectChat) projectPendingApprovals(ctx context.Context, personaID string, st *coreDirectChatPersona) error {
	pending, err := c.Core.ListApprovals(ctx, personaID, "pending")
	if err != nil {
		return err
	}
	var out []json.RawMessage
	for _, a := range pending {
		surface, err := c.inputSurface(ctx, personaID, a.InputID, st)
		if err != nil {
			return err
		}
		if surface != "direct_chat" {
			continue
		}
		raw, err := marshalEvent(approvalRequestedEvent(a))
		if err != nil {
			return err
		}
		if _, ok := st.seen[sha256.Sum256(raw)]; ok {
			continue
		}
		out = append(out, raw)
	}
	return c.emit(ctx, personaID, st, out)
}

// inputSurface resolves an input's immutable source surface, cached per
// persona so repeated sweeps do not re-read the same rows.
func (c *CoreDirectChat) inputSurface(
	ctx context.Context,
	personaID, inputID string,
	st *coreDirectChatPersona,
) (string, error) {
	if surface, ok := st.inputKind[inputID]; ok {
		return surface, nil
	}
	in, _, err := c.Core.GetInput(ctx, personaID, inputID)
	if err != nil {
		return "", err
	}
	st.inputKind[inputID] = in.SourceSurface
	return in.SourceSurface, nil
}

// projectFailedTurns surfaces turns that committed 'failed' as a terminal
// error message, so a visible human message never ends silently unanswered.
func (c *CoreDirectChat) projectFailedTurns(ctx context.Context, personaID string, st *coreDirectChatPersona) error {
	failed, err := c.Core.FailedDirectChatTurns(ctx, personaID)
	if err != nil {
		return err
	}
	var out []json.RawMessage
	for _, f := range failed {
		raw, err := c.errorMessageEnd(personaID, f)
		if err != nil {
			return err
		}
		if _, ok := st.seen[sha256.Sum256(raw)]; ok {
			continue
		}
		out = append(out, raw)
	}
	return c.emit(ctx, personaID, st, out)
}

// reconcileCommands walks the durable command log: user_message commands
// whose core input is missing are (re)submitted; approval_decision commands
// are applied idempotently; anything the core path cannot serve is closed
// with a rejected disposition instead of hanging forever pending.
func (c *CoreDirectChat) reconcileCommands(ctx context.Context, personaID string, st *coreDirectChatPersona) error {
	commands, err := c.Gateway.commands.CatchUp(ctx, personaID, st.commandSeq+1)
	if err != nil {
		return err
	}
	// A transferred secretary's commands are all rejected for the same
	// reason — it moved — regardless of which command shape failed. One
	// read per sweep keeps the reason honest for every command kind.
	moved := false
	if authority, aerr := c.Core.PersonaAuthority(ctx, personaID); aerr == nil {
		moved = authority == "transferred"
	}
	for _, env := range commands {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, done := st.disposedCommands[env.CommandID]; done {
			// The command already has a terminal receipt. It is never
			// re-dispositioned — a restarted projector or a changed
			// authority must not mint a second one with a different
			// reason.
			st.commandSeq = env.Seq
			continue
		}
		var head browserCommandHead
		if err := json.Unmarshal(env.Command, &head); err != nil {
			st.commandSeq = env.Seq
			continue
		}
		if head.Type == "user_message" {
			_, _, lookupErr := c.Core.GetInput(ctx, personaID, "direct-chat:"+env.CommandID)
			switch {
			case lookupErr == nil:
				// The durable core input exists — the command is admitted
				// into the core's queue, so its receipt is applied.
				if aerr := c.appendDisposition(ctx, personaID, st, env, "applied", ""); aerr != nil {
					return aerr
				}
			case errors.Is(lookupErr, agentstate.ErrInputNotFound):
				if err := c.ensureMessageInput(ctx, env.Provenance, env); err != nil {
					if isPermanentCoreSubmitError(err) {
						if aerr := c.appendDisposition(ctx, personaID, st, env, "rejected", commandRejectReason(err, moved)); aerr != nil {
							return aerr
						}
					} else {
						return err
					}
				} else {
					// The resubmitted input is now durable core work: the
					// command's receipt is applied, same as a command whose
					// input was already present. The deduped append keeps a
					// replayed sweep from minting a second receipt.
					if aerr := c.appendDisposition(ctx, personaID, st, env, "applied", ""); aerr != nil {
						return aerr
					}
				}
			default:
				return lookupErr
			}
		} else if head.Type == "approval_decision" {
			err := c.applyApprovalDecision(ctx, env.Provenance, env)
			switch {
			case err == nil:
			case errors.Is(err, agentstate.ErrApprovalConflict):
				if aerr := c.appendDisposition(ctx, personaID, st, env, "superseded", ""); aerr != nil {
					return aerr
				}
			case errors.Is(err, agentstate.ErrApprovalNotFound),
				errors.Is(err, agentstate.ErrApprovalForbidden),
				errors.Is(err, agentstate.ErrPersonaInactive):
				// An inactive persona takes no decisions; transferred is
				// terminal. Close it rejected rather than letting the
				// command wedge every sweep.
				if aerr := c.appendDisposition(ctx, personaID, st, env, "rejected", commandRejectReason(err, moved)); aerr != nil {
					return aerr
				}
			default:
				return err
			}
		} else {
			reason := string(RejectNotAllowed)
			if moved {
				reason = string(RejectSecretaryMoved)
			}
			if err := c.appendDisposition(ctx, personaID, st, env, "rejected", reason); err != nil {
				return err
			}
		}
		st.commandSeq = env.Seq
	}
	return nil
}

// commandDispositionKey is the durable dedup identity of a command's
// terminal receipt: one command_id admits exactly one command_disposition,
// whatever its status or reject_reason. The lock-level dedup in
// AppendProjectedEvents then refuses a second receipt for the same command
// even when a restarted or overlapping projector would classify the command
// differently — the first committed terminal result stays final.
func commandDispositionKey(commandID string) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte("sumi-core-direct-chat\x00command-disposition\x00"))
	h.Write([]byte(commandID))
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	return key
}

// commandDispositionDedupKey returns the durable dedup identity of a
// committed command_disposition event — the same command-scoped key
// appendDisposition writes under — or false when the event is not a
// disposition carrying a command_id. The gateway's dedup-index recovery
// uses it so a rebuilt index reproduces the real key; hashing the stored
// content can never match a key derived from the command_id, which would
// leave a stale projector free to commit a second terminal receipt.
func commandDispositionDedupKey(event json.RawMessage) ([sha256.Size]byte, bool) {
	var head struct {
		Type      string `json:"type"`
		CommandID string `json:"command_id"`
	}
	if json.Unmarshal(event, &head) != nil ||
		head.Type != "command_disposition" || head.CommandID == "" {
		return [sha256.Size]byte{}, false
	}
	return commandDispositionKey(head.CommandID), true
}

func (c *CoreDirectChat) appendDisposition(
	ctx context.Context,
	personaID string,
	st *coreDirectChatPersona,
	env CommandEnvelope,
	status, reason string,
) error {
	if _, done := st.disposedCommands[env.CommandID]; done {
		return nil
	}
	raw := dispositionEvent(env, status, reason)
	// Dispositions are receipts, not run content: they never open a run and
	// land inside whatever run happens to be open.
	if err := c.Gateway.AppendProjectedEvents(ctx, personaID,
		[]ProjectedEvent{{Event: raw, DedupKey: commandDispositionKey(env.CommandID)}}); err != nil {
		return err
	}
	st.seen[sha256.Sum256(raw)] = struct{}{}
	st.disposedCommands[env.CommandID] = struct{}{}
	return nil
}

func isPermanentCoreSubmitError(err error) bool {
	return errors.Is(err, agentstate.ErrPersonaInactive) ||
		errors.Is(err, agentstate.ErrTurnConflict) ||
		errors.Is(err, agentstate.ErrBadRequest) ||
		errors.Is(err, agentstate.ErrPersonaBound)
}

// commandRejectReason picks the reject_reason for a command's terminal
// disposition. A secretary transferred to another placement reports
// secretary_moved — durable history and reconnected browsers then tell the
// person where the conversation went — while every other permanent refusal
// stays not_allowed.
func commandRejectReason(err error, moved bool) string {
	if moved || errors.Is(err, agentstate.ErrPersonaTransferred) {
		return string(RejectSecretaryMoved)
	}
	return string(RejectNotAllowed)
}

// ---------------------------------------------------------------------------
// Journal → browser-event translation. Every event below is a deterministic
// function of durable core state (journal payload, committed turn row, usage
// facts, command log), so projection replays byte-identically after restart.
// ---------------------------------------------------------------------------

func (c *CoreDirectChat) translate(
	ctx context.Context,
	personaID string,
	ev agentstate.Event,
	following []agentstate.Event,
	st *coreDirectChatPersona,
) ([]json.RawMessage, error) {
	surface, err := c.eventSurface(ctx, personaID, ev, st)
	if err != nil {
		return nil, err
	}
	if surface != "direct_chat" {
		return nil, nil
	}
	switch ev.Kind {
	case "input_received":
		return c.translateInputReceived(personaID, ev)
	case "assistant_message":
		return c.translateAssistantMessage(ctx, personaID, ev, following)
	case "tool_call":
		return translateToolCall(ev)
	case "tool_result":
		return translateToolResult(ev)
	case "approval_decided":
		return c.translateApprovalDecided(ctx, personaID, ev)
	default:
		return nil, nil
	}
}

// eventSurface attributes a journal event to its input's source surface so a
// persona's Messaging traffic never leaks into the Direct Chat history. Input
// surfaces are immutable, so they are cached per persona.
func (c *CoreDirectChat) eventSurface(
	ctx context.Context,
	personaID string,
	ev agentstate.Event,
	st *coreDirectChatPersona,
) (string, error) {
	inputID := ""
	if ev.Kind == "input_received" {
		if s, ok := ev.Payload["source_surface"].(string); ok {
			if id, ok := ev.Payload["input_id"].(string); ok {
				st.inputKind[id] = s
			}
			return s, nil
		}
		return "", nil
	}
	if ev.Kind == "approval_decided" {
		// The decision journals onto the parked turn; its payload's input_id
		// names the input the approval belongs to. Decisions arriving over
		// other surfaces (e.g. the messaging approval inbox) must not leak
		// into this persona's direct-chat history.
		inputID, _ = ev.Payload["input_id"].(string)
	}
	if inputID == "" {
		var ok bool
		inputID, ok = st.turnInput[ev.TurnID]
		if !ok {
			turn, err := c.Core.Turn(ctx, personaID, ev.TurnID)
			if err != nil {
				return "", err
			}
			inputID = turn.InputID
			st.turnInput[ev.TurnID] = inputID
		}
	}
	surface, ok := st.inputKind[inputID]
	if ok {
		return surface, nil
	}
	in, _, err := c.Core.GetInput(ctx, personaID, inputID)
	if err != nil {
		return "", err
	}
	st.inputKind[inputID] = in.SourceSurface
	return in.SourceSurface, nil
}

func (c *CoreDirectChat) translateInputReceived(personaID string, ev agentstate.Event) ([]json.RawMessage, error) {
	var out []json.RawMessage
	text, _ := ev.Payload["text"].(string)
	inputID, _ := ev.Payload["input_id"].(string)
	// The browser reconciles its optimistic entry by the canonical message
	// id — UUIDv5 over the command's UUID bytes under the wire namespace —
	// the same id the legacy runtime minted. Anything else renders the sent
	// message twice.
	commandID := strings.TrimPrefix(inputID, "direct-chat:")
	messageID, err := userMessageIDFromCommandID(commandID)
	if err != nil {
		return nil, fmt.Errorf("input %q does not carry a command UUID: %w", inputID, err)
	}
	userMessage, err := marshalEvent(map[string]any{
		"type":       "message_end",
		"message_id": messageID,
		"message": map[string]any{
			"role":      "user",
			"content":   []any{map[string]any{"type": "text", "text": text}},
			"timestamp": eventTimestamp(ev, "occurred_at"),
		},
	})
	if err != nil {
		return nil, err
	}
	out = append(out, userMessage)
	return out, nil
}

func (c *CoreDirectChat) translateAssistantMessage(
	ctx context.Context,
	personaID string,
	ev agentstate.Event,
	following []agentstate.Event,
) ([]json.RawMessage, error) {
	text, _ := ev.Payload["text"].(string)
	// A round with trailing same-turn events decided tool calls; the final
	// round's message closes the turn.
	stopReason := "stop"
	for _, later := range following {
		if later.TurnID == ev.TurnID {
			stopReason = "tool_use"
			break
		}
	}
	if stopReason == "stop" {
		later, err := c.Core.Events(ctx, personaID, ev.Seq, 8)
		if err != nil {
			return nil, err
		}
		for _, e := range later {
			if e.TurnID == ev.TurnID {
				stopReason = "tool_use"
				break
			}
		}
	}
	turn, err := c.Core.Turn(ctx, personaID, ev.TurnID)
	if err != nil {
		return nil, err
	}
	info := c.turnModelInfo(ctx, personaID, ev.TurnID)
	messageID := projectedMessageID(personaID, "assistant", ev.TurnID, fmt.Sprint(ev.Seq))
	start, err := marshalEvent(map[string]any{
		"type":       "message_start",
		"message_id": messageID,
		"message":    assistantMessage(info, turn, nil, stopReason, ev.CreatedAt),
	})
	if err != nil {
		return nil, err
	}
	end, err := marshalEvent(map[string]any{
		"type":       "message_end",
		"message_id": messageID,
		"message": assistantMessage(info, turn,
			[]any{map[string]any{"type": "text", "text": text, "wire_item_index": 0}}, stopReason, ev.CreatedAt),
	})
	if err != nil {
		return nil, err
	}
	return []json.RawMessage{start, end}, nil
}

func translateToolCall(ev agentstate.Event) ([]json.RawMessage, error) {
	callID, _ := ev.Payload["call_id"].(string)
	if callID == "" {
		callID = fmt.Sprintf("call-%s-%d", ev.TurnID, ev.Seq)
	}
	tool, _ := ev.Payload["tool"].(string)
	request, _ := ev.Payload["request"].(map[string]any)
	if request == nil {
		request = map[string]any{}
	}
	raw, err := marshalEvent(map[string]any{
		"type":         "tool_execution_start",
		"tool_call_id": callID,
		"tool_name":    tool,
		"args":         request,
	})
	if err != nil {
		return nil, err
	}
	return []json.RawMessage{raw}, nil
}

func translateToolResult(ev agentstate.Event) ([]json.RawMessage, error) {
	callID, _ := ev.Payload["call_id"].(string)
	if callID == "" {
		callID = fmt.Sprintf("call-%s-%d", ev.TurnID, ev.Seq)
	}
	var result any = ev.Payload["response"]
	isError := false
	if msg, ok := ev.Payload["error"].(string); ok && msg != "" {
		result = map[string]any{"error": msg}
		isError = true
	}
	if denied, _ := ev.Payload["denied"].(bool); denied {
		isError = true
	}
	if result == nil {
		result = map[string]any{}
	}
	raw, err := marshalEvent(map[string]any{
		"type":         "tool_execution_end",
		"tool_call_id": callID,
		"result":       result,
		"is_error":     isError,
	})
	if err != nil {
		return nil, err
	}
	return []json.RawMessage{raw}, nil
}

// approvalRequestedEvent renders a durable pending approval as the browser's
// approval_requested event. Every field is a deterministic function of the
// approval row, so the content hash suppresses replays after restart.
func approvalRequestedEvent(a agentstate.ToolApproval) map[string]any {
	request := a.Request
	if request == nil {
		request = map[string]any{}
	}
	return map[string]any{
		"type": "approval_requested",
		"request": map[string]any{
			"id":           a.ApprovalID,
			"tool_call_id": a.OperationID,
			"tool_name":    a.Tool,
			"action":       map[string]any{"reviewable": request},
			"args_summary": request,
		},
	}
}

func (c *CoreDirectChat) translateApprovalDecided(
	ctx context.Context,
	personaID string,
	ev agentstate.Event,
) ([]json.RawMessage, error) {
	approvalID, _ := ev.Payload["approval_id"].(string)
	decision, _ := ev.Payload["decision"].(string)
	if approvalID == "" || (decision != "approve_once" && decision != "deny_once") {
		return nil, nil
	}
	resolved, err := marshalEvent(map[string]any{
		"type":       "approval_resolved",
		"request_id": approvalID,
		"resolution": map[string]any{"decision": map[string]any{"type": decision}},
	})
	if err != nil {
		return nil, err
	}
	out := []json.RawMessage{resolved}
	// If the decision arrived as a direct-chat command, close its receipt:
	// the deciding command is the one in the durable log carrying this
	// approval id.
	env, found, err := c.findApprovalDecisionCommand(ctx, personaID, approvalID)
	if err != nil {
		return nil, err
	}
	if found {
		out = append(out, dispositionEvent(env, "applied", ""))
	}
	return out, nil
}

func (c *CoreDirectChat) findApprovalDecisionCommand(
	ctx context.Context,
	personaID, approvalID string,
) (CommandEnvelope, bool, error) {
	commands, err := c.Gateway.commands.CatchUp(ctx, personaID, 1)
	if err != nil {
		return CommandEnvelope{}, false, err
	}
	for _, env := range commands {
		var cmd struct {
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
		}
		if err := json.Unmarshal(env.Command, &cmd); err != nil {
			continue
		}
		if cmd.Type == "approval_decision" && cmd.RequestID == approvalID {
			return env, true, nil
		}
	}
	return CommandEnvelope{}, false, nil
}

// errorMessageEnd builds the terminal assistant error record for a failed
// turn — deterministic per turn, so the sweep can offer it repeatedly and
// only the first lands.
func (c *CoreDirectChat) errorMessageEnd(personaID string, f agentstate.FailedDirectChatTurn) (json.RawMessage, error) {
	when := time.Now().UTC()
	if f.FinishedAt != nil {
		when = f.FinishedAt.UTC()
	}
	info := coreChatModelInfo{InstanceID: "env", Protocol: "open_ai_responses", Model: "unknown", Provider: "unknown"}
	// The host's bounded failure classification rides the existing nullable
	// provider_code field — no contract change, and the browser maps
	// "no_model_connection" to its localized guidance instead of the raw
	// provider detail.
	var providerCode any
	if f.ErrorKind != "" {
		providerCode = f.ErrorKind
	}
	return marshalEvent(map[string]any{
		"type":       "message_end",
		"message_id": projectedMessageID(personaID, "error", f.TurnID),
		"message": map[string]any{
			"role":          "assistant",
			"content":       []any{},
			"model":         info.Model,
			"provider":      info.Provider,
			"origin":        info.origin(),
			"usage":         zeroUsage(),
			"stop_reason":   "error",
			"error_message": f.Error,
			"provider_code": providerCode,
			"interrupted":   false,
			"timestamp":     when.Format(time.RFC3339),
		},
	})
}

type coreChatModelInfo struct {
	InstanceID string
	Protocol   string
	Model      string
	Provider   string
}

func (i coreChatModelInfo) origin() map[string]any {
	return map[string]any{
		"provider_instance_id": i.InstanceID,
		"protocol":             i.Protocol,
		"model":                i.Model,
	}
}

// turnModelInfo resolves the call-time model identity recorded on the turn's
// usage facts — never the selection that happens to be current at read time.
func (c *CoreDirectChat) turnModelInfo(ctx context.Context, personaID, turnID string) coreChatModelInfo {
	funding, err := c.Core.TurnFundingRef(ctx, personaID, turnID)
	if err != nil || funding == nil {
		return coreChatModelInfo{InstanceID: "env", Protocol: "open_ai_responses", Model: "env", Provider: "env"}
	}
	if funding.Kind == "connection" {
		model := funding.Model
		if model == "" {
			model = funding.Provider
		}
		return coreChatModelInfo{
			InstanceID: funding.ID,
			Protocol:   presetProtocol(funding.Provider),
			Model:      model,
			Provider:   funding.Provider,
		}
	}
	provider := funding.Provider
	if provider == "" {
		provider = funding.ID
	}
	return coreChatModelInfo{
		InstanceID: funding.ID,
		Protocol:   presetProtocol(provider),
		Model:      provider,
		Provider:   provider,
	}
}

// presetProtocol maps a model-connection preset (or environment provider
// name) to the wire's provider protocol vocabulary.
func presetProtocol(preset string) string {
	switch preset {
	case "anthropic":
		return "anthropic_messages"
	case "openai-responses":
		return "open_ai_responses"
	default:
		// openai-chat and the other chat-completions presets, plus the
		// local mock/fixture env providers.
		return "open_ai_chat_completions"
	}
}

func zeroUsage() map[string]any {
	return map[string]any{
		"input": 0, "output": 0, "cache_read": 0,
		"cache_write": 0, "reasoning": 0, "total_tokens": 0,
	}
}

// turnUsage flattens the committed turn's per-round provider usage into the
// wire's usage shape.
func turnUsage(turn *agentstate.Turn) map[string]any {
	u := zeroUsage()
	rounds, _ := turn.Usage["rounds"].([]any)
	for _, r := range rounds {
		m, _ := r.(map[string]any)
		u["input"] = u["input"].(int) + usageNumber(m, "input", "input_tokens", "prompt_tokens")
		u["output"] = u["output"].(int) + usageNumber(m, "output", "output_tokens", "completion_tokens")
		u["cache_read"] = u["cache_read"].(int) + usageNumber(m, "cache_read", "cached_tokens")
		u["cache_write"] = u["cache_write"].(int) + usageNumber(m, "cache_write", "cache_creation_input_tokens")
		u["reasoning"] = u["reasoning"].(int) + usageNumber(m, "reasoning", "reasoning_tokens")
		u["total_tokens"] = u["total_tokens"].(int) + usageNumber(m, "total_tokens", "total")
	}
	if u["total_tokens"].(int) == 0 {
		u["total_tokens"] = u["input"].(int) + u["output"].(int)
	}
	return u
}

func usageNumber(m map[string]any, keys ...string) int {
	for _, k := range keys {
		switch v := m[k].(type) {
		case float64:
			return int(v)
		case int64:
			return int(v)
		case int:
			return v
		}
	}
	return 0
}

func assistantMessage(info coreChatModelInfo, turn *agentstate.Turn, content []any, stopReason string, at time.Time) map[string]any {
	if content == nil {
		content = []any{}
	}
	var errMsg any
	if turn.Error != nil {
		errMsg = *turn.Error
	}
	return map[string]any{
		"role":          "assistant",
		"content":       content,
		"model":         info.Model,
		"provider":      info.Provider,
		"origin":        info.origin(),
		"usage":         turnUsage(turn),
		"stop_reason":   stopReason,
		"error_message": errMsg,
		"provider_code": nil,
		"interrupted":   false,
		"timestamp":     at.UTC().Format(time.RFC3339),
	}
}

func eventTimestamp(ev agentstate.Event, payloadKey string) string {
	if s, ok := ev.Payload[payloadKey].(string); ok && s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
		return s
	}
	return ev.CreatedAt.UTC().Format(time.RFC3339)
}

func dispositionEvent(env CommandEnvelope, status, reason string) json.RawMessage {
	m := map[string]any{
		"type":        "command_disposition",
		"command_id":  env.CommandID,
		"command_seq": env.Seq,
		"status":      status,
	}
	if status == "rejected" {
		m["reject_reason"] = reason
	}
	raw, _ := json.Marshal(m)
	return raw
}

func marshalEvent(m map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if err := validateEvent(raw); err != nil {
		return nil, fmt.Errorf("generated event failed wire validation: %w", err)
	}
	return raw, nil
}

// userMessageNamespace is the wire's canonical user-message namespace — the
// same constant as apps/web/src/agent/user-message-id.ts and the legacy
// Rust runtime's USER_MESSAGE_ID_NAMESPACE.
var userMessageNamespace = uuid.MustParse("78f62d15-b945-4a4f-9d84-d73c7f932b51")

// userMessageIDFromCommandID reproduces the browser's
// userMessageIdFromCommandId: UUIDv5(namespace, command UUID bytes). The
// optimistic-entry reconciler keys on this exact value.
func userMessageIDFromCommandID(commandID string) (string, error) {
	id, err := uuid.Parse(commandID)
	if err != nil {
		return "", err
	}
	return uuid.NewSHA1(userMessageNamespace, id[:]).String(), nil
}

// projectedMessageID derives a stable canonical UUID for a projected message
// so regenerated events are byte-identical after restart.
func projectedMessageID(personaID string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte("sumi-core-direct-chat\x00"))
	h.Write([]byte(personaID))
	for _, p := range parts {
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	sum := h.Sum(nil)
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
