package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

// TerminalFailureNotice is the agentstate terminal-failure hook. When a
// directed Messaging input — a mention, DM, reply to the secretary, or its
// own reminder — can no longer produce a reply, the secretary leaves one
// durable message in the same place saying so. The requester sees the failed
// request, its cause, and what to do next inside the existing conversation;
// the notice replies to the requesting message, so the association survives
// reconnect through ordinary history.
//
// The notice appends inside the commit transaction, so it is atomic with the
// failure record, and its client nonce is derived from the input id, so an
// identical-commit replay after a lost response re-reads the existing notice
// instead of posting a second one. A notice error never vetoes the commit:
// the failure record must land even when nothing can be posted.
func (d *CoreAttentionDelivery) TerminalFailureNotice(ctx context.Context, tx pgx.Tx, f agentstate.TerminalFailure) (func(context.Context), error) {
	if d == nil || d.Messaging == nil {
		return nil, nil
	}
	var surface, attention string
	var payload []byte
	err := tx.QueryRow(ctx, `
		SELECT source_surface, attention, payload::text
		FROM core_inputs WHERE persona_id = $1 AND input_id = $2`,
		f.PersonaID, f.InputID).Scan(&surface, &attention, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Only a directed Messaging request earns a visible failure. Ambient
	// observations never asked for a reply, and inputs from other surfaces
	// have no conversation here to report into.
	if surface != "messaging" || attention != "reply" {
		return nil, nil
	}
	var provenance struct {
		Place struct {
			ID string `json:"id"`
		} `json:"place"`
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(payload, &provenance); err != nil {
		return nil, err
	}
	placeID := provenance.Place.ID
	if placeID == "" {
		return nil, nil
	}
	// Scope resolves live, inside this transaction: the place's current
	// installation and the secretary's current membership authorize the
	// notice exactly as they would an ordinary send. A place the secretary
	// can no longer reach has no permitted conversation to report into —
	// that is a valid "no notice" outcome, not a failure of the hook.
	scoped, err := d.scopeForCorePlace(ctx, tx, f.PersonaID, placeID)
	switch {
	case err == nil:
	case errors.Is(err, ErrPlaceNotFound), errors.Is(err, ErrInvalidScope),
		errors.Is(err, applicationapps.ErrInstallationNotFound),
		errors.Is(err, applicationapps.ErrAppDisabled):
		return nil, nil
	default:
		return nil, err
	}
	// The request may have failed after some of its effects committed. The
	// durable operation ledger is the record of what actually ran — it, not
	// the error text, decides whether "ask again" is honest.
	var effectsCommitted bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM core_operations
			WHERE persona_id = $1 AND turn_id = $2)`,
		f.PersonaID, f.TurnID).Scan(&effectsCommitted); err != nil {
		return nil, err
	}
	var next string
	if effectsCommitted {
		next = "Some of what it asked may already have been done — please check what changed and send me a new message about what is still missing, rather than asking me to repeat the whole request."
	} else {
		next = "Nothing from it was sent or changed — you can simply ask me again."
	}
	appendIn := AppendInput{
		PlaceID:     placeID,
		Content:     "I could not complete that request — " + failureNoticeCause(f) + " " + next,
		ReplyTo:     provenance.MessageID,
		ClientNonce: coreToolNonce("core.failure_notice", f.InputID),
	}
	if err := normalizeAppendInput(&appendIn); err != nil {
		return nil, err
	}
	message, created, err := scoped.appendScopedInTx(ctx, tx, appendIn)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotAMember), errors.Is(err, ErrPlaceNotFound),
		errors.Is(err, ErrMessageNotFound), errors.Is(err, ErrForbidden),
		errors.Is(err, ErrNotReachable):
		// Live scope/authority/visibility refused the notice — for example
		// the secretary was removed or the requesting message left its
		// visible window. Suppressing the notice is correct: posting into a
		// conversation the secretary cannot currently author would leak the
		// request's existence.
		return nil, nil
	default:
		return nil, err
	}
	if !created {
		// An identical-commit replay found the durable notice already
		// posted: no second message, no second live frame.
		return nil, nil
	}
	return func(ctx context.Context) {
		d.afterSendCommit(ctx, f.PersonaID,
			map[string]any{"place_id": placeID},
			map[string]any{"message_id": message.MessageID})
	}, nil
}

// failureNoticeCause renders the recorded reason as one short line the
// requester can act on. It never carries a stack trace, credentials, or
// unrelated context: known classifications get fixed wording, anything else
// is the first line of the recorded error, capped well under any message
// bound.
func failureNoticeCause(f agentstate.TerminalFailure) string {
	switch f.ErrorKind {
	case "no_model_connection":
		return "I had no usable model connection."
	case "oversize_plan":
		return "my answer was too large to record — it exceeded the service's per-request size limit."
	}
	cause := f.Error
	if i := strings.IndexAny(cause, "\r\n"); i >= 0 {
		cause = cause[:i]
	}
	cause = strings.Map(func(r rune) rune {
		if r < 0x20 {
			return -1
		}
		return r
	}, cause)
	if !utf8.ValidString(cause) {
		cause = strings.ToValidUTF8(cause, "")
	}
	const maxCauseRunes = 200
	if utf8.RuneCountInString(cause) > maxCauseRunes {
		runes := []rune(cause)
		cause = string(runes[:maxCauseRunes-1]) + "…"
	}
	if cause == "" {
		return "it stopped without a recorded reason."
	}
	return "it stopped with the recorded reason: " + cause + "."
}
