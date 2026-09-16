package messaging

import (
	"context"
	"encoding/json"
	"errors"

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
		// The request may have failed after some of its effects committed.
		// Repeating the whole request can double-apply that work, so the
		// notice points at what remains instead of inviting a blind retry.
		next = "一部の処理はすでに実行された可能性があります。状態を確認のうえ、まだ必要なことを新しいメッセージでお知らせください。"
	} else {
		next = "このリクエストによる送信や変更は行われていません。" + failureNoticeNext(f)
	}
	appendIn := AppendInput{
		PlaceID:     placeID,
		Content:     "このリクエストは完了できませんでした。" + failureNoticeCause(f) + next,
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

// failureNoticeCause renders the public cause of a terminal failure in the
// product's own Japanese register. Only the bounded error_kind
// classification is safe to show: the recorded error is private diagnostic
// text — provider, tool, and SQL detail — and stays in the turn record and
// service logs. An unclassified failure gets truthful generic wording, never
// a copy of the raw reason.
func failureNoticeCause(f agentstate.TerminalFailure) string {
	switch f.ErrorKind {
	case "no_model_connection":
		return "モデル接続が選択されていないため、応答できませんでした。"
	case "oversize_plan":
		return "回答が大きすぎて記録できませんでした（1リクエストのサイズ上限を超えました）。"
	}
	return "予期しない問題が発生しました。"
}

// failureNoticeNext renders what the requester can do next when the
// operation ledger shows the turn committed nothing — a plain retry is
// honest there. Where the classification names a concrete fix, the notice
// says so instead of a bare "try again".
func failureNoticeNext(f agentstate.TerminalFailure) string {
	switch f.ErrorKind {
	case "no_model_connection":
		return "「AIの接続」で使う接続を選んでから、もう一度お尋ねください。"
	case "oversize_plan":
		return "内容を分けて、もう一度お尋ねください。"
	}
	return "もう一度お尋ねください。"
}
