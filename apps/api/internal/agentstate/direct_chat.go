package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// PersonaIDsByInputSurface lists personas that hold at least one durable
// input admitted from the named source surface. The direct-chat projection
// uses it to rediscover the personas it serves after an API restart — inputs
// are the durable record, so a restart can never lose track of a persona
// that has accepted direct-chat work.
func (s *Store) PersonaIDsByInputSurface(ctx context.Context, surface string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT persona_id::text FROM core_inputs
		WHERE source_surface = $1 ORDER BY persona_id`, surface)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Turn returns the durable turn row for personaID/turnID — the projection
// reads it for committed usage and terminal status when translating journal
// events into browser-visible direct-chat messages.
func (s *Store) Turn(ctx context.Context, personaID, turnID string) (*Turn, error) {
	return s.turn(ctx, s.pool, personaID, turnID)
}

// FailedDirectChatTurn is one committed-as-failed turn that served a
// direct-chat input. The projection turns each into a terminal error
// message exactly once.
type FailedDirectChatTurn struct {
	TurnID     string
	InputID    string
	Error      string
	FinishedAt *time.Time
}

// TurnFundingRef returns the funding identity recorded on the turn's most
// recent model-call usage fact — the connection snapshot (or environment
// default) that actually produced the turn's text. The direct-chat
// projection uses it to label assistant messages with the model that ran,
// not the selection that happens to be current at read time.
func (s *Store) TurnFundingRef(ctx context.Context, personaID, turnID string) (*FundingRef, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT funding FROM usage_facts
		WHERE persona_id = $1 AND turn_id = $2 AND phase = 'turn'
		ORDER BY recorded_at DESC, fact_id DESC LIMIT 1`,
		personaID, turnID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var f FundingRef
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// FailedDirectChatTurns lists turns in terminal 'failed' status that served
// inputs admitted from the direct-chat surface, so the projection can
// surface the failure in the conversation instead of leaving the human's
// visible message unanswered forever.
func (s *Store) FailedDirectChatTurns(ctx context.Context, personaID string) ([]FailedDirectChatTurn, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.turn_id, t.input_id, COALESCE(t.error, ''), t.finished_at
		FROM core_turns t
		JOIN core_inputs i ON i.persona_id = t.persona_id AND i.turn_id = t.turn_id
		WHERE t.persona_id = $1 AND t.status = 'failed' AND i.source_surface = 'direct_chat'
		ORDER BY t.finished_at`, personaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FailedDirectChatTurn
	for rows.Next() {
		var f FailedDirectChatTurn
		if err := rows.Scan(&f.TurnID, &f.InputID, &f.Error, &f.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
