package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Trigger reasons, matching the frozen TriggerReason enum in
// contracts/agent-events.yaml.
const (
	TriggerMention = "mention"
	TriggerDM      = "dm"
)

// AttentionCandidate is one row of the per-agent inbox (凍結契約 v1 §2). It is
// a pointer to a delivered message plus the authenticated trigger snapshot —
// never the message body, and never an injection into a provider turn. What
// the recipient does with it (interrupt / inject / defer / observe) is theirs.
type AttentionCandidate struct {
	CandidateID        string
	CandidateSeq       int64
	PersonalityAgentID string
	Place              Place
	MessageID          string
	MessageSeq         int64
	Author             ParticipantRef
	Addressees         []ParticipantRef
	TriggerReason      string
	Urgency            string
	UnreadFrom         int64
	UnreadTo           int64
	ArrivalTime        time.Time
}

// issueCandidates runs inside the message-commit transaction: the inbox is its
// own transactional outbox, so a committed message and its candidates are
// inseparable (Codex合意 5).
//
// v0 eligibility is the sensible default until notification settings land
// (凍結契約 v1 未確定 (b)): mention と DM は起こす、それ以外は溜める。The
// evaluation reads the owner's standing instruction once the settings store
// exists — this boundary executes instructions, it does not judge attention.
// Rate limiting and block lists join here for the same reason (権限と安全).
func (s *Store) issueCandidates(ctx context.Context, tx pgx.Tx, place Place, msg Message) error {
	triggers := map[string]string{}
	for _, m := range msg.Mentions {
		if m.Kind == KindPersonalityAgent && m != msg.Author {
			triggers[m.ID] = TriggerMention
		}
	}
	if place.Kind != PlaceChannel {
		members, err := s.activeMembers(ctx, tx, place)
		if err != nil {
			return err
		}
		for _, member := range members {
			p := member.Participant
			if p.Kind != KindPersonalityAgent || p == msg.Author {
				continue
			}
			if _, mentioned := triggers[p.ID]; mentioned {
				continue // mention is the more specific trigger
			}
			triggers[p.ID] = TriggerDM
		}
	}
	if len(triggers) == 0 {
		return nil
	}
	// Deterministic issue order so candidate_seq allocation is stable.
	agentIDs := make([]string, 0, len(triggers))
	for id := range triggers {
		agentIDs = append(agentIDs, id)
	}
	sort.Strings(agentIDs)

	for _, agentID := range agentIDs {
		var lastRead int64
		err := tx.QueryRow(ctx,
			`SELECT last_read_seq FROM read_markers
			 WHERE place_id = $1 AND member_kind = 'personality_agent' AND member_id = $2`,
			place.PlaceID, agentID).Scan(&lastRead)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read marker for candidate: %w", err)
		}
		var seq int64
		if err := tx.QueryRow(ctx,
			`INSERT INTO attention_cursors (personality_agent_id, issued_seq)
			 VALUES ($1, 1)
			 ON CONFLICT (personality_agent_id)
			 DO UPDATE SET issued_seq = attention_cursors.issued_seq + 1, updated_at = now()
			 RETURNING issued_seq`, agentID).Scan(&seq); err != nil {
			return fmt.Errorf("allocate candidate seq: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO attention_inbox
			   (candidate_id, personality_agent_id, candidate_seq, place_id, message_id,
			    message_seq, trigger_reason, urgency, unread_from, unread_to)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			newUUIDv7(), agentID, seq, place.PlaceID, msg.MessageID,
			msg.Seq, triggers[agentID], msg.Urgency, lastRead+1, msg.Seq); err != nil {
			return fmt.Errorf("insert attention candidate: %w", err)
		}
	}
	return nil
}

// PendingCandidates returns unresolved candidates with candidate_seq strictly
// after afterSeq, ascending — the at-least-once redelivery read. The consumer
// deduplicates by candidate_id and advances its cursor with AckCandidates.
func (s *Store) PendingCandidates(ctx context.Context, agentID string, afterSeq int64, limit int) ([]AttentionCandidate, error) {
	if err := PersonalityAgent(agentID).Validate(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > MaxHistoryLimit {
		limit = MaxHistoryLimit
	}
	rows, err := s.pool.Query(ctx,
		`SELECT ai.candidate_id, ai.candidate_seq, ai.personality_agent_id,
		        p.place_id, p.kind, p.workspace_id, p.name, p.topic, p.visibility, p.last_seq,
		        ai.message_id, ai.message_seq, m.author_kind, m.author_id,
		        ai.trigger_reason, ai.urgency, ai.unread_from, ai.unread_to, ai.arrival_time
		 FROM attention_inbox ai
		 JOIN places p ON p.place_id = ai.place_id
		 JOIN messages m ON m.message_id = ai.message_id
		 WHERE ai.personality_agent_id = $1 AND ai.candidate_seq > $2 AND ai.resolved_at IS NULL
		 ORDER BY ai.candidate_seq ASC
		 LIMIT $3`, agentID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending candidates: %w", err)
	}
	defer rows.Close()
	var out []AttentionCandidate
	for rows.Next() {
		var (
			c           AttentionCandidate
			workspaceID *string
			name        *string
			authorKind  string
		)
		if err := rows.Scan(&c.CandidateID, &c.CandidateSeq, &c.PersonalityAgentID,
			&c.Place.PlaceID, &c.Place.Kind, &workspaceID, &name,
			&c.Place.Topic, &c.Place.Visibility, &c.Place.LastSeq,
			&c.MessageID, &c.MessageSeq, &authorKind, &c.Author.ID,
			&c.TriggerReason, &c.Urgency, &c.UnreadFrom, &c.UnreadTo, &c.ArrivalTime); err != nil {
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		c.Author.Kind = ParticipantKind(authorKind)
		if workspaceID != nil {
			c.Place.WorkspaceID = *workspaceID
		}
		if name != nil {
			c.Place.Name = *name
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate candidates: %w", err)
	}
	// Addressees are the message's resolved mentions (provenance delivery
	// data); load them in one query.
	if len(out) > 0 {
		messages := make([]Message, len(out))
		for i, c := range out {
			messages[i] = Message{MessageID: c.MessageID}
		}
		if err := s.attachMentions(ctx, messages); err != nil {
			return nil, err
		}
		for i := range out {
			out[i].Addressees = messages[i].Mentions
		}
	}
	return out, nil
}

// AckCandidates advances the agent's delivery cursor. Monotonic and idempotent
// like every cursor here; acking beyond what was issued is rejected so the
// cursor cannot claim undelivered candidates.
func (s *Store) AckCandidates(ctx context.Context, agentID string, throughSeq int64) error {
	if err := PersonalityAgent(agentID).Validate(); err != nil {
		return err
	}
	if throughSeq < 0 {
		return fmt.Errorf("ack seq must be non-negative")
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE attention_cursors
		 SET acked_seq = GREATEST(acked_seq, LEAST(issued_seq, $2)), updated_at = now()
		 WHERE personality_agent_id = $1 AND $2 <= issued_seq`,
		agentID, throughSeq)
	if err != nil {
		return fmt.Errorf("ack candidates: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrSeqBeyondLatest
	}
	return nil
}

// AckedCandidateSeq returns the agent's current delivery cursor.
func (s *Store) AckedCandidateSeq(ctx context.Context, agentID string) (int64, error) {
	if err := PersonalityAgent(agentID).Validate(); err != nil {
		return 0, err
	}
	var acked int64
	err := s.pool.QueryRow(ctx,
		"SELECT acked_seq FROM attention_cursors WHERE personality_agent_id = $1",
		agentID).Scan(&acked)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("query acked seq: %w", err)
	}
	return acked, nil
}

// supersedeCandidates resolves pending candidates whose message the agent has
// read past (凍結契約 v1: placeのread cursorが候補のseqを超えたらsuperseded —
// スマホで読んだらPCの通知が消えるのと同じ)。Called from ReadThrough when the
// reader is a personality agent.
func (s *Store) supersedeCandidates(ctx context.Context, agentID, placeID string, throughSeq int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE attention_inbox
		 SET resolved_at = now(), resolution = 'superseded'
		 WHERE personality_agent_id = $1 AND place_id = $2
		   AND resolved_at IS NULL AND message_seq <= $3`,
		agentID, placeID, throughSeq)
	if err != nil {
		return fmt.Errorf("supersede candidates: %w", err)
	}
	return nil
}
