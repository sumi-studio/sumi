package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// AttentionCandidate は agent 側の「呼ばれた」の受け口である。人間の Web Push
// と同じひとつの判定（NotificationDecisionsFor）から分かれた、もう一本の
// adapter——凍結契約 v1「Push 通知レイヤーとの対応」の右列にあたる。
//
//	人間  : アプリを見ていない → APNs/FCM   → 端末の OS 設定
//	agent : runtime 停止(cold) → wake gate  → 本人の通知設定
//
// 正本を shared control plane 側に置くのは契約の決定である（runtime 停止中に
// 届いたものを受け取れるのは shared 側だけ）。agent-private DB は projection。
//
// **これは暫定配線である。** 覚醒トリガの本設計は ADR 0010 / issue #173 に
// あり、そこでは候補の到着そのものが agent を起こす。ここにあるのは
// 「積む」と「本人が道具で取りに来られる」までで、自動覚醒は無い。本設計が
// 入るとき、この queue は捨てるのではなく wake gate の入力になる想定。
type AttentionCandidate struct {
	CandidateID string
	Agent       ParticipantRef
	// CandidateSeq は agent ごとの単調増加。place の seq とは別軸である
	// （凍結契約 v1 §2）。ack はこの軸の cursor を進めることで行う。
	CandidateSeq int64
	PlaceID      string
	PlaceKind    string
	MessageSeq   int64
	Reason       string
	CreatedAt    time.Time
	ConsumedAt   *time.Time
}

// MaxAttentionCandidates bounds one poll. 溜まった候補を一息に全部渡すのは
// 「見渡す」ではなく「浴びせる」なので、頭から一定数だけ渡す。残りは次の poll
// で取れる（消さないので失われない）。
const MaxAttentionCandidates = 50

const defaultAttentionCandidates = 20

// recordAttentionCandidates queues one committed message for every agent the
// server decided to call. 人間ぶんの decision はここでは無視する（そちらは
// push.go）。
//
// メッセージ確定後のベストエフォートである。ここが落ちても未読は place の seq
// から再構成できるので、次の覚醒で本人が見渡せる（凍結契約 v1「欠落時」）。
func (s *Store) recordAttentionCandidates(ctx context.Context, place Place, msg Message, decisions []NotificationDecision) {
	for _, decision := range decisions {
		if decision.Participant.Kind != KindPersonalityAgent {
			continue
		}
		if err := s.appendAttentionCandidate(ctx, decision.Participant, place, msg, decision.Reason); err != nil {
			log.Printf("messaging attention: queue candidate for %s: %v", decision.Participant.Key(), err)
		}
	}
}

func (s *Store) appendAttentionCandidate(
	ctx context.Context, agent ParticipantRef, place Place, msg Message, reason string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin attention candidate: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// candidate_seq は agent ごとに詰まっていなければならない。同じ agent 宛の
	// 発行を直列化しておかないと、二人が同時に呼びかけたときに同じ番号を
	// 取り合う。place ごとではなく agent ごとに掛けるのは、番号の軸が agent
	// だからである。
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", agent.ID); err != nil {
		return fmt.Errorf("lock attention sequence: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO attention_candidates
		   (candidate_id, agent_id, candidate_seq, place_id, message_seq, reason)
		 SELECT $1::uuidv7, $2::uuidv7, COALESCE(MAX(candidate_seq), 0) + 1, $3::uuidv7, $4, $5
		 FROM attention_candidates WHERE agent_id = $2::uuidv7
		 ON CONFLICT (agent_id, place_id, message_seq) DO NOTHING`,
		newUUIDv7(), agent.ID, place.PlaceID, msg.Seq, reason); err != nil {
		return fmt.Errorf("insert attention candidate: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attention candidate: %w", err)
	}
	return nil
}

// PendingAttentionCandidates lists the agent's own unconsumed candidates,
// oldest first.
//
// 既に読んだ place の候補は superseded として黙って解決する（凍結契約 v1
// 「read_through との連動」）。読んだものでもう一度起こさないためで、候補が
// 消えるのではなく「もう解決済み」になる。
func (s *Store) PendingAttentionCandidates(
	ctx context.Context, agent ParticipantRef, limit int,
) ([]AttentionCandidate, error) {
	if agent.Kind != KindPersonalityAgent {
		return nil, fmt.Errorf("attention candidates belong to a personality agent")
	}
	if err := agent.Validate(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = defaultAttentionCandidates
	}
	if limit > MaxAttentionCandidates {
		limit = MaxAttentionCandidates
	}
	if err := s.supersedeReadCandidates(ctx, agent); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT ac.candidate_id, ac.candidate_seq, ac.place_id, p.kind,
		        ac.message_seq, ac.reason, ac.created_at
		 FROM attention_candidates ac
		 JOIN places p USING (place_id)
		 WHERE ac.agent_id = $1 AND ac.consumed_at IS NULL
		 ORDER BY ac.candidate_seq
		 LIMIT $2`, agent.ID, limit)
	if err != nil {
		return nil, fmt.Errorf("query attention candidates: %w", err)
	}
	defer rows.Close()
	candidates := []AttentionCandidate{}
	for rows.Next() {
		candidate := AttentionCandidate{Agent: agent}
		if err := rows.Scan(&candidate.CandidateID, &candidate.CandidateSeq, &candidate.PlaceID,
			&candidate.PlaceKind, &candidate.MessageSeq, &candidate.Reason,
			&candidate.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan attention candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate attention candidates: %w", err)
	}
	return candidates, nil
}

// ConsumeAttentionCandidates acks everything up to and including throughSeq.
// 冪等で、単調である：古い generation の ack が cursor を巻き戻さないよう、
// 既に consumed の行には触れない。行は消さない——予算切れや再起動で候補を
// 捨てないという決定（ADR 0011 §9）に、残すことで従う。
func (s *Store) ConsumeAttentionCandidates(
	ctx context.Context, agent ParticipantRef, throughSeq int64,
) (int64, error) {
	if agent.Kind != KindPersonalityAgent {
		return 0, fmt.Errorf("attention candidates belong to a personality agent")
	}
	if err := agent.Validate(); err != nil {
		return 0, err
	}
	if throughSeq <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE attention_candidates SET consumed_at = now()
		 WHERE agent_id = $1 AND candidate_seq <= $2 AND consumed_at IS NULL`,
		agent.ID, throughSeq)
	if err != nil {
		return 0, fmt.Errorf("consume attention candidates: %w", err)
	}
	return tag.RowsAffected(), nil
}

// supersedeReadCandidates resolves candidates whose place the agent has already
// read past. 「既に読んだものでもう一度起こさない」（凍結契約 v1）。
func (s *Store) supersedeReadCandidates(ctx context.Context, agent ParticipantRef) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE attention_candidates ac SET consumed_at = now()
		 FROM read_markers rm
		 WHERE ac.agent_id = $1 AND ac.consumed_at IS NULL
		   AND rm.place_id = ac.place_id
		   AND rm.member_kind = 'personality_agent' AND rm.member_id = $1
		   AND rm.last_read_seq >= ac.message_seq`, agent.ID); err != nil {
		return fmt.Errorf("supersede read attention candidates: %w", err)
	}
	return nil
}

// LatestAttentionSeq is the highest candidate_seq ever issued to the agent,
// consumed or not. 「どこまで配られたか」を本人が知るための目盛りで、
// ConsumeAttentionCandidates に渡す cursor の上限でもある。
func (s *Store) LatestAttentionSeq(ctx context.Context, agent ParticipantRef) (int64, error) {
	if err := agent.Validate(); err != nil {
		return 0, err
	}
	var seq int64
	err := s.pool.QueryRow(ctx,
		"SELECT COALESCE(MAX(candidate_seq), 0) FROM attention_candidates WHERE agent_id = $1",
		agent.ID).Scan(&seq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("latest attention seq: %w", err)
	}
	return seq, nil
}
