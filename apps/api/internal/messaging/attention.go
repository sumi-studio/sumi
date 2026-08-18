package messaging

import (
	"context"
	"fmt"
	"time"
)

// AttentionCandidate は agent 側の「呼ばれた」の受け口である。人間の Web Push
// と同じひとつの判定から分かれた、もう一本の adapter——凍結契約 v1「Push 通知
// レイヤーとの対応」の右列にあたる。
//
//	人間  : アプリを見ていない → APNs/FCM   → 端末の OS 設定
//	agent : runtime 停止(cold) → wake gate  → 本人の通知設定
//
// 正本を shared control plane 側に置くのは契約の決定である（runtime 停止中に
// 届いたものを受け取れるのは shared 側だけ）。agent-private DB は projection。
//
// **候補は判定をやり直さない。** message_notification_intents が message と
// 同じ transaction で確定した typed intent の正本で（migration 0015）、
// attention_candidates はそこから本人ぶんを取り出して番号を振っただけの層
// である。候補を commit 後にベストエフォートで積み直す形にすると正本が二つ
// になり、commit と insert の間に呼びかけを落とす窓ができる。
//
// **番号は本人が受け取った順に振る。** 契約は candidate_seq を「agent ごとの
// 単調増加」と定め、ack をその cursor と定義している。intent 側に採番して
// しまうと、同時 commit で採番順と可視順がずれ、cursor が未読の候補を飛び越す。
// 採番を poll 側に置けば、番号の順序と「本人が見た順序」が定義上一致する。
//
// **これは暫定配線である。** 覚醒トリガの本設計は ADR 0010 / issue #173 に
// あり、そこでは候補の到着そのものが agent を起こす。ここにあるのは「積む」と
// 「本人が道具で取りに来られる」までで、自動覚醒は無い。本設計が入るとき、
// この inbox は捨てるのではなく wake gate の入力になる想定。
type AttentionCandidate struct {
	CandidateID string
	// CandidateSeq は agent ごとの単調増加。place の seq とは別軸である
	// （凍結契約 v1 §2）。ack はこの軸の cursor を進めることで行う。
	CandidateSeq int64
	PlaceID      string
	PlaceKind    string
	MessageSeq   int64
	Reason       string
	CreatedAt    time.Time
}

// AttentionInbox is one poll's answer: what is still waiting, how much this
// call acknowledged, and how far it is safe to acknowledge.
type AttentionInbox struct {
	Candidates []AttentionCandidate
	Consumed   int64
	// LatestSeq はこの Workspace での **配布済み高水位** である。candidate_seq は
	// agent ごとに単調だが、poll / ack は Workspace binding ごとの local-control
	// request なので、別 Workspace の配布はここへ含めない。server が実際に配った
	// 範囲を越えないため、caller が任意に大きい consume_through を渡しても、この
	// Workspace で未配布の候補は消えず次の poll で再配送される。
	LatestSeq int64
}

// MaxAttentionCandidates bounds one poll. 溜まった候補を一息に全部渡すのは
// 「見渡す」ではなく「浴びせる」なので、頭から一定数だけ渡す。残りは次の poll
// で取れる（消さないので失われない）。
const MaxAttentionCandidates = 50

const defaultAttentionCandidates = 20

// PollAttentionCandidates acknowledges through the given candidate_seq, numbers
// whatever notification intents this agent has not been offered yet, resolves
// the ones it has already read past, and returns what is still waiting.
//
// consume_through を先に適用するのは、「ここまで取り込んだ」と言ってから
// 「次は何か」を聞く順だからである。全部をひとつの transaction に入れるのは、
// 採番・ack・supersede が互いに追い越すと cursor の意味が壊れるため。
func (s *ScopedStore) PollAttentionCandidates(
	ctx context.Context, consumeThrough int64, limit int,
) (AttentionInbox, error) {
	if s.Scope.Actor.Kind != KindPersonalityAgent {
		// 人間の対応物は push 購読であって候補ではない（同じ判定、別の身体）。
		return AttentionInbox{}, fmt.Errorf("%w: attention candidates belong to a personality agent", ErrForbidden)
	}
	if consumeThrough < 0 {
		return AttentionInbox{}, fmt.Errorf("%w: consume_through", ErrInvalidScope)
	}
	if limit <= 0 {
		limit = defaultAttentionCandidates
	}
	if limit > MaxAttentionCandidates {
		limit = MaxAttentionCandidates
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AttentionInbox{}, fmt.Errorf("begin attention poll: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return AttentionInbox{}, err
	}
	agentID := s.Scope.Actor.ID
	// 番号の軸は agent なので、Workspace を混ぜずに直列化する。二つの Workspace
	// から同時に poll しても同じ番号を取り合わない。配布済み高水位はこの後に
	// Workspace ごとに読む。
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtext($1))",
		agentID); err != nil {
		return AttentionInbox{}, fmt.Errorf("lock attention sequence: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO attention_agent_inboxes (agent_id) VALUES ($1)
		ON CONFLICT (agent_id) DO NOTHING`, agentID); err != nil {
		return AttentionInbox{}, fmt.Errorf("ensure attention inbox: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO attention_workspace_inboxes (agent_id, workspace_id) VALUES ($1, $2)
		ON CONFLICT (agent_id, workspace_id) DO NOTHING`, agentID, s.Scope.WorkspaceID); err != nil {
		return AttentionInbox{}, fmt.Errorf("ensure workspace attention inbox: %w", err)
	}
	var deliveredThrough int64
	if err := tx.QueryRow(ctx, `
		SELECT delivered_through FROM attention_workspace_inboxes
		WHERE agent_id = $1 AND workspace_id = $2 FOR UPDATE`, agentID, s.Scope.WorkspaceID).Scan(&deliveredThrough); err != nil {
		return AttentionInbox{}, fmt.Errorf("read attention delivery cursor: %w", err)
	}

	inbox := AttentionInbox{Candidates: []AttentionCandidate{}}
	if consumeThrough > 0 {
		// The server, not the caller, enforces the delivery boundary. A large
		// cursor is accepted as a convenient idempotent retry, but it cannot
		// consume a candidate that has not been handed to this agent yet.
		if consumeThrough > deliveredThrough {
			consumeThrough = deliveredThrough
		}
		// ack は冪等で単調である：既に consumed の行には触れないので、古い
		// generation の ack が cursor を巻き戻さない。行は消さない——予算切れや
		// 再起動を理由に候補を捨てないという決定（ADR 0011 §9）に、残すことで従う。
		tag, err := tx.Exec(ctx, `
			UPDATE attention_candidates SET consumed_at = now()
			WHERE workspace_id = $1 AND agent_id = $2 AND candidate_seq <= $3 AND consumed_at IS NULL`,
			s.Scope.WorkspaceID, agentID, consumeThrough)
		if err != nil {
			return AttentionInbox{}, fmt.Errorf("consume attention candidates: %w", err)
		}
		inbox.Consumed = tag.RowsAffected()
	}

	if err := s.supersedeReadAttentionCandidates(ctx, tx); err != nil {
		return AttentionInbox{}, err
	}
	var waiting int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM attention_candidates
		WHERE workspace_id = $1 AND agent_id = $2 AND consumed_at IS NULL`,
		s.Scope.WorkspaceID, agentID).Scan(&waiting); err != nil {
		return AttentionInbox{}, fmt.Errorf("count pending attention candidates: %w", err)
	}
	// Allocate only the response's empty slots. Every newly allocated sequence
	// is therefore returned in this transaction; the per-agent delivery cursor
	// remains a gap-free acknowledgement boundary even across Workspaces.
	if waiting < limit {
		if err := s.numberAttentionCandidates(ctx, tx, membership.WorkspaceMemberID, limit-waiting); err != nil {
			return AttentionInbox{}, err
		}
	}
	// read marker は採番の直前にも query で見るが、ReadThrough が採番と同時に
	// commit することもある。返却直前にも同じ supersede を通し、既読済みの
	// candidate をこの応答へ載せない。
	if err := s.supersedeReadAttentionCandidates(ctx, tx); err != nil {
		return AttentionInbox{}, err
	}

	rows, err := tx.Query(ctx, `
		SELECT ac.candidate_id, ac.candidate_seq, ac.place_id, p.kind,
		       ac.message_seq, ac.reason, ac.created_at
		FROM attention_candidates ac
		JOIN places p ON p.workspace_id = ac.workspace_id AND p.place_id = ac.place_id
		WHERE ac.workspace_id = $1 AND ac.agent_id = $2 AND ac.consumed_at IS NULL
		ORDER BY ac.candidate_seq
		LIMIT $3`, s.Scope.WorkspaceID, agentID, limit)
	if err != nil {
		return AttentionInbox{}, fmt.Errorf("query attention candidates: %w", err)
	}
	for rows.Next() {
		var candidate AttentionCandidate
		if err := rows.Scan(&candidate.CandidateID, &candidate.CandidateSeq, &candidate.PlaceID,
			&candidate.PlaceKind, &candidate.MessageSeq, &candidate.Reason,
			&candidate.CreatedAt); err != nil {
			rows.Close()
			return AttentionInbox{}, fmt.Errorf("scan attention candidate: %w", err)
		}
		inbox.Candidates = append(inbox.Candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return AttentionInbox{}, fmt.Errorf("iterate attention candidates: %w", err)
	}
	rows.Close()

	if n := len(inbox.Candidates); n > 0 {
		// ORDER BY candidate_seq なので末尾が返した中の最大。新規採番は
		// 必ずこの応答へ入るため、ここまでが安全な Workspace ごとの ack cursor。
		if err := tx.QueryRow(ctx, `
			UPDATE attention_workspace_inboxes
			SET delivered_through = GREATEST(delivered_through, $2)
			WHERE agent_id = $1 AND workspace_id = $3
			RETURNING delivered_through`, agentID, inbox.Candidates[n-1].CandidateSeq, s.Scope.WorkspaceID).
			Scan(&deliveredThrough); err != nil {
			return AttentionInbox{}, fmt.Errorf("advance attention delivery cursor: %w", err)
		}
	}
	inbox.LatestSeq = deliveredThrough
	if err := tx.Commit(ctx); err != nil {
		return AttentionInbox{}, fmt.Errorf("commit attention poll: %w", err)
	}
	return inbox, nil
}

// numberAttentionCandidates turns the intents this agent has never been offered
// into numbered candidates, oldest first. 可視性は overview / unread と同じ
// 条件で引く：もう居ない place の呼びかけで起こされても、開いて読めない。
func (s *ScopedStore) numberAttentionCandidates(
	ctx context.Context, tx querier, workspaceMemberID string, limit int,
) error {
	if limit <= 0 {
		return nil
	}
	type pending struct {
		messageID  string
		placeID    string
		messageSeq int64
		reason     string
	}
	rows, err := tx.Query(ctx, `
		WITH visible_places AS (
			SELECT p.place_id, COALESCE(pm.visible_from_seq, 1) AS visible_from_seq,
			       COALESCE(rm.last_read_seq, 0) AS last_read_seq
			FROM places p
			LEFT JOIN place_members pm
			  ON pm.workspace_id = p.workspace_id AND pm.place_id = p.place_id
			 AND pm.workspace_member_id = $2 AND pm.left_at IS NULL
			LEFT JOIN read_markers rm
			  ON rm.place_id = p.place_id AND rm.place_member_id = pm.place_member_id
			WHERE p.workspace_id = $1
			  AND (p.kind = 'channel'
			       OR (p.kind IN ('dm', 'group_dm') AND pm.place_member_id IS NOT NULL))
		)
		SELECT m.message_id, m.place_id, m.seq, i.reason
		FROM message_notification_intents i
		JOIN messages m ON m.message_id = i.message_id
		JOIN visible_places vp ON vp.place_id = m.place_id
		LEFT JOIN attention_candidates ac
		  ON ac.workspace_id = m.workspace_id
		 AND ac.agent_id = i.recipient_id
		 AND ac.message_id = m.message_id
		WHERE i.recipient_kind = $3 AND i.recipient_id = $4
		  AND m.workspace_id = $1
		  AND m.deleted_at IS NULL
		  AND m.seq >= vp.visible_from_seq
		  AND m.seq > vp.last_read_seq
		  AND ac.candidate_id IS NULL
		ORDER BY i.issued_at, m.message_id
		LIMIT $5`,
		s.Scope.WorkspaceID, workspaceMemberID,
		KindPersonalityAgent, s.Scope.Actor.ID, limit)
	if err != nil {
		return fmt.Errorf("query un-numbered attention intents: %w", err)
	}
	var waiting []pending
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.messageID, &item.placeID, &item.messageSeq, &item.reason); err != nil {
			rows.Close()
			return fmt.Errorf("scan un-numbered attention intent: %w", err)
		}
		waiting = append(waiting, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate un-numbered attention intents: %w", err)
	}
	rows.Close()
	if len(waiting) == 0 {
		return nil
	}
	for _, item := range waiting {
		var next int64
		if err := tx.QueryRow(ctx, `
			UPDATE attention_agent_inboxes
			SET next_candidate_seq = next_candidate_seq + 1
			WHERE agent_id = $1
			RETURNING next_candidate_seq - 1`, s.Scope.Actor.ID).Scan(&next); err != nil {
			return fmt.Errorf("allocate attention sequence: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO attention_candidates
			  (candidate_id, workspace_id, agent_id, candidate_seq,
			   message_id, place_id, message_seq, reason)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			newUUIDv7(), s.Scope.WorkspaceID, s.Scope.Actor.ID, next,
			item.messageID, item.placeID, item.messageSeq, item.reason); err != nil {
			return fmt.Errorf("number attention candidate: %w", err)
		}
	}
	return nil
}

// supersedeReadAttentionCandidates resolves candidates whose place the agent has
// already read past（凍結契約 v1「read_through との連動」）。既に読んだもので
// もう一度起こさないためで、候補が消えるのではなく「もう解決済み」になる。
//
// read_through の書き込み側ではなく poll 側に置く。契約は「place の read cursor
// が候補の seq を超えたら未 ack 候補は superseded」とだけ定めていて、いつ解決
// するかは決めていない。poll 側なら共有スパインの ReadThrough に触らずに済む。
func (s *ScopedStore) supersedeReadAttentionCandidates(ctx context.Context, tx querier) error {
	if _, err := tx.Exec(ctx, `
		UPDATE attention_candidates ac SET consumed_at = now()
		FROM place_members pm
		JOIN read_markers rm
		  ON rm.place_id = pm.place_id AND rm.place_member_id = pm.place_member_id
		WHERE ac.workspace_id = $1 AND ac.agent_id = $2 AND ac.consumed_at IS NULL
		  AND pm.workspace_id = $1 AND pm.place_id = ac.place_id
		  AND pm.member_kind = $3 AND pm.member_id = $2 AND pm.left_at IS NULL
		  AND rm.last_read_seq >= ac.message_seq`,
		s.Scope.WorkspaceID, s.Scope.Actor.ID, KindPersonalityAgent); err != nil {
		return fmt.Errorf("supersede read attention candidates: %w", err)
	}
	return nil
}
