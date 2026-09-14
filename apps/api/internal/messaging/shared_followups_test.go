package messaging

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// Fixtures for the shared-messaging followups slice own their own database
// prefix (sumi_shared_followups_) and port range (10670-10689); they never
// touch the earlier shared-intake fixtures or another worker's resources.
func createSharedFollowupsDB(t *testing.T) *pgxpool.Pool {
	return createOwnedTestDB(t, "sumi_shared_followups_")
}

func newFollowupsWorld(t *testing.T, ctx context.Context) world {
	t.Helper()
	return newWorldOnPool(t, ctx, createSharedFollowupsDB(t))
}

// pendingEvent reads the frozen event payload of a still-pending delivery row.
func pendingEvent(t *testing.T, ctx context.Context, w world, paID string) AgentAttentionEvent {
	t.Helper()
	var payload []byte
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT payload FROM agent_attention_deliveries
		WHERE personality_agent_id = $1 AND admitted_at IS NULL AND suppressed_at IS NULL
		ORDER BY event_id LIMIT 1`, paID).Scan(&payload); err != nil {
		t.Fatalf("pending event: %v", err)
	}
	var event AgentAttentionEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	return event
}

func deliveryRow(t *testing.T, ctx context.Context, w world, eventID string) (admitted bool, suppressed bool, reason string, payload []byte) {
	t.Helper()
	err := w.store.core.pool.QueryRow(ctx, `
		SELECT admitted_at IS NOT NULL, suppressed_at IS NOT NULL,
		       COALESCE(suppression_reason, ''), payload
		FROM agent_attention_deliveries WHERE event_id = $1`, eventID).
		Scan(&admitted, &suppressed, &reason, &payload)
	if err != nil {
		t.Fatalf("delivery row: %v", err)
	}
	return admitted, suppressed, reason, payload
}

func coreInputsFor(t *testing.T, ctx context.Context, w world, paID string) []agentstate.Input {
	t.Helper()
	rows, err := w.store.core.pool.Query(ctx, `
		SELECT input_id, kind, actor_kind, actor_id, source_surface, thread_id,
		       occurred_at, attention, status, payload
		FROM core_inputs WHERE persona_id = $1 ORDER BY admission_seq`, paID)
	if err != nil {
		t.Fatalf("read core inputs: %v", err)
	}
	defer rows.Close()
	var inputs []agentstate.Input
	for rows.Next() {
		var in agentstate.Input
		var occurred *time.Time
		if err := rows.Scan(&in.InputID, &in.Kind, &in.ActorKind, &in.ActorID,
			&in.SourceSurface, &in.ThreadID, &occurred, &in.Attention, &in.Status,
			&in.Payload); err != nil {
			t.Fatalf("scan core input: %v", err)
		}
		in.OccurredAt = occurred
		in.PersonaID = paID
		inputs = append(inputs, in)
	}
	return inputs
}

// An edit of an already-delivered message becomes a second, attributed input:
// the original stays untouched and the new one carries the current view.
func TestSharedIntakeEditDeliversNewCurrentViewEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)

	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 最初の相談")
	if stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil || stats.Admitted != 1 {
		t.Fatalf("drain original: %+v %v", stats, err)
	}
	sender := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	edited, err := sender.EditMessage(ctx, ch.PlaceID, msg.MessageID, "訂正後の相談", msg.Revision)
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil || stats.Admitted != 1 {
		t.Fatalf("drain edit: %+v %v", stats, err)
	}
	inputs := coreInputsFor(t, ctx, w, w.agent.ID)
	if len(inputs) != 2 {
		t.Fatalf("inputs = %d, want original + edit event", len(inputs))
	}
	original, update := inputs[0], inputs[1]
	if original.Payload["text"] != "@Kuro（Yohaku） 最初の相談" || original.Payload["message_change"] != nil {
		t.Fatalf("original input changed: %+v", original.Payload)
	}
	if update.Payload["message_change"] != "edited" || update.Payload["text"] != "訂正後の相談" ||
		update.Payload["message_id"] != msg.MessageID || update.Payload["message_revision"] != float64(edited.Revision) {
		t.Fatalf("edit input = %+v", update.Payload)
	}
	// The edit is re-evaluated against current notification decisions: the
	// edited content no longer mentions Kuro, so he is reached at his "all"
	// level as observe — the recorded recipient is still included.
	if update.ActorKind != "human" || update.ActorID != w.humanB.ID || update.Attention != "observe" {
		t.Fatalf("edit input attribution = %+v/%s", update, update.Attention)
	}
	// The deliveries row records the new revision as a distinct deduped source.
	var revision int64
	var change string
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT source_revision, payload->>'change' FROM agent_attention_deliveries
		WHERE message_id=$1 AND personality_agent_id=$2 AND admitted_at IS NOT NULL
		ORDER BY source_revision DESC LIMIT 1`, msg.MessageID, w.agent.ID).Scan(&revision, &change); err != nil {
		t.Fatalf("delivery row: %v", err)
	}
	if revision != edited.Revision || change != "edited" {
		t.Fatalf("edit delivery = rev %d change %q", revision, change)
	}
}

// A deletion reaches the recorded recipient as a tombstone event: the original
// input is preserved, the new input reports the current view at "observe".
func TestSharedIntakeDeleteDeliversTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)

	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 消す前の相談")
	if stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil || stats.Admitted != 1 {
		t.Fatalf("drain original: %+v %v", stats, err)
	}
	sender := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	if _, err := sender.DeleteMessage(ctx, ch.PlaceID, msg.MessageID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil || stats.Admitted != 1 || stats.Suppressed != 0 {
		t.Fatalf("drain delete: %+v %v", stats, err)
	}
	inputs := coreInputsFor(t, ctx, w, w.agent.ID)
	if len(inputs) != 2 {
		t.Fatalf("inputs = %d, want original + tombstone", len(inputs))
	}
	if inputs[0].Payload["text"] != "@Kuro（Yohaku） 消す前の相談" {
		t.Fatalf("original input rewritten: %+v", inputs[0].Payload)
	}
	tombstone := inputs[1]
	if tombstone.Payload["message_change"] != "deleted" || tombstone.Payload["text"] != nil ||
		tombstone.Payload["message_id"] != msg.MessageID || tombstone.Attention != "observe" ||
		tombstone.ActorID != w.humanB.ID {
		t.Fatalf("tombstone input = %+v", tombstone)
	}
}

// An edit that newly mentions a second secretary reaches that secretary as its
// first view of the message — the change cue still applies.
func TestSharedIntakeEditReachesNewlyMentionedSecretary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)

	// The original goes out while Kuro is the only secretary in the place.
	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 最初は君だけ")
	if stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil || stats.Admitted != 1 {
		t.Fatalf("drain original: %+v %v", stats, err)
	}

	secondID, err := koseki.New(w.store.core.pool).MintSecretary(ctx, w.humanB.ID)
	if err != nil {
		t.Fatalf("mint second secretary: %v", err)
	}
	second := PersonalityAgent(secondID)
	if _, err := w.store.core.pool.Exec(ctx,
		"UPDATE agents SET display_name='Shiro' WHERE personality_agent_id=$1", secondID); err != nil {
		t.Fatal(err)
	}
	if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, second, RoleMember); err != nil {
		t.Fatalf("add second secretary: %v", err)
	}

	sender := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	if _, err := sender.EditMessage(ctx, ch.PlaceID, msg.MessageID, "@Kuro（Yohaku） @Shiro（Haru） ふたりとも", msg.Revision); err != nil {
		t.Fatalf("edit: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	// Kuro's edit event + Shiro's first view of the message via the new mention.
	if stats.Admitted != 2 {
		t.Fatalf("admitted %d, want 2", stats.Admitted)
	}
	shiro := coreInputsFor(t, ctx, w, second.ID)
	if len(shiro) != 1 || shiro[0].Payload["message_change"] != "edited" ||
		shiro[0].Payload["reason"] != NotifyReasonMention || shiro[0].Attention != "reply" {
		t.Fatalf("newly mentioned secretary input = %+v", shiro)
	}
}

// f-shared-intake-123: a persona permanently transferred off this placement
// can never accept the input — the delivery suppresses terminally with the
// truthful reason, keeps the frozen message, and stops retrying.
func TestSharedIntakeTransferredPersonaGetsTerminalReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)
	if _, _, err := coreStore.EnsurePersona(ctx, w.agent.ID, &w.humanA.ID, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	if _, err := w.store.core.pool.Exec(ctx,
		"UPDATE core_personas SET authority='transferred' WHERE persona_id=$1", w.agent.ID); err != nil {
		t.Fatal(err)
	}
	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 転出後の相談")
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Suppressed != 1 || stats.Retried != 0 || stats.Admitted != 0 {
		t.Fatalf("drain to transferred persona: %+v %v", stats, err)
	}
	var reason string
	var admittedAt *time.Time
	var payload []byte
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT COALESCE(suppression_reason,''), admitted_at, payload
		FROM agent_attention_deliveries WHERE message_id=$1 AND personality_agent_id=$2`,
		msg.MessageID, w.agent.ID).Scan(&reason, &admittedAt, &payload); err != nil {
		t.Fatalf("delivery row: %v", err)
	}
	if reason != "recipient_transferred" || admittedAt != nil {
		t.Fatalf("terminal state reason=%q admitted=%v", reason, admittedAt)
	}
	// The original message stays inspectable in the suppressed row.
	var stored AgentAttentionEvent
	if err := json.Unmarshal(payload, &stored); err != nil || stored.Content != "@Kuro（Yohaku） 転出後の相談" {
		t.Fatalf("frozen payload lost: %s %v", payload, err)
	}
	// Nothing was admitted to the core queue, and the terminal row is never
	// retried on later drains.
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 0 {
		t.Fatalf("transferred persona got %d inputs", n)
	}
	stats, err = w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Retried != 0 || stats.Suppressed != 0 || stats.Admitted != 0 {
		t.Fatalf("terminal row retried: %+v %v", stats, err)
	}
}

// A sealed persona can still come back to active when the transfer aborts —
// its pending delivery must stay retryable, never suppressed.
func TestSharedIntakeSealedPersonaStaysPendingAndRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)
	if _, _, err := coreStore.EnsurePersona(ctx, w.agent.ID, &w.humanA.ID, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	if _, err := w.store.core.pool.Exec(ctx,
		"UPDATE core_personas SET authority='sealed' WHERE persona_id=$1", w.agent.ID); err != nil {
		t.Fatal(err)
	}
	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 封印中の相談")
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err == nil || stats.Retried != 1 || stats.Suppressed != 0 {
		t.Fatalf("sealed drain: %+v %v", stats, err)
	}
	admitted, suppressed, reason, _ := deliveryRow(t, ctx, w, pendingEventID(t, ctx, w, msg))
	if admitted || suppressed || reason != "" {
		t.Fatalf("sealed delivery suppressed or admitted: admitted=%t suppressed=%t reason=%q", admitted, suppressed, reason)
	}
	// Abort: the persona returns to active and the same delivery lands.
	if _, err := w.store.core.pool.Exec(ctx,
		`UPDATE core_personas SET authority='active' WHERE persona_id=$1`, w.agent.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.core.pool.Exec(ctx,
		`UPDATE agent_attention_deliveries SET next_attempt_at=now() WHERE message_id=$1`, msg.MessageID); err != nil {
		t.Fatal(err)
	}
	stats, err = w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != 1 {
		t.Fatalf("post-abort drain: %+v %v", stats, err)
	}
	inputs := coreInputsFor(t, ctx, w, w.agent.ID)
	if len(inputs) != 1 || inputs[0].Payload["text"] != "@Kuro（Yohaku） 封印中の相談" {
		t.Fatalf("recovered inputs = %+v", inputs)
	}
}

func pendingEventID(t *testing.T, ctx context.Context, w world, msg Message) string {
	t.Helper()
	var id string
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT event_id::text FROM agent_attention_deliveries WHERE message_id=$1`, msg.MessageID).Scan(&id); err != nil {
		t.Fatalf("pending event id: %v", err)
	}
	return id
}

// An input id already bound to different content is not this event's receipt:
// the delivery must not claim it — it suppresses with input_conflict instead.
func TestSharedIntakeConflictingInputIDIsNotDelivered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)

	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 衝突する確認")
	event := pendingEvent(t, ctx, w, w.agent.ID)
	if _, _, err := coreStore.EnsurePersona(ctx, w.agent.ID, &w.humanA.ID, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	occurred := event.OccurredAt
	if _, _, err := coreStore.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agent.ID, InputID: "messaging:" + event.EventID, Kind: "message",
		Payload:   map[string]any{"text": "全然別の内容", "message_id": msg.MessageID},
		ActorKind: "human", ActorID: w.humanB.ID, SourceSurface: "messaging",
		ThreadID: ch.PlaceID, OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("pre-admit conflicting input: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Suppressed != 1 || stats.Admitted != 0 || stats.Retried != 0 {
		t.Fatalf("conflict drain: %+v %v", stats, err)
	}
	admitted, suppressed, reason, _ := deliveryRow(t, ctx, w, event.EventID)
	if admitted || !suppressed || reason != "input_conflict" {
		t.Fatalf("conflict row admitted=%t suppressed=%t reason=%q", admitted, suppressed, reason)
	}
	// The conflicting input keeps its own payload — nothing rewrote it, and
	// no second row appeared.
	inputs := coreInputsFor(t, ctx, w, w.agent.ID)
	if len(inputs) != 1 || inputs[0].Payload["text"] != "全然別の内容" {
		t.Fatalf("conflicting input changed: %+v", inputs)
	}
}

// The inverse: an input id whose stored content is exactly this event is a
// real replay — Lookup reconciles the receipt without a second admission.
func TestSharedIntakeMatchingReplayDeduplicates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)

	w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 再送の確認")
	event := pendingEvent(t, ctx, w, w.agent.ID)
	if _, _, err := coreStore.EnsurePersona(ctx, w.agent.ID, &w.humanA.ID, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	// The receipt acknowledgement was lost after SubmitInput committed — the
	// durable input already carries this exact event.
	input, _, err := coreStore.SubmitInput(ctx, coreInputFromEvent(event))
	if err != nil {
		t.Fatalf("pre-admit matching input: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != 1 || stats.Retried != 0 {
		t.Fatalf("replay drain: %+v %v", stats, err)
	}
	admitted, suppressed, _, _ := deliveryRow(t, ctx, w, event.EventID)
	if !admitted || suppressed {
		t.Fatalf("matching replay not admitted: %t/%t", admitted, suppressed)
	}
	var seq int64
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT admitted_command_seq FROM agent_attention_deliveries WHERE event_id=$1`,
		event.EventID).Scan(&seq); err != nil || seq != input.CreatedAt.UnixMilli() {
		t.Fatalf("receipt seq %d vs input created %v: %v", seq, input.CreatedAt, err)
	}
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 1 {
		t.Fatalf("replay created a second input: %d", n)
	}
}

// An edit superseded by a deletion before delivery: the original and the edit
// events are suppressed as unavailable, but the tombstone still delivers.
func TestSharedIntakeEditThenDeleteDeliversOnlyTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)

	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 編集して消す")
	sender := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	if _, err := sender.EditMessage(ctx, ch.PlaceID, msg.MessageID, "消す前の訂正", msg.Revision); err != nil {
		t.Fatalf("edit: %v", err)
	}
	if _, err := sender.DeleteMessage(ctx, ch.PlaceID, msg.MessageID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Suppressed != 2 || stats.Admitted != 1 {
		t.Fatalf("drain: %+v %v", stats, err)
	}
	inputs := coreInputsFor(t, ctx, w, w.agent.ID)
	if len(inputs) != 1 || inputs[0].Payload["message_change"] != "deleted" ||
		inputs[0].Payload["message_id"] != msg.MessageID {
		t.Fatalf("only the tombstone should be admitted: %+v", inputs)
	}
	// The suppressed rows keep their inspectable reasons.
	var reasons []string
	rows, err := w.store.core.pool.Query(ctx, `
		SELECT suppression_reason FROM agent_attention_deliveries
		WHERE message_id=$1 AND suppressed_at IS NOT NULL ORDER BY source_revision`, msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			t.Fatal(err)
		}
		reasons = append(reasons, r)
	}
	if len(reasons) != 2 || reasons[0] != "source_unavailable" || reasons[1] != "source_unavailable" {
		t.Fatalf("suppression reasons = %v", reasons)
	}
}

// The real Node core against the real state service and real PostgreSQL: an
// edit lands as a second journaled input_received carrying message_change,
// and the original receipt keeps its own frozen provenance.
func TestSharedFollowupsEditDeleteReachRealCoreJournal(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not on PATH; skipping Node core e2e")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	w := newFollowupsWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	delivery, _ := newSharedIntakeDelivery(t, w)
	coreSrv, baseURL := startOwnedCoreServer(t, w, delivery, 10670, 10689)

	sender := w.store.mustScopeForPlace(t, ctx, dm.PlaceID, w.humanA)
	msg, _, err := sender.AppendMessage(ctx, AppendInput{PlaceID: dm.PlaceID, Content: "初めの相談", ClientNonce: "fu-orig"})
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"SUMI_STATE_URL="+baseURL,
		"SUMI_PERSONA_ID="+w.agent.ID,
		"SUMI_PERSONA_TOKEN="+coreSrv.PersonaToken(w.agent.ID),
		"SUMI_MODEL_PROVIDER=mock",
		"SUMI_LEASE_TTL_MS=4000",
		"SUMI_ONCE_IDLE_MS=800",
	)
	runOnce := func() {
		t.Helper()
		cmd := exec.CommandContext(ctx, nodePath,
			filepath.Join(coreRootDir(t), "src", "host", "local.ts"), "--once")
		cmd.Dir = coreRootDir(t)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("core --once failed: %v\n%s", err, out)
		}
	}
	drain := func(want int) {
		t.Helper()
		stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
		if err != nil || stats.Admitted != want {
			t.Fatalf("drain: %+v %v", stats, err)
		}
	}
	drain(1)
	runOnce()
	if _, err := sender.EditMessage(ctx, dm.PlaceID, msg.MessageID, "訂正後の相談", msg.Revision); err != nil {
		t.Fatalf("edit: %v", err)
	}
	drain(1)
	runOnce()
	if _, err := sender.DeleteMessage(ctx, dm.PlaceID, msg.MessageID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	drain(1)
	runOnce()

	rows, err := w.store.core.pool.Query(ctx, `
		SELECT payload FROM core_events
		WHERE persona_id=$1 AND kind='input_received' ORDER BY seq`, w.agent.ID)
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	defer rows.Close()
	var payloads []map[string]any
	for rows.Next() {
		var p map[string]any
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, p)
	}
	if len(payloads) != 3 {
		t.Fatalf("input_received count = %d, want original+edit+tombstone", len(payloads))
	}
	if payloads[0]["text"] != "初めの相談" || payloads[0]["message_change"] != nil {
		t.Fatalf("original receipt = %+v", payloads[0])
	}
	if payloads[1]["text"] != "訂正後の相談" || payloads[1]["message_change"] != "edited" ||
		payloads[1]["message_id"] != msg.MessageID {
		t.Fatalf("edit receipt = %+v", payloads[1])
	}
	if payloads[2]["message_change"] != "deleted" || payloads[2]["message_id"] != msg.MessageID ||
		payloads[2]["attention"] != "observe" {
		t.Fatalf("tombstone receipt = %+v", payloads[2])
	}
	// The secretary never posted: observe traffic and change events produce
	// journal entries, not automatic Messaging replies.
	var agentPosts int
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM messages WHERE place_id=$1 AND author_kind='personality_agent'`,
		dm.PlaceID).Scan(&agentPosts); err != nil {
		t.Fatal(err)
	}
	if agentPosts != 0 {
		t.Fatalf("secretary auto-posted %d messages", agentPosts)
	}
}
