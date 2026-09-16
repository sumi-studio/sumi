package messaging

// Tests for the delegated Messaging tool surface the TypeScript secretary
// core drives: every effect is claimed through the real agentstate operation
// ledger against real PostgreSQL, so authorization, idempotency, and epoch
// fences are exercised end to end rather than mocked.

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

func newCoreToolsWorld(t *testing.T, ctx context.Context) world {
	t.Helper()
	return newWorldOnPool(t, ctx, createOwnedTestDB(t, "sumi-core-messaging-parity-20260916-"))
}

// coreToolsFixture is one claimed writer turn on a prepared persona, with
// every Messaging effect registered exactly as cmd/server does.
type coreToolsFixture struct {
	world
	delivery *CoreAttentionDelivery
	core     *agentstate.Store
	gen      int64
	turnID   string
	pos      int
}

func newCoreToolsFixture(t *testing.T, ctx context.Context, w world) *coreToolsFixture {
	t.Helper()
	coreStore := agentstate.NewStore(w.store.core.pool)
	delivery := &CoreAttentionDelivery{Core: coreStore, Messaging: w.store.core}
	for tool, effect := range delivery.CoreToolEffects() {
		if err := coreStore.RegisterEffect(tool, effect); err != nil {
			t.Fatalf("register %s: %v", tool, err)
		}
	}
	release, err := delivery.Prepare(ctx, w.agent.ID)
	if err != nil {
		t.Fatalf("prepare persona: %v", err)
	}
	release()
	f := &coreToolsFixture{world: w, delivery: delivery, core: coreStore}
	occurred := time.Now()
	if _, _, err := coreStore.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agent.ID, InputID: "messaging:core-tools-wake", Kind: "message",
		Payload:   map[string]any{"text": "hello"},
		ActorKind: "human", ActorID: w.humanA.ID, SourceSurface: "messaging",
		OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input: %v", err)
	}
	lease, err := coreStore.AcquireWriter(ctx, w.agent.ID, "runtime", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	res, err := coreStore.LoadTurn(ctx, w.agent.ID, lease.Generation, "turn-1", 20)
	if err != nil || res.Turn == nil {
		t.Fatalf("load turn: %v %+v", err, res)
	}
	f.gen, f.turnID = lease.Generation, res.Turn.TurnID
	return f
}

// claim runs one tool call through the plan→claim pipeline. The returned
// operation is the recorded receipt; replay re-claims the same position.
func (f *coreToolsFixture) claim(t *testing.T, ctx context.Context, tool string, request map[string]any) agentstate.Operation {
	t.Helper()
	pos := f.pos
	f.pos++
	if _, _, err := f.core.SavePlan(ctx, f.agent.ID, f.turnID, f.gen, int64(pos),
		agentstate.Decision{
			Text:  "acting",
			Calls: []agentstate.PlanCall{{CallID: fmt.Sprintf("c%d", pos), Tool: tool, Route: "normal", Request: request}},
		}); err != nil {
		t.Fatalf("save plan %d: %v", pos, err)
	}
	op, _, fresh, err := f.core.ClaimOperation(ctx, f.agent.ID, f.turnID,
		f.gen, fmt.Sprintf("%s:op:%d", f.turnID, pos), tool, pos, request)
	if err != nil {
		t.Fatalf("claim %s: %v", tool, err)
	}
	if !fresh || op.Status != "done" {
		t.Fatalf("op = %+v fresh=%t", op, fresh)
	}
	return op
}

func (f *coreToolsFixture) claimError(t *testing.T, ctx context.Context, tool string, request map[string]any) error {
	t.Helper()
	pos := f.pos
	f.pos++
	if _, _, err := f.core.SavePlan(ctx, f.agent.ID, f.turnID, f.gen, int64(pos),
		agentstate.Decision{
			Text:  "acting",
			Calls: []agentstate.PlanCall{{CallID: fmt.Sprintf("c%d", pos), Tool: tool, Route: "normal", Request: request}},
		}); err != nil {
		t.Fatalf("save plan %d: %v", pos, err)
	}
	_, _, _, err := f.core.ClaimOperation(ctx, f.agent.ID, f.turnID,
		f.gen, fmt.Sprintf("%s:op:%d", f.turnID, pos), tool, pos, request)
	return err
}

// replay re-claims an earlier position with the identical request — the
// stored operation receipt must come back without re-running the effect.
func (f *coreToolsFixture) replay(t *testing.T, ctx context.Context, pos int, tool string, request map[string]any) agentstate.Operation {
	t.Helper()
	op, _, fresh, err := f.core.ClaimOperation(ctx, f.agent.ID, f.turnID,
		f.gen, fmt.Sprintf("%s:op:%d", f.turnID, pos), tool, pos, request)
	if err != nil {
		t.Fatalf("replay %s pos %d: %v", tool, pos, err)
	}
	if fresh {
		t.Fatalf("replay %s pos %d re-ran the effect", tool, pos)
	}
	return op
}

func TestCoreToolsReadSurface(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, ch.PlaceID, w.humanA, "金曜の持ち物リスト")
	w.send(t, ctx, ch.PlaceID, w.humanB, "共有済みです")
	f := newCoreToolsFixture(t, ctx, w)

	// overview: the same workspace view a human's app opens with.
	op := f.claim(t, ctx, MessagingCoreOverviewTool, map[string]any{"workspace_id": ws.WorkspaceID})
	channels, _ := op.Response["channels"].([]any)
	found := false
	for _, raw := range channels {
		if c, _ := raw.(map[string]any); c["channel_id"] == ch.PlaceID {
			found = true
		}
	}
	if !found {
		t.Fatalf("overview channels = %v, want %s", channels, ch.PlaceID)
	}
	if self, _ := op.Response["self"].(map[string]any); self["personality_agent_id"] != w.agent.ID {
		t.Fatalf("overview self = %+v", self)
	}

	// open: members, history, read cursor — the human sees the same place.
	op = f.claim(t, ctx, MessagingCoreOpenTool, map[string]any{"place_id": ch.PlaceID})
	messages, _ := op.Response["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("open messages = %d, want 2", len(messages))
	}
	first, _ := messages[0].(map[string]any)
	if first["content"] != "金曜の持ち物リスト" {
		t.Fatalf("first message = %+v", first)
	}
	place, _ := op.Response["place"].(map[string]any)
	if place["place_id"] != ch.PlaceID || place["name"] != "general" {
		t.Fatalf("open place = %+v", place)
	}
	members, _ := op.Response["members"].([]any)
	if len(members) != 3 {
		t.Fatalf("open members = %d, want 3", len(members))
	}

	// search: snippets under the same visibility rules.
	op = f.claim(t, ctx, MessagingCoreSearchTool, map[string]any{
		"workspace_id": ws.WorkspaceID, "query": "持ち物"})
	results, _ := op.Response["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("search results = %d, want 1", len(results))
	}
	hit, _ := results[0].(map[string]any)
	if hit["message_id"] != first["message_id"] {
		t.Fatalf("search hit = %+v", hit)
	}

	// A place in another workspace is not the secretary's to open.
	other := newSharedIntakeIsolatedPlace(t, ctx, w)
	if err := f.claimError(t, ctx, MessagingCoreOpenTool, map[string]any{"place_id": other}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign place open: got %v, want ErrBadRequest", err)
	}
	if err := f.claimError(t, ctx, MessagingCoreSearchTool, map[string]any{
		"workspace_id": ws.WorkspaceID, "place_id": other, "query": "x"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign place search: got %v, want ErrBadRequest", err)
	}
}

func TestCoreToolsCreateAndMutatePlaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	// Channel management is an app capability the workspace grants by role —
	// the secretary gets the same grant an owner would give a human member.
	grantManageChannels(t, ctx, w, ws.WorkspaceID, w.agent)
	f := newCoreToolsFixture(t, ctx, w)

	// create_channel → humans see the same place.
	op := f.claim(t, ctx, MessagingCoreCreateChannelTool, map[string]any{
		"workspace_id": ws.WorkspaceID, "name": "計画", "topic": "秋の計画"})
	channel, _ := op.Response["channel"].(map[string]any)
	newID, _ := channel["channel_id"].(string)
	if newID == "" || op.Response["created"] != true {
		t.Fatalf("create channel = %+v", op.Response)
	}
	if place, err := w.store.PlaceFor(ctx, newID, w.humanA); err != nil || place.Name != "計画" {
		t.Fatalf("human view of secretary channel = %+v %v", place, err)
	}
	// Replay: the stored receipt — the same channel, no second place.
	op = f.replay(t, ctx, 0, MessagingCoreCreateChannelTool, map[string]any{
		"workspace_id": ws.WorkspaceID, "name": "計画", "topic": "秋の計画"})
	if ch, _ := op.Response["channel"].(map[string]any); ch["channel_id"] != newID {
		t.Fatalf("replayed channel = %+v", ch)
	}

	// update_channel → the human's view reflects the rename.
	op = f.claim(t, ctx, MessagingCoreUpdateChannelTool, map[string]any{
		"place_id": newID, "name": "計画と記録"})
	if place, err := w.store.PlaceFor(ctx, newID, w.humanA); err != nil || place.Name != "計画と記録" {
		t.Fatalf("after update = %+v %v", place, err)
	}

	// duplicate_channel → a second channel.
	op = f.claim(t, ctx, MessagingCoreDuplicateChannelTool, map[string]any{
		"place_id": newID, "name": "計画のコピー"})
	dup, _ := op.Response["channel"].(map[string]any)
	if dup["channel_id"] == "" || dup["channel_id"] == newID {
		t.Fatalf("duplicate = %+v", dup)
	}

	// start_dm: one other → DM (deduplicated by the pair key), two → group DM.
	op = f.claim(t, ctx, MessagingCoreStartDMTool, map[string]any{
		"workspace_id": ws.WorkspaceID,
		"participants": []any{map[string]any{"kind": "human", "human_id": w.humanB.ID}},
	})
	dm, _ := op.Response["dm"].(map[string]any)
	dmID, _ := dm["dm_id"].(string)
	if dmID == "" || dm["kind"] != "dm" {
		t.Fatalf("start dm = %+v", dm)
	}
	if _, err := w.store.History(ctx, dmID, w.humanB, HistoryOptions{}); err != nil {
		t.Fatalf("human cannot read secretary dm: %v", err)
	}
	op = f.claim(t, ctx, MessagingCoreStartDMTool, map[string]any{
		"workspace_id": ws.WorkspaceID,
		"participants": []any{map[string]any{"kind": "human", "human_id": w.humanB.ID}},
	})
	if dm2, _ := op.Response["dm"].(map[string]any); dm2["dm_id"] != dmID {
		t.Fatalf("second dm = %+v, want reuse of %s", dm2, dmID)
	}
	op = f.claim(t, ctx, MessagingCoreStartDMTool, map[string]any{
		"workspace_id": ws.WorkspaceID,
		"participants": []any{
			map[string]any{"kind": "human", "human_id": w.humanA.ID},
			map[string]any{"kind": "human", "human_id": w.humanB.ID},
		},
	})
	group, _ := op.Response["dm"].(map[string]any)
	if group["kind"] != "group_dm" {
		t.Fatalf("group dm = %+v", group)
	}

	// create_thread anchored to a message in the general channel.
	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "スレッドで続きを")
	op = f.claim(t, ctx, MessagingCoreCreateThreadTool, map[string]any{
		"place_id": ch.PlaceID, "name": "続き", "message_id": msg.MessageID})
	thread, _ := op.Response["thread"].(map[string]any)
	if thread["thread_id"] == "" || op.Response["created"] != true {
		t.Fatalf("create thread = %+v", op.Response)
	}
	// The same request again answers with the existing thread.
	op = f.claim(t, ctx, MessagingCoreCreateThreadTool, map[string]any{
		"place_id": ch.PlaceID, "name": "続き", "message_id": msg.MessageID})
	if op.Response["created"] != false {
		t.Fatalf("second thread create = %+v", op.Response)
	}
	if th, _ := op.Response["thread"].(map[string]any); th["thread_id"] != thread["thread_id"] {
		t.Fatalf("second thread = %+v", th)
	}
}

func TestCoreToolsEditAndRetractOwnMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	f := newCoreToolsFixture(t, ctx, w)

	// The secretary sends, edits, and retracts its own message.
	op := f.claim(t, ctx, MessagingCoreTool, map[string]any{
		"place_id": ch.PlaceID, "content": "第一報"})
	messageID, _ := op.Response["message_id"].(string)
	if messageID == "" {
		t.Fatalf("send = %+v", op.Response)
	}
	op = f.claim(t, ctx, MessagingCoreEditMessageTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": messageID, "content": "訂正版"})
	m, _ := op.Response["message"].(map[string]any)
	if m["content"] != "訂正版" || m["edited_at"] == nil {
		t.Fatalf("edit response = %+v", m)
	}
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("human history: %v", err)
	}
	if history[0].Content != "訂正版" || history[0].EditedAt == nil {
		t.Fatalf("human sees edit = %+v", history[0])
	}
	// Replay of the same edit position returns the recorded outcome.
	f.replay(t, ctx, 1, MessagingCoreEditMessageTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": messageID, "content": "訂正版"})

	// Another actor's message can never be edited or retracted.
	foreign := w.send(t, ctx, ch.PlaceID, w.humanB, "自分のメモ")
	if err := f.claimError(t, ctx, MessagingCoreEditMessageTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": foreign.MessageID, "content": "改ざん"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("edit another's message: got %v, want ErrBadRequest", err)
	}
	if err := f.claimError(t, ctx, MessagingCoreDeleteMessageTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": foreign.MessageID}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("delete another's message: got %v, want ErrBadRequest", err)
	}

	// Own retract → tombstone both sides see.
	op = f.claim(t, ctx, MessagingCoreDeleteMessageTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": messageID})
	if op.Response["deleted"] != true {
		t.Fatalf("delete response = %+v", op.Response)
	}
	history, err = w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("human history after delete: %v", err)
	}
	var tombstone *Message
	for i := range history {
		if history[i].MessageID == messageID {
			tombstone = &history[i]
		}
	}
	if tombstone == nil || !tombstone.Deleted {
		t.Fatalf("human tombstone = %+v", tombstone)
	}
	// Editing a tombstone stays refused.
	if err := f.claimError(t, ctx, MessagingCoreEditMessageTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": messageID, "content": "墓標から"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("edit tombstone: got %v, want ErrBadRequest", err)
	}
}

func TestCoreToolsNotificationSettings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	f := newCoreToolsFixture(t, ctx, w)

	op := f.claim(t, ctx, MessagingCoreNotificationTool, map[string]any{"workspace_id": ws.WorkspaceID})
	setting, _ := op.Response["setting"].(map[string]any)
	defaults, _ := setting["defaults"].(map[string]any)
	if defaults["level"] != NotifyLevelAll {
		t.Fatalf("initial setting = %+v", setting)
	}

	// Own settings: default mute with a per-place override and keywords.
	op = f.claim(t, ctx, MessagingCoreNotificationTool, map[string]any{
		"workspace_id":   ws.WorkspaceID,
		"defaults_level": "mute",
		"keywords":       []any{"緊急"},
		"per_place": []any{map[string]any{
			"place": map[string]any{"channel_id": ch.PlaceID}, "level": "all"},
		},
	})
	setting, _ = op.Response["setting"].(map[string]any)
	defaults, _ = setting["defaults"].(map[string]any)
	if defaults["level"] != "mute" {
		t.Fatalf("updated defaults = %+v", defaults)
	}
	owner, _ := setting["owner"].(map[string]any)
	if owner["personality_agent_id"] != w.agent.ID {
		t.Fatalf("setting owner = %+v, want the secretary", owner)
	}

	// The stored setting is the secretary's own — read it back through the
	// domain API under the secretary's scope.
	scoped := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
	got, err := scoped.NotificationSettingFor(ctx)
	if err != nil {
		t.Fatalf("read setting: %v", err)
	}
	if got.DefaultLevel != "mute" || len(got.Keywords) != 1 || got.Keywords[0] != "緊急" ||
		len(got.PerPlace) != 1 || got.PerPlace[0].PlaceID != ch.PlaceID || got.PerPlace[0].Level != "all" {
		t.Fatalf("stored setting = %+v", got)
	}
	// An invalid level is a deterministic refusal.
	if err := f.claimError(t, ctx, MessagingCoreNotificationTool, map[string]any{
		"workspace_id": ws.WorkspaceID, "defaults_level": "loud"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("invalid level: got %v, want ErrBadRequest", err)
	}
}

func TestCoreToolsAttachmentsAndAttachmentOnlyWake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	root := filepath.Join(t.TempDir(), "attachments")
	blobs, err := NewDiskAttachments(root)
	if err != nil {
		t.Fatalf("disk attachments: %v", err)
	}
	if err := w.store.core.ConfigureAttachments(blobs, AttachmentPolicy{
		WorkspaceQuotaBytes:   64 << 20,
		WorkspaceQuotaObjects: 10_000,
		TotalQuotaBytes:       256 << 20,
		TotalQuotaObjects:     50_000,
	}); err != nil {
		t.Fatalf("configure attachments: %v", err)
	}
	f := newCoreToolsFixture(t, ctx, w)

	// The secretary uploads its own file — bytes in base64, filename is
	// display metadata only.
	payload := []byte("秘書のアップロードだよ")
	op := f.claim(t, ctx, MessagingCoreUploadTool, map[string]any{
		"place_id":       ch.PlaceID,
		"filename":       "報告.txt",
		"content_base64": base64.StdEncoding.EncodeToString(payload),
		"alt":            "報告書"})
	att, _ := op.Response["attachment"].(map[string]any)
	attID, _ := att["attachment_id"].(string)
	if attID == "" || op.Response["created"] != true {
		t.Fatalf("upload = %+v", op.Response)
	}
	if got := readBlob(t, blobs, attID); string(got) != string(payload) {
		t.Fatalf("stored blob = %q", got)
	}
	// Upload replay: one finalized object, the stored receipt.
	op = f.replay(t, ctx, 0, MessagingCoreUploadTool, map[string]any{
		"place_id":       ch.PlaceID,
		"filename":       "報告.txt",
		"content_base64": base64.StdEncoding.EncodeToString(payload),
		"alt":            "報告書"})
	if att2, _ := op.Response["attachment"].(map[string]any); att2["attachment_id"] != attID {
		t.Fatalf("replayed upload = %+v", op.Response)
	}
	var objects int64
	if err := w.store.core.pool.QueryRow(ctx,
		"SELECT COALESCE(SUM(object_count), 0) FROM message_attachment_quotas").Scan(&objects); err != nil {
		t.Fatalf("quota read: %v", err)
	}
	if objects != 1 {
		t.Fatalf("attachment objects = %d, want 1", objects)
	}

	// Bind it to a message the humans read — at urgent priority.
	op = f.claim(t, ctx, MessagingCoreTool, map[string]any{
		"place_id": ch.PlaceID, "content": "資料です", "urgency": "urgent",
		"attachments": []any{attID}})
	messageID, _ := op.Response["message_id"].(string)
	attachments, _ := op.Response["attachments"].([]any)
	if len(attachments) != 1 {
		t.Fatalf("send attachments = %+v", op.Response)
	}
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil || len(history) != 1 || len(history[0].Attachments) != 1 ||
		history[0].Urgency != UrgencyUrgent {
		t.Fatalf("human history = %+v %v", history, err)
	}

	// The secretary reads the bytes back under the exact place+message
	// binding; a mismatched message identity is refused.
	op = f.claim(t, ctx, MessagingCoreOpenAttachmentTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": messageID, "attachment_id": attID})
	got, err := base64.StdEncoding.DecodeString(op.Response["content_base64"].(string))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("open attachment = %v %v", got, err)
	}
	if err := f.claimError(t, ctx, MessagingCoreOpenAttachmentTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": "msg-not-real", "attachment_id": attID}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("mismatched message: got %v, want ErrBadRequest", err)
	}

	// The wake bug: a human posts a message that is only an attachment, and
	// the secretary's durable input must carry the metadata — not an empty
	// wake.
	humanScope := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanA)
	fx := attachmentFixture{world: w, root: root, blobs: blobs}
	uploaded := fx.mustUpload(t, ctx, humanScope, ch.PlaceID, "human-up-1", "図.png", "image/png", pngHeader)
	humanMsg, created, err := humanScope.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "", ClientNonce: "human-att-only",
		AttachmentIDs: []string{uploaded.AttachmentID},
	})
	if err != nil || !created {
		t.Fatalf("attachment-only send: created=%t err=%v", created, err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, f.delivery, 25)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if stats.Admitted == 0 {
		t.Fatalf("attachment-only message admitted no events")
	}
	var foundInput bool
	rows, err := w.store.core.pool.Query(ctx,
		"SELECT payload FROM core_inputs WHERE persona_id = $1", w.agent.ID)
	if err != nil {
		t.Fatalf("read inputs: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var p map[string]any
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("scan input: %v", err)
		}
		if p["message_id"] != humanMsg.MessageID {
			continue
		}
		foundInput = true
		if _, hasText := p["text"]; hasText {
			t.Fatalf("attachment-only input carried text: %+v", p)
		}
		atts, _ := p["attachments"].([]any)
		if len(atts) != 1 {
			t.Fatalf("input attachments = %+v", p)
		}
		a, _ := atts[0].(map[string]any)
		if a["attachment_id"] != uploaded.AttachmentID || a["filename"] != "図.png" ||
			a["mime"] != "image/png" {
			t.Fatalf("input attachment metadata = %+v", a)
		}
	}
	if !foundInput {
		t.Fatalf("attachment-only message produced no core input")
	}
	// And the secretary can read those bytes by the shown identity.
	op = f.claim(t, ctx, MessagingCoreOpenAttachmentTool, map[string]any{
		"place_id": ch.PlaceID, "message_id": humanMsg.MessageID,
		"attachment_id": uploaded.AttachmentID})
	got, err = base64.StdEncoding.DecodeString(op.Response["content_base64"].(string))
	if err != nil || !bytes.Equal(got, pngHeader) {
		t.Fatalf("secretary read of human's attachment = %v %v", got, err)
	}
}

// TestCoreToolsEndToEndCoreSurface runs the real Node secretary core against
// the real agentstate HTTP service and real PostgreSQL: each human message
// carries a mock-model directive, so the runtime claims the queued input,
// invokes the delegated Messaging tool over HTTP, and records the tool
// result — the same path a production model call takes.
func TestCoreToolsEndToEndCoreSurface(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not on PATH; skipping Node core e2e")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	grantManageChannels(t, ctx, w, ws.WorkspaceID, w.agent)
	w.send(t, ctx, ch.PlaceID, w.humanA, "開会は金曜")
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}

	delivery, coreStore := newSharedIntakeDelivery(t, w)
	for tool, effect := range delivery.CoreToolEffects() {
		if err := coreStore.RegisterEffect(tool, effect); err != nil {
			t.Fatalf("register %s: %v", tool, err)
		}
	}
	srv := agentstate.NewServer(w.store.core.pool, "core-tools-e2e-token-0123456789")
	for tool, effect := range delivery.CoreToolEffects() {
		if err := srv.RegisterToolEffect(tool, effect); err != nil {
			t.Fatalf("serve %s: %v", tool, err)
		}
	}
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	var ln net.Listener
	for port := 13421; port <= 13439; port++ {
		ln, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			break
		}
	}
	if ln == nil {
		t.Fatalf("no free port in 13421-13439: %v", err)
	}
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(ln) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	})
	baseURL := "http://" + ln.Addr().String()

	// Four inputs, one tool call each: open the channel, read overview,
	// create a channel, and change the secretary's own notification level.
	directives := []string{
		fmt.Sprintf(`!messaging.open {"place_id":%q}`, ch.PlaceID),
		fmt.Sprintf(`!messaging.overview {"workspace_id":%q}`, ws.WorkspaceID),
		fmt.Sprintf(`!messaging.create_channel {"workspace_id":%q,"name":"ノード側から"}`, ws.WorkspaceID),
		fmt.Sprintf(`!messaging.notification_settings {"workspace_id":%q,"defaults_level":"mute","keywords":["緊急"]}`, ws.WorkspaceID),
	}
	for _, directive := range directives {
		w.send(t, ctx, dm.PlaceID, w.humanA, directive)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if stats.Admitted != len(directives)+1 {
		// The channel message above also reaches the secretary as ambient
		// awareness; the four DM directives are the tool-call inputs.
		t.Fatalf("admitted %d events, want %d", stats.Admitted, len(directives)+1)
	}

	env := append(os.Environ(),
		"SUMI_STATE_URL="+baseURL,
		"SUMI_PERSONA_ID="+w.agent.ID,
		"SUMI_PERSONA_TOKEN="+srv.PersonaToken(w.agent.ID),
		"SUMI_MODEL_PROVIDER=mock",
		"SUMI_LEASE_TTL_MS=8000",
		"SUMI_ONCE_IDLE_MS=1500",
	)
	cmd := exec.CommandContext(ctx, nodePath,
		filepath.Join(coreRootDir(t), "src", "host", "local.ts"), "--once")
	cmd.Dir = coreRootDir(t)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("core --once failed: %v\n%s", err, out)
	}

	// Every directive produced a real tool_result with domain truth.
	evRows, err := w.store.core.pool.Query(ctx,
		`SELECT payload FROM core_events WHERE persona_id=$1 AND kind='tool_result' ORDER BY seq`, w.agent.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer evRows.Close()
	var results []map[string]any
	for evRows.Next() {
		var p map[string]any
		if err := evRows.Scan(&p); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, p)
	}
	if len(results) != len(directives) {
		t.Fatalf("tool_results = %d, want %d: %+v", len(results), len(directives), results)
	}
	openResp, _ := results[0]["response"].(map[string]any)
	if msgs, _ := openResp["messages"].([]any); len(msgs) != 1 {
		t.Fatalf("open result messages = %+v", openResp)
	}
	overviewResp, _ := results[1]["response"].(map[string]any)
	if chs, _ := overviewResp["channels"].([]any); len(chs) != 1 {
		t.Fatalf("overview channels = %+v", overviewResp)
	}
	createResp, _ := results[2]["response"].(map[string]any)
	created, _ := createResp["channel"].(map[string]any)
	newID, _ := created["channel_id"].(string)
	if newID == "" {
		t.Fatalf("create_channel result = %+v", createResp)
	}
	// The secretary-created channel is an ordinary place the human sees.
	if place, err := w.store.PlaceFor(ctx, newID, w.humanA); err != nil || place.Name != "ノード側から" {
		t.Fatalf("human view of node-created channel = %+v %v", place, err)
	}
	settingResp, _ := results[3]["response"].(map[string]any)
	setting, _ := settingResp["setting"].(map[string]any)
	defaults, _ := setting["defaults"].(map[string]any)
	if defaults["level"] != "mute" {
		t.Fatalf("notification result = %+v", setting)
	}
	// And the change landed in the domain store.
	scoped := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
	got, err := scoped.NotificationSettingFor(ctx)
	if err != nil || got.DefaultLevel != "mute" || len(got.Keywords) != 1 {
		t.Fatalf("stored setting = %+v %v", got, err)
	}
}

func TestCoreToolsMembershipAndEpochFences(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newCoreToolsWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	f := newCoreToolsFixture(t, ctx, w)

	// Sanity: the effect works before the fence drops.
	f.claim(t, ctx, MessagingCoreOverviewTool, map[string]any{"workspace_id": ws.WorkspaceID})

	// Removing the secretary's workspace membership revokes every effect.
	if err := w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, w.agent); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if err := f.claimError(t, ctx, MessagingCoreTool, map[string]any{
		"place_id": ch.PlaceID, "content": "まだ読めますか"}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("send after removal: got %v, want ErrBadRequest", err)
	}
	if err := f.claimError(t, ctx, MessagingCoreOpenTool, map[string]any{
		"place_id": ch.PlaceID}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("open after removal: got %v, want ErrBadRequest", err)
	}

	// Reinstalling Messaging moves the authority epoch — the effect resolves
	// the current installation, so a disabled installation fails fast and a
	// reinstalled one authorizes fresh effects.
	if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, w.agent, RoleMember); err != nil {
		t.Fatalf("re-add member: %v", err)
	}
	installationID := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent).Scope.InstallationID
	if err := w.apps.UninstallByID(ctx, installationID, w.humanA); err != nil {
		t.Fatalf("uninstall messaging: %v", err)
	}
	if err := f.claimError(t, ctx, MessagingCoreOverviewTool, map[string]any{
		"workspace_id": ws.WorkspaceID}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("overview after uninstall: got %v, want ErrBadRequest", err)
	}
}
