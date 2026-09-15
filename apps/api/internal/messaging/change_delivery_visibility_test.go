package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// Fixtures for the change-delivery slice own their database prefix
// (sumi_msg_edit_review_swe_) so they never share databases with another
// worker.
func newChangeDeliveryWorld(t *testing.T, ctx context.Context) world {
	t.Helper()
	return newWorldOnPool(t, ctx, createOwnedTestDB(t, "sumi_msg_edit_review_swe_"))
}

// changeDeliveryRow reads the state and frozen payload of the delivery row one
// change event left for one secretary.
func changeDeliveryRow(t *testing.T, ctx context.Context, w world, paID, messageID, change string) (admitted bool, suppressed bool, reason string, event AgentAttentionEvent) {
	t.Helper()
	var payload []byte
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT admitted_at IS NOT NULL, suppressed_at IS NOT NULL,
		       COALESCE(suppression_reason, ''), payload
		FROM agent_attention_deliveries
		WHERE personality_agent_id=$1 AND message_id=$2 AND COALESCE(payload->>'change','')=$3`,
		paID, messageID, change).Scan(&admitted, &suppressed, &reason, &payload); err != nil {
		t.Fatalf("change delivery row: %v", err)
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode stored event: %v", err)
	}
	return admitted, suppressed, reason, event
}

// f-shared-conversation-ui-202: an edit issued while the reply's parent still
// stood is an authorized correction the recipient already saw. If the parent
// is deleted before the event drains, the update must still arrive — the reply
// designation is what dies, not the delivery. What the secretary observes is a
// plain current-view edit at observe, naming no parent: the same thing an edit
// issued after the deletion would have said.
func TestEditIssuedOnLiveParentSurvivesParentDeletion(t *testing.T) {
	// The recipient's only claim on the reply was answering its question —
	// no independent notification reason stands behind the change event.
	t.Run("reply_only_recipient", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newChangeDeliveryWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("level: %v", err)
		}
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "race-only"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
			t.Fatalf("edit while parent live: %v", err)
		}
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent before delivery: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		admitted, suppressed, reason, stored := changeDeliveryRow(t, ctx, w, w.agent.ID, reply.MessageID, AttentionChangeEdited)
		if !admitted || suppressed || reason != "" {
			t.Fatalf("edit delivery = admitted:%v suppressed:%v reason:%q", admitted, suppressed, reason)
		}
		// The stored row keeps what was issued — the designation held then.
		if stored.ReplyToMessageID != question.MessageID {
			t.Fatalf("frozen event lost its reply designation: %+v", stored)
		}
		update := lastCoreInput(t, ctx, w, w.agent.ID, 2)
		if update.Attention != "observe" || update.Payload["reply_to_message_id"] != nil ||
			update.Payload["message_change"] != AttentionChangeEdited || update.Payload["text"] != "訂正後の回答" {
			t.Fatalf("delivered edit = %s %+v", update.Attention, update.Payload)
		}
	})

	// A private-DM reason stands on its own: the correction still asks for a
	// reply, it just no longer names a parent that cannot be answered.
	t.Run("independent_dm_reason", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newChangeDeliveryWorld(t, ctx)
		w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
		if err != nil {
			t.Fatalf("dm: %v", err)
		}
		pa := w.store.mustScopeForPlace(t, ctx, dm.PlaceID, w.agent)
		question := w.send(t, ctx, dm.PlaceID, w.agent, "DMの質問")
		replier := w.store.mustScopeForPlace(t, ctx, dm.PlaceID, w.humanA)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: dm.PlaceID, Content: "DMの回答", ReplyTo: question.MessageID, ClientNonce: "race-dm"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := replier.EditMessage(ctx, dm.PlaceID, reply.MessageID, "DMの訂正回答", reply.Revision); err != nil {
			t.Fatalf("edit while parent live: %v", err)
		}
		if _, err := pa.DeleteMessage(ctx, dm.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent before delivery: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		update := lastCoreInput(t, ctx, w, w.agent.ID, 2)
		if update.Attention != "reply" || update.Payload["reason"] != NotifyReasonDM ||
			update.Payload["reply_to_message_id"] != nil || update.Payload["message_change"] != AttentionChangeEdited {
			t.Fatalf("dm edit after parent deletion = %s %+v", update.Attention, update.Payload)
		}
	})

	// A mention the edit itself added is a current reason, not a stale one:
	// the update still asks for a reply through it.
	t.Run("mention_added_by_edit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newChangeDeliveryWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("level: %v", err)
		}
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "race-mention"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "@Kuro（Yohaku） 回答を見てください", reply.Revision); err != nil {
			t.Fatalf("edit while parent live: %v", err)
		}
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent before delivery: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		update := lastCoreInput(t, ctx, w, w.agent.ID, 2)
		if update.Attention != "reply" || update.Payload["event_kind"] != AgentAttentionMention ||
			update.Payload["reason"] != NotifyReasonMention || update.Payload["reply_to_message_id"] != nil {
			t.Fatalf("mention edit after parent deletion = %s %+v", update.Attention, update.Payload)
		}
	})
}

// f-shared-conversation-ui-203: the recorded reason described the view as it
// was delivered, not what this update asks. An edit that removed the mention
// must not keep the "mention" reason — and must not let it smuggle a reply
// request past a parent that no longer stands. The secretary observes a plain
// correction with no reason and no parent.
func TestEditDroppedMentionAndDeletedParentDeliversObserve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "@Kuro（Yohaku） 回答は42です", ReplyTo: question.MessageID, ClientNonce: "drop-mention"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if in := lastCoreInput(t, ctx, w, w.agent.ID, 1); in.Attention != "reply" || in.Payload["reason"] != NotifyReasonMention {
		t.Fatalf("mention reply = %s %+v", in.Attention, in.Payload)
	}
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "回答は42です", reply.Revision); err != nil {
		t.Fatalf("edit while parent live: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent before delivery: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	update := lastCoreInput(t, ctx, w, w.agent.ID, 2)
	if update.Attention != "observe" || update.Payload["reply_to_message_id"] != nil ||
		update.Payload["reason"] != nil || update.Payload["event_kind"] != AgentAttentionMessage {
		t.Fatalf("edit that dropped its mention = %s %+v", update.Attention, update.Payload)
	}
}

// The downgraded event is what Admit wrote, so a retry after the receipt was
// lost re-derives the same input and reconciles — it does not redeliver or
// conflict on the frozen designation that no longer applies.
func TestEditDowngradeReconcilesAfterLostReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "reconcile"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
		t.Fatalf("edit while parent live: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent before delivery: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	// The core appended the input; the delivery-row acknowledgement is lost.
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE agent_attention_deliveries SET admitted_at=NULL, admitted_command_id=NULL, available_at=now(), next_attempt_at=now(),
		admitted_command_seq=NULL WHERE message_id=$1 AND payload->>'change'='edited'`,
		reply.MessageID); err != nil {
		t.Fatalf("simulate lost receipt: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 2 {
		t.Fatalf("reconcile redelivered: %d inputs", n)
	}
}

// pausePrepareDelivery gates the admission hold so a test can interleave a
// source mutation between the first reconcile check and the locked admit.
type pausePrepareDelivery struct {
	inner   *CoreAttentionDelivery
	entered chan struct{}
	release chan struct{}
}

func (p *pausePrepareDelivery) Prepare(ctx context.Context, paID string) (func(), error) {
	close(p.entered)
	select {
	case <-p.release:
		return p.inner.Prepare(ctx, paID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pausePrepareDelivery) Lookup(ctx context.Context, key string, event AgentAttentionEvent) (AgentAttentionReceipt, bool, error) {
	return p.inner.Lookup(ctx, key, event)
}

func (p *pausePrepareDelivery) Admit(ctx context.Context, key string, event AgentAttentionEvent) (AgentAttentionReceipt, error) {
	return p.inner.Admit(ctx, key, event)
}

// The gap between the reconcile check and the admission is exactly where the
// race lives: the first pass authorized the designation, the parent dies
// while the runtime starts, and the locked second pass must retire it instead
// of admitting a stale reply request — or suppressing a lawful correction.
func TestEditDowngradesWhenParentDiesBetweenCheckAndAdmit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "midflight"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
		t.Fatalf("edit while parent live: %v", err)
	}
	gate := &pausePrepareDelivery{inner: delivery, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		stats, err := w.store.core.DeliverAgentAttention(ctx, gate, 25)
		if err == nil && stats.Admitted != 1 {
			err = fmt.Errorf("drain = %+v, want 1 admitted", stats)
		}
		done <- err
	}()
	<-gate.entered
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent mid-delivery: %v", err)
	}
	close(gate.release)
	if err := <-done; err != nil {
		t.Fatalf("drain: %v", err)
	}
	update := lastCoreInput(t, ctx, w, w.agent.ID, 2)
	if update.Attention != "observe" || update.Payload["reply_to_message_id"] != nil {
		t.Fatalf("mid-flight deleted parent edit = %s %+v", update.Attention, update.Payload)
	}
}

// The asymmetry is deliberate: an original reply whose only basis was the
// parent still suppresses when the parent dies before first delivery — the
// recipient never saw the message, so there is no view to update. Only the
// change event rides on an existing view.
func TestPendingOriginalReplyStillSuppressesWhenParentDeleted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "まだ届いていない回答", ReplyTo: question.MessageID, ClientNonce: "pending-reply"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent before first delivery: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 0, 1)
	admitted, suppressed, reason, _ := changeDeliveryRow(t, ctx, w, w.agent.ID, reply.MessageID, "")
	if admitted || !suppressed || reason != "source_unavailable" {
		t.Fatalf("original reply = admitted:%v suppressed:%v reason:%q", admitted, suppressed, reason)
	}
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 0 {
		t.Fatalf("suppressed reply reached the core: %d inputs", n)
	}
}

// Private-place authorization still holds: a recipient who left the group DM
// between issue and delivery gets nothing — no downgrade, no delivery.
func TestPendingReplyToDepartedGroupMemberGetsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	secondID, err := koseki.New(w.store.core.pool).MintSecretary(ctx, w.humanB.ID)
	if err != nil {
		t.Fatalf("mint second secretary: %v", err)
	}
	second := PersonalityAgent(secondID)
	registerTestStore(w.store, second)
	ws, _ := w.workspaceWithChannel(t, ctx)
	if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, second, RoleMember); err != nil {
		t.Fatalf("add second secretary: %v", err)
	}
	group, err := w.store.CreateGroupDM(ctx, w.humanA, []ParticipantRef{w.humanB, second})
	if err != nil {
		t.Fatalf("group dm: %v", err)
	}
	question := w.send(t, ctx, group.PlaceID, second, "在籍中の質問")
	replier := w.store.mustScopeForPlace(t, ctx, group.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: group.PlaceID, Content: "離脱前の回答", ReplyTo: question.MessageID, ClientNonce: "departed"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	if _, err := w.store.core.pool.Exec(ctx,
		"UPDATE place_members SET left_at=now() WHERE place_id=$1 AND member_id=$2",
		group.PlaceID, second.ID); err != nil {
		t.Fatalf("depart: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 0, 1)
	admitted, suppressed, reason, _ := changeDeliveryRow(t, ctx, w, second.ID, reply.MessageID, "")
	if admitted || !suppressed || reason != "source_unavailable" {
		t.Fatalf("departed member's reply = admitted:%v suppressed:%v reason:%q", admitted, suppressed, reason)
	}
	if n := len(coreInputsFor(t, ctx, w, second.ID)); n != 0 {
		t.Fatalf("departed member received %d inputs", n)
	}
}

// Provenance rules the designation does not override: a tombstone still names
// the deleted parent it reports, and a self-reply never addresses its own
// author. The mute gate keeps working at issue time.
func TestChangeDeliveryDesignationBoundaries(t *testing.T) {
	t.Run("tombstone_names_deleted_parent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newChangeDeliveryWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("level: %v", err)
		}
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "tomb-parent"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent: %v", err)
		}
		if _, err := replier.DeleteMessage(ctx, ch.PlaceID, reply.MessageID); err != nil {
			t.Fatalf("delete reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		tombstone := lastCoreInput(t, ctx, w, w.agent.ID, 2)
		if tombstone.Attention != "observe" || tombstone.Payload["message_change"] != AttentionChangeDeleted ||
			tombstone.Payload["reply_to_message_id"] != question.MessageID {
			t.Fatalf("tombstone = %s %+v", tombstone.Attention, tombstone.Payload)
		}
	})

	t.Run("self_reply_tombstone_names_no_parent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newChangeDeliveryWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "自分の質問")
		reply, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "自分への回答", ReplyTo: question.MessageID, ClientNonce: "self-reply"})
		if err != nil {
			t.Fatalf("self reply: %v", err)
		}
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, reply.MessageID); err != nil {
			t.Fatalf("delete self reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 0, 0)
		if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 0 {
			t.Fatalf("self-authored tombstone reached the author: %d inputs", n)
		}
	})

	// A mute applied after the original delivery is a current fact: the edit
	// still reaches the view it already established, at observe, with no
	// stale reason and no reply designation.
	t.Run("muted_after_delivery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newChangeDeliveryWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "muted-after"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMute, nil, nil); err != nil {
			t.Fatalf("mute: %v", err)
		}
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		update := lastCoreInput(t, ctx, w, w.agent.ID, 2)
		if update.Attention != "observe" || update.Payload["reply_to_message_id"] != nil ||
			update.Payload["reason"] != nil {
			t.Fatalf("edit to muted recipient = %s %+v", update.Attention, update.Payload)
		}
	})
}

// F1 closure: the edit event was admitted while the parent stood — with its
// reply designation — but the delivery-row acknowledgement was lost. After the
// parent is deleted, the retry derives the downgraded event, yet the stored
// input is the lawful designated variant. Reconciliation must match either
// lawful variant, commit the receipt, and never record a false input_conflict
// on the delivery's own earlier admission.
func TestDesignatedAdmitLostReceiptReconcilesAfterParentDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "f1-dl"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
		t.Fatalf("edit while parent live: %v", err)
	}
	// Parent still stands: the edit admits WITH its designation.
	drainAttention(t, ctx, w, delivery, 1, 0)
	inputs := coreInputsFor(t, ctx, w, w.agent.ID)
	if len(inputs) != 2 || inputs[1].Payload["reply_to_message_id"] != question.MessageID || inputs[1].Attention != "reply" {
		t.Fatalf("designated edit admission = %+v", inputs)
	}
	// Lose the delivery-row acknowledgement; then the parent dies.
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE agent_attention_deliveries SET admitted_at=NULL, admitted_command_id=NULL, available_at=now(), next_attempt_at=now(),
		admitted_command_seq=NULL WHERE message_id=$1 AND payload->>'change'='edited'`,
		reply.MessageID); err != nil {
		t.Fatalf("simulate lost receipt: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent after admission: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != 1 || stats.Suppressed != 0 || stats.Retried != 0 {
		t.Fatalf("reconcile drain = %+v %v, want admitted=1", stats, err)
	}
	// The receipt for the lawful earlier admission is committed — the row is
	// admitted, not falsely suppressed.
	var admitted, suppressed bool
	var suppressionReason, commandID string
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT admitted_at IS NOT NULL, suppressed_at IS NOT NULL,
		       COALESCE(suppression_reason,''), COALESCE(admitted_command_id::text,'')
		FROM agent_attention_deliveries
		WHERE message_id=$1 AND payload->>'change'='edited'`, reply.MessageID).
		Scan(&admitted, &suppressed, &suppressionReason, &commandID); err != nil {
		t.Fatalf("receipt row: %v", err)
	}
	if !admitted || suppressed || suppressionReason != "" || commandID == "" {
		t.Fatalf("receipt = admitted:%v suppressed:%v reason:%q command:%q — "+
			"the lawful designated admission must reconcile, not conflict",
			admitted, suppressed, suppressionReason, commandID)
	}
	// No duplicate admission: the input count is unchanged, and the delivered
	// designated input is left as the secretary saw it.
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 2 {
		t.Fatalf("inputs = %d, want the 2 already admitted", n)
	}
	// The recipient holds a live view of the corrected reply: a later edit and
	// the tombstone must still reach it.
	var rev int64
	if err := w.store.core.pool.QueryRow(ctx, `SELECT revision FROM messages WHERE message_id=$1`, reply.MessageID).Scan(&rev); err != nil {
		t.Fatalf("revision: %v", err)
	}
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正その2", rev); err != nil {
		t.Fatalf("second edit: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if _, err := replier.DeleteMessage(ctx, ch.PlaceID, reply.MessageID); err != nil {
		t.Fatalf("delete reply: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 4 {
		t.Fatalf("final inputs = %d, want 4 (original, edit1, edit2, tombstone)", n)
	}
}

// F1 boundary: the either-variant reconcile must not weaken the real
// input_conflict guard — a stored input under this event id carrying foreign
// content is still a terminal conflict, not an admission.
func TestLostReceiptForeignContentStillConflicts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "f1-fc"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正", reply.Revision); err != nil {
		t.Fatalf("edit: %v", err)
	}
	drainAttention(t, ctx, w, delivery, 1, 0)
	// Lose the receipt, corrupt the stored input under this event id, then the
	// parent dies — the retry must still see a foreign-content conflict.
	var eventID string
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT event_id::text FROM agent_attention_deliveries
		WHERE message_id=$1 AND payload->>'change'='edited'`, reply.MessageID).Scan(&eventID); err != nil {
		t.Fatalf("event id: %v", err)
	}
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE agent_attention_deliveries SET admitted_at=NULL, admitted_command_id=NULL, available_at=now(), next_attempt_at=now(),
		admitted_command_seq=NULL WHERE event_id=$1`, eventID); err != nil {
		t.Fatalf("lose receipt: %v", err)
	}
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE core_inputs SET payload='{"kind":"unrelated","text":"foreign"}'::jsonb
		WHERE input_id=$1`, "messaging:"+eventID); err != nil {
		t.Fatalf("corrupt input: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	dumpDeliveries(t, ctx, w, reply.MessageID)
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != 0 || stats.Suppressed != 1 || stats.Retried != 0 {
		t.Fatalf("conflict drain = %+v %v, want suppressed=1", stats, err)
	}
	var reason string
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT suppression_reason FROM agent_attention_deliveries WHERE event_id=$1`,
		eventID).Scan(&reason); err != nil {
		t.Fatalf("suppression: %v", err)
	}
	if reason != "input_conflict" {
		t.Fatalf("suppression_reason = %q, want input_conflict — foreign content must stay terminal", reason)
	}
}

// F2 closure: a pending original delivery row alone does not establish that
// the recipient saw the message. The change event waits on the in-flight
// original rather than deciding on a pending row, and resolves with its
// outcome — suppressed original retires the change with no_prior_view, an
// admitted one delivers the correction.
func TestChangeDeliveryWaitsOnPendingOriginal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	question := w.send(t, ctx, ch.PlaceID, w.agent, "質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "f2-wait"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後", reply.Revision); err != nil {
		t.Fatalf("edit before first drain: %v", err)
	}
	// Push the original's attempt into the future so the drain selects only
	// the edit: the edit must wait — retry, not admit, not suppress. The edit
	// row's own schedule is pinned against insert/drain clock skew.
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE agent_attention_deliveries SET next_attempt_at=now()+interval '1 hour'
		WHERE message_id=$1 AND payload->>'change' IS NULL`, reply.MessageID); err != nil {
		t.Fatalf("defer original: %v", err)
	}
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE agent_attention_deliveries SET available_at=now(), next_attempt_at=now()
		WHERE message_id=$1 AND payload->>'change'='edited'`, reply.MessageID); err != nil {
		t.Fatalf("schedule edit: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if stats.Retried != 1 || stats.Admitted != 0 || stats.Suppressed != 0 {
		t.Fatalf("wait drain = %+v err=%v, want retried=1 only", stats, err)
	}
	var editPending bool
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT admitted_at IS NULL AND suppressed_at IS NULL FROM agent_attention_deliveries
		WHERE message_id=$1 AND payload->>'change'='edited'`, reply.MessageID).Scan(&editPending); err != nil {
		t.Fatalf("edit row: %v", err)
	}
	if !editPending {
		t.Fatal("edit row must stay pending while the original is undecided")
	}
	// The original resolves — the change follows it as an ordinary update.
	if _, err := w.store.core.pool.Exec(ctx, `
		UPDATE agent_attention_deliveries SET next_attempt_at=now() WHERE message_id=$1`,
		reply.MessageID); err != nil {
		t.Fatalf("undefer: %v", err)
	}
	stats, err = w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != 2 || stats.Suppressed != 0 || stats.Retried != 0 {
		t.Fatalf("resolve drain = %+v %v, want admitted=2", stats, err)
	}
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 2 {
		t.Fatalf("inputs = %d, want original + correction", n)
	}
}

// F2 closure: the same drain resolves the original first — a reply-only
// recipient whose original admits sees the correction as an ordinary follow-up,
// no waiting needed.
func TestChangeFollowsSameDrainAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーへの質問2")
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "f2-same"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後", reply.Revision); err != nil {
		t.Fatalf("edit before drain: %v", err)
	}
	// One drain: the earlier original admits first, establishing the view the
	// correction then updates — order inside the batch makes the basis real.
	drainAttention(t, ctx, w, delivery, 2, 0)
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 2 {
		t.Fatalf("inputs = %d, want original + correction", n)
	}
}

// F2 closure: the original is suppressed before any admission and the edit
// names no current reason — there is no view to correct, so the change
// suppresses with no_prior_view and the secretary hears nothing.
func TestChangeToNeverViewedRecipientGetsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "f2-noview"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	// Edit lands while the original is still pending; then the parent dies.
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後", reply.Revision); err != nil {
		t.Fatalf("edit before first drain: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != 0 || stats.Suppressed != 2 || stats.Retried != 0 {
		t.Fatalf("drain = %+v %v, want suppressed=2", stats, err)
	}
	var reason string
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT suppression_reason FROM agent_attention_deliveries
		WHERE message_id=$1 AND payload->>'change'='edited'`, reply.MessageID).Scan(&reason); err != nil {
		t.Fatalf("edit suppression: %v", err)
	}
	if reason != "no_prior_view" {
		t.Fatalf("edit suppression_reason = %q, want no_prior_view", reason)
	}
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 0 {
		t.Fatalf("recipient with no established view received %d inputs, want 0", n)
	}
}

// F2 boundary: once the original is suppressed, a later edit issues no event
// at all for that recipient — the suppressed row earns no candidate.
func TestEditAfterSuppressedOriginalIssuesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newChangeDeliveryWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)
	if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
		t.Fatalf("level: %v", err)
	}
	pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
	question := w.send(t, ctx, ch.PlaceID, w.agent, "質問")
	replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "f2-post"})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	// The pending original suppresses — no view was ever established.
	drainAttention(t, ctx, w, delivery, 0, 1)
	if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後", reply.Revision); err != nil {
		t.Fatalf("edit: %v", err)
	}
	var rows int
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT count(*) FROM agent_attention_deliveries
		WHERE message_id=$1 AND payload->>'change'='edited'`, reply.MessageID).Scan(&rows); err != nil {
		t.Fatalf("edit rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("edit issued %d delivery rows to a recipient who never saw the message", rows)
	}
	drainAttention(t, ctx, w, delivery, 0, 0)
	if n := len(coreInputsFor(t, ctx, w, w.agent.ID)); n != 0 {
		t.Fatalf("inputs = %d, want 0", n)
	}
}

// dumpDeliveries logs every delivery row for one message so a failing run
// still records the observed suppression/admission state.
func dumpDeliveries(t *testing.T, ctx context.Context, w world, messageID string) {
	t.Helper()
	rows, err := w.store.core.pool.Query(ctx, `
		SELECT event_id, COALESCE(payload->>'change',''), admitted_at IS NOT NULL,
		       suppressed_at IS NOT NULL, COALESCE(suppression_reason,''),
		       COALESCE(payload->>'reply_to_message_id',''),
		       EXISTS (SELECT 1 FROM core_inputs ci WHERE ci.input_id='messaging:'||event_id::text)
		FROM agent_attention_deliveries
		WHERE message_id=$1 ORDER BY available_at, event_id`, messageID)
	if err != nil {
		t.Fatalf("delivery rows: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, change, reason, replyTo string
		var admitted, suppressed, inputExists bool
		if err := rows.Scan(&id, &change, &admitted, &suppressed, &reason, &replyTo, &inputExists); err != nil {
			t.Fatalf("scan delivery row: %v", err)
		}
		t.Logf("delivery %s change=%q admitted=%v suppressed=%v reason=%q reply_to=%q input=%v",
			id, change, admitted, suppressed, reason, replyTo, inputExists)
	}
}
