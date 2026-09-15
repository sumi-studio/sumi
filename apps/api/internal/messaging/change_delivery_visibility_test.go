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
		UPDATE agent_attention_deliveries SET admitted_at=NULL, admitted_command_id=NULL,
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
