package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// Fixtures for the edit-reply visibility slice own their database prefix
// (sumi_msg_edit_opus_) so they never share databases with another worker.
func newEditReplyVisibilityWorld(t *testing.T, ctx context.Context) world {
	t.Helper()
	return newWorldOnPool(t, ctx, createOwnedTestDB(t, "sumi_msg_edit_opus_"))
}

func drainAttention(t *testing.T, ctx context.Context, w world, delivery AgentAttentionDelivery, admitted, suppressed int) {
	t.Helper()
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil || stats.Admitted != admitted || stats.Suppressed != suppressed || stats.Retried != 0 {
		t.Fatalf("drain = %+v %v, want admitted=%d suppressed=%d", stats, err, admitted, suppressed)
	}
}

func lastCoreInput(t *testing.T, ctx context.Context, w world, paID string, want int) agentstate.Input {
	t.Helper()
	inputs := coreInputsFor(t, ctx, w, paID)
	if len(inputs) != want {
		t.Fatalf("inputs = %d, want %d: %+v", len(inputs), want, inputs)
	}
	return inputs[want-1]
}

// assertEditIsNotAddressedReply checks the input a secretary actually receives
// for an edit: it reports the current content, but names no parent it answers.
func assertEditIsNotAddressedReply(t *testing.T, in agentstate.Input, reply Message, attention string) {
	t.Helper()
	if in.Payload["message_change"] != AttentionChangeEdited || in.Payload["message_id"] != reply.MessageID {
		t.Fatalf("edit input = %+v", in.Payload)
	}
	if _, ok := in.Payload["reply_to_message_id"]; ok {
		t.Fatalf("edit input names a parent this secretary cannot be answered on: %+v", in.Payload)
	}
	if in.Attention != attention {
		t.Fatalf("edit attention = %q, want %q (payload %+v)", in.Attention, attention, in.Payload)
	}
}

// f-call-participation-138 (text Messaging): a reply whose parent is deleted
// no longer answers anyone — an ordinary reply posted now would reach the
// parent's secretary at most through its own notification rules. Editing the
// reply's content must not turn that back into an addressed reply.
func TestEditOfReplyToDeletedParentIsNotAddressedReply(t *testing.T) {
	t.Run("deleted_before_reply_all_level", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent: %v", err)
		}
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "消えた質問への回答", ReplyTo: question.MessageID, ClientNonce: "del-before"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		original := lastCoreInput(t, ctx, w, w.agent.ID, 1)
		if original.Attention != "observe" || original.Payload["reply_to_message_id"] != nil {
			t.Fatalf("ordinary reply to deleted parent = %s %+v", original.Attention, original.Payload)
		}
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "消えた質問への訂正回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		assertEditIsNotAddressedReply(t, lastCoreInput(t, ctx, w, w.agent.ID, 2), reply, "observe")
	})

	t.Run("deleted_after_reply_all_level", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "del-after-all"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if in := lastCoreInput(t, ctx, w, w.agent.ID, 1); in.Attention != "reply" || in.Payload["reply_to_message_id"] != question.MessageID {
			t.Fatalf("reply while parent was live = %s %+v", in.Attention, in.Payload)
		}
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent: %v", err)
		}
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		assertEditIsNotAddressedReply(t, lastCoreInput(t, ctx, w, w.agent.ID, 2), reply, "observe")
	})

	// The secretary received this reply only because it answered its message.
	// It already saw the reply, so the edit keeps its view current — at observe,
	// not as a reply obligation, and not silently dropped either.
	t.Run("deleted_after_reply_only_recipient", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("level: %v", err)
		}
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "del-after-only"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent: %v", err)
		}
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		assertEditIsNotAddressedReply(t, lastCoreInput(t, ctx, w, w.agent.ID, 2), reply, "observe")
	})

	// Explicit mention policy survives: an edit that names the secretary asks
	// for its response through the mention, without claiming a vanished parent.
	t.Run("deleted_parent_mention_added_by_edit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		if _, err := w.store.SetNotificationSetting(ctx, w.agent, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("level: %v", err)
		}
		pa := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.agent)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		if _, err := pa.DeleteMessage(ctx, ch.PlaceID, question.MessageID); err != nil {
			t.Fatalf("delete parent: %v", err)
		}
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "del-mention"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 0, 0)
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "@Kuro 回答を見てください", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		in := lastCoreInput(t, ctx, w, w.agent.ID, 1)
		assertEditIsNotAddressedReply(t, in, reply, "reply")
		if in.Payload["event_kind"] != AgentAttentionMention || in.Payload["reason"] != NotifyReasonMention {
			t.Fatalf("mention added by edit lost its reason: %+v", in.Payload)
		}
	})
}

// A parent that precedes the secretary's current place tenure is outside its
// visible history: an ordinary reply to it is not addressed to the secretary,
// and an edit of that reply must not name it either. Channels and threads keep
// full history (visible_from_seq 1); group-DM re-admission is the schema's
// documented boundary, so the tenure is set as that re-admission records it.
func TestEditOfReplyToParentBeforeTenureIsNotAddressedReply(t *testing.T) {
	setBoundary := func(t *testing.T, ctx context.Context, w world, placeID string, seq int64) {
		t.Helper()
		tag, err := w.store.core.pool.Exec(ctx, `UPDATE place_members SET visible_from_seq=$1
			WHERE place_id=$2 AND member_kind='personality_agent' AND member_id=$3 AND left_at IS NULL`,
			seq, placeID, w.agent.ID)
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("set tenure boundary: rows=%d %v", tag.RowsAffected(), err)
		}
	}

	// DM reason keeps reply attention; only the unseen parent must not be named.
	t.Run("group_dm_readmission", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		group, err := w.store.CreateGroupDM(ctx, w.humanA, []ParticipantRef{w.humanB, w.agent})
		if err != nil {
			t.Fatalf("group dm: %v", err)
		}
		question := w.send(t, ctx, group.PlaceID, w.agent, "前の在籍期間の質問")
		boundary := w.send(t, ctx, group.PlaceID, w.humanA, "再参加後の最初の発言")
		setBoundary(t, ctx, w, group.PlaceID, boundary.Seq)
		replier := w.store.mustScopeForPlace(t, ctx, group.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: group.PlaceID, Content: "昔の質問への回答", ReplyTo: question.MessageID, ClientNonce: "tenure-group"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 2, 0)
		if in := lastCoreInput(t, ctx, w, w.agent.ID, 2); in.Payload["reply_to_message_id"] != nil {
			t.Fatalf("ordinary reply to pre-tenure parent names it: %+v", in.Payload)
		}
		if _, err := replier.EditMessage(ctx, group.PlaceID, reply.MessageID, "昔の質問への訂正回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		in := lastCoreInput(t, ctx, w, w.agent.ID, 3)
		assertEditIsNotAddressedReply(t, in, reply, "reply")
		if in.Payload["reason"] != NotifyReasonDM {
			t.Fatalf("group DM reason lost: %+v", in.Payload)
		}
	})
}

// Controls: where an ordinary reply is addressed, its edit stays addressed.
func TestEditOfReplyAddressesOnlyWhereOrdinaryReplyWould(t *testing.T) {
	t.Run("visible_live_parent_all_level", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		_, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		question := w.send(t, ctx, ch.PlaceID, w.agent, "セクレタリーの質問")
		replier := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: ch.PlaceID, Content: "回答します", ReplyTo: question.MessageID, ClientNonce: "visible-all"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := replier.EditMessage(ctx, ch.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		in := lastCoreInput(t, ctx, w, w.agent.ID, 2)
		if in.Payload["message_change"] != AttentionChangeEdited || in.Payload["reply_to_message_id"] != question.MessageID ||
			in.Attention != "reply" || in.Payload["reason"] != NotifyReasonAll {
			t.Fatalf("edit of addressed reply = %s %+v", in.Attention, in.Payload)
		}
	})

	// An ordinary reply does not re-enroll a secretary that left the thread.
	// The secretary saw the reply while present, so its view of the edit is
	// still updated — without the reply obligation of a present participant.
	t.Run("former_thread_participant", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newEditReplyVisibilityWorld(t, ctx)
		ws, ch := w.workspaceWithChannel(t, ctx)
		delivery, _ := newSharedIntakeDelivery(t, w)
		pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
		if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMentions, nil, nil); err != nil {
			t.Fatalf("level: %v", err)
		}
		thread, _, err := pa.CreateThread(ctx, ch.PlaceID, "相談", "", "edit-thread")
		if err != nil {
			t.Fatalf("thread: %v", err)
		}
		question, _, err := pa.AppendMessage(ctx, AppendInput{PlaceID: thread.Place.PlaceID, Content: "質問", ClientNonce: "question"})
		if err != nil {
			t.Fatalf("question: %v", err)
		}
		replier := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
		reply, _, err := replier.AppendMessage(ctx, AppendInput{PlaceID: thread.Place.PlaceID, Content: "回答", ReplyTo: question.MessageID, ClientNonce: "answer"})
		if err != nil {
			t.Fatalf("reply: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		if _, err := w.store.core.pool.Exec(ctx, "UPDATE place_members SET left_at=now() WHERE place_id=$1 AND member_id=$2", thread.Place.PlaceID, w.agent.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := replier.EditMessage(ctx, thread.Place.PlaceID, reply.MessageID, "訂正後の回答", reply.Revision); err != nil {
			t.Fatalf("edit: %v", err)
		}
		drainAttention(t, ctx, w, delivery, 1, 0)
		assertEditIsNotAddressedReply(t, lastCoreInput(t, ctx, w, w.agent.ID, 2), reply, "observe")
	})
}
