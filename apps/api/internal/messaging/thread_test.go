package messaging

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestThreadIsAPlaceUnderItsChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	origin := w.send(t, ctx, ch.PlaceID, w.humanA, "この件は長くなりそう")

	thread, err := w.store.CreateThread(ctx, ch.PlaceID, "リダイレクトの件", origin.MessageID, w.humanA)
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	if thread.Place.Kind != PlaceThread || thread.ParentPlaceID != ch.PlaceID ||
		thread.ParentMessageID != origin.MessageID {
		t.Fatalf("thread identity = %+v", thread)
	}

	// 親チャンネルのメンバーは、参加していなくても閲覧・投稿できる。
	if _, err := w.store.PlaceFor(ctx, thread.Place.PlaceID, w.humanB); err != nil {
		t.Fatalf("parent member cannot see the thread: %v", err)
	}
	reply := w.send(t, ctx, thread.Place.PlaceID, w.humanB, "こちらで続けます")
	if reply.Seq != 1 {
		t.Fatalf("thread seq starts at its own place: got %d", reply.Seq)
	}

	// 投稿したことで参加者になる（未読と通知の対象）。
	threads, err := w.store.ThreadsIn(ctx, ch.PlaceID, w.humanB)
	if err != nil {
		t.Fatalf("list threads: %v", err)
	}
	if len(threads) != 1 || threads[0].MessageCount != 1 ||
		threads[0].LastMessagePreview != "こちらで続けます" {
		t.Fatalf("thread summary = %+v", threads)
	}
	joined := map[string]bool{}
	for _, participant := range threads[0].Participants {
		joined[participant.Key()] = true
	}
	if !joined[w.humanA.Key()] || !joined[w.humanB.Key()] {
		t.Fatalf("participants = %+v", threads[0].Participants)
	}

	// 参加者にはbootstrap相当の一覧に出る。参加していない人には出ない。
	mine, err := w.store.ThreadsFor(ctx, w.humanB)
	if err != nil {
		t.Fatalf("threads for participant: %v", err)
	}
	if len(mine) != 1 {
		t.Fatalf("participant should see their thread: %+v", mine)
	}
	none, err := w.store.ThreadsFor(ctx, w.agent)
	if err != nil {
		t.Fatalf("threads for non-participant: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("non-participant should not carry the thread: %+v", none)
	}
}

func TestOneThreadPerOriginAndChannelsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	origin := w.send(t, ctx, ch.PlaceID, w.humanA, "起点")

	if _, err := w.store.CreateThread(ctx, ch.PlaceID, "一本目", origin.MessageID, w.humanA); err != nil {
		t.Fatalf("first thread: %v", err)
	}
	_, err := w.store.CreateThread(ctx, ch.PlaceID, "二本目", origin.MessageID, w.humanB)
	if !errors.Is(err, ErrThreadExists) {
		t.Fatalf("second thread on the same origin = %v, want ErrThreadExists", err)
	}

	// DMの中に脇道は作らない（v0: 親はチャンネルだけ）。
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.humanB)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	if _, err := w.store.CreateThread(ctx, dm.PlaceID, "だめ", "", w.humanA); !errors.Is(err, ErrNotThreadable) {
		t.Fatalf("thread in a dm = %v, want ErrNotThreadable", err)
	}

	// 見えない人にはスレッドも場所の存在も明かさない。
	outsider := w.store
	if _, err := outsider.ThreadsIn(ctx, ch.PlaceID, PersonalityAgent(newUUIDv7())); err == nil {
		t.Fatal("threads of an invisible channel must not be listed")
	}
}

func TestThreadNotifiesItsParticipantsNotTheWholeChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	thread, err := w.store.CreateThread(ctx, ch.PlaceID, "脇道", "", w.humanA)
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	place, err := w.store.PlaceFor(ctx, thread.Place.PlaceID, w.humanA)
	if err != nil {
		t.Fatalf("load thread place: %v", err)
	}

	// humanA（作成者）だけが参加者。humanB はまだ呼ばれない。
	msg := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "始めます")
	decisions, err := w.store.NotificationDecisionsFor(ctx, place, msg)
	if err != nil {
		t.Fatalf("decisions: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("a tangent must not ring the whole channel: %+v", decisions)
	}

	// 名前を呼べば届く。チャンネルの外の理由ではなくmentionとして。
	named := w.send(t, ctx, thread.Place.PlaceID, w.humanA, "@Haru 見てもらえますか")
	decisions, err = w.store.NotificationDecisionsFor(ctx, place, named)
	if err != nil {
		t.Fatalf("decisions for mention: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Participant != w.humanB ||
		decisions[0].Reason != NotifyReasonMention {
		t.Fatalf("mention in a thread = %+v", decisions)
	}
}
