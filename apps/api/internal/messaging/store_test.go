package messaging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// world is one isolated database with a human founder, a second human, and a
// personality agent — the minimum cast for membership, mention, and
// authorization tests. Fixtures are synthetic and disposable (dev reset rule:
// never reuse a PersonalityAgentId across lives).
type world struct {
	store  *Store
	humanA ParticipantRef
	humanB ParticipantRef
	agent  ParticipantRef
}

func newWorld(t *testing.T, ctx context.Context) world {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := koseki.New(pool)
	humanA, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human A: %v", err)
	}
	humanB, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human B: %v", err)
	}
	agent, err := registry.MintSecretary(ctx, humanA)
	if err != nil {
		t.Fatalf("mint agent: %v", err)
	}
	for id, name := range map[string]string{humanA: "Yohaku", humanB: "Haru"} {
		if _, err := pool.Exec(ctx, "UPDATE humans SET display_name = $1 WHERE human_id = $2", name, id); err != nil {
			t.Fatalf("name human: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE agents SET display_name = 'Kuro' WHERE personality_agent_id = $1", agent); err != nil {
		t.Fatalf("name agent: %v", err)
	}
	return world{
		store:  New(pool),
		humanA: Human(humanA),
		humanB: Human(humanB),
		agent:  PersonalityAgent(agent),
	}
}

// workspaceWithChannel enrolls everyone and returns a channel.
func (w world) workspaceWithChannel(t *testing.T, ctx context.Context) (Workspace, Place) {
	t.Helper()
	ws, err := w.store.CreateWorkspace(ctx, "sumi-dev", w.humanA)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, m := range []ParticipantRef{w.humanB, w.agent} {
		if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, m, RoleMember); err != nil {
			t.Fatalf("add member %s: %v", m.Key(), err)
		}
	}
	ch, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "general", "日々のこと", w.humanA)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	return ws, ch
}

func (w world) send(t *testing.T, ctx context.Context, placeID string, author ParticipantRef, content string) Message {
	t.Helper()
	msg, created, err := w.store.AppendMessage(ctx, AppendInput{
		PlaceID: placeID, Author: author, Content: content,
		ClientNonce: fmt.Sprintf("nonce-%s-%d", author.Key(), time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("send %q: %v", content, err)
	}
	if !created {
		t.Fatalf("send %q: expected a fresh message", content)
	}
	return msg
}

func TestMemberProfilesQualifyCanonicalSumiByStableHuman(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	if _, err := w.store.pool.Exec(ctx,
		"UPDATE agents SET display_name='Sumi' WHERE personality_agent_id=$1", w.agent.ID); err != nil {
		t.Fatal(err)
	}
	secondAgentID, err := koseki.New(w.store.pool).MintSecretary(ctx, w.humanB.ID)
	if err != nil {
		t.Fatalf("mint second Secretary: %v", err)
	}
	workspace, _ := w.workspaceWithChannel(t, ctx)
	second := PersonalityAgent(secondAgentID)
	if err := w.store.AddWorkspaceMember(ctx, workspace.WorkspaceID, second, RoleMember); err != nil {
		t.Fatalf("add second Secretary: %v", err)
	}
	profiles, err := w.store.WorkspaceMemberProfiles(ctx, workspace.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, profile := range profiles {
		if profile.Participant.Kind == KindPersonalityAgent {
			names[profile.Participant.ID] = profile.ProjectedDisplayName()
		}
	}
	if names[w.agent.ID] != "Sumi（Yohaku）" || names[secondAgentID] != "Sumi（Haru）" {
		t.Fatalf("Secretary labels = %#v", names)
	}
}

func TestChannelPostingFollowsWorkspaceMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)

	// Humans and agents post through the identical path.
	first := w.send(t, ctx, ch.PlaceID, w.humanA, "おはよう")
	second := w.send(t, ctx, ch.PlaceID, w.agent, "おはようございます")
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("seq must be dense from 1: got %d then %d", first.Seq, second.Seq)
	}

	// A non-member cannot see the place, let alone post — and is not told the
	// place exists.
	stranger, err := koseki.New(w.store.pool).MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint stranger: %v", err)
	}
	_, _, err = w.store.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Author: Human(stranger), Content: "入れて", ClientNonce: "n1",
	})
	if !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("stranger post: got %v, want ErrPlaceNotFound", err)
	}
	if _, err := w.store.PlaceFor(ctx, ch.PlaceID, Human(stranger)); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("stranger view: got %v, want ErrPlaceNotFound", err)
	}
	if _, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "secret", "", Human(stranger)); !errors.Is(err, ErrNotAMember) {
		t.Fatalf("stranger create channel: got %v, want ErrNotAMember", err)
	}

	// Leaving closes access but not history.
	if err := w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, w.humanB); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	if _, err := w.store.PlaceFor(ctx, ch.PlaceID, w.humanB); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("left member view: got %v, want ErrPlaceNotFound", err)
	}
}

func TestAppendIsIdempotentOnClientNonce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	in := AppendInput{
		PlaceID: ch.PlaceID, Author: w.humanA, Content: "一度だけ", ClientNonce: "retry-me",
	}
	first, created, err := w.store.AppendMessage(ctx, in)
	if err != nil || !created {
		t.Fatalf("first send: created=%v err=%v", created, err)
	}
	again, created, err := w.store.AppendMessage(ctx, in)
	if err != nil {
		t.Fatalf("retry send: %v", err)
	}
	if created {
		t.Fatal("retry must not create a second message")
	}
	if again.MessageID != first.MessageID || again.Seq != first.Seq {
		t.Fatalf("retry returned a different message: %+v vs %+v", again, first)
	}
	place, err := w.store.PlaceFor(ctx, ch.PlaceID, w.humanA)
	if err != nil {
		t.Fatalf("reload place: %v", err)
	}
	if place.LastSeq != 1 {
		t.Fatalf("last_seq must stay 1 after a retried send, got %d", place.LastSeq)
	}
}

func waitForAdvisoryLocks(
	t *testing.T, ctx context.Context, store *Store, key string, granted bool, want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := store.pool.QueryRow(ctx,
			`WITH key AS (SELECT hashtextextended($1, 0) AS value)
			 SELECT count(*)
			 FROM pg_locks, key
			 WHERE locktype = 'advisory' AND granted = $2
			   AND classid::bigint = ((value >> 32) & 4294967295)
			   AND objid::bigint = (value & 4294967295)`, key, granted).Scan(&count)
		if err != nil {
			t.Fatalf("inspect advisory waiters: %v", err)
		}
		if count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("advisory locks for %q (granted=%v) did not reach %d", key, granted, want)
}

func assertDefaultMembershipTombstoned(
	t *testing.T, ctx context.Context, store *Store, participant ParticipantRef,
) {
	t.Helper()
	var active, historical int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE left_at IS NULL),
		        count(*) FILTER (WHERE left_at IS NOT NULL)
		 FROM workspace_members
		 WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3`,
		DefaultWorkspaceID, participant.Kind, participant.ID).Scan(&active, &historical); err != nil {
		t.Fatalf("inspect default membership for %s: %v", participant.Key(), err)
	}
	if active != 0 || historical != 1 {
		t.Fatalf("default membership for %s = active %d historical %d, want 0/1",
			participant.Key(), active, historical)
	}
}

func TestEnsureDefaultWorkspaceMembershipPreservesExplicitRemoval(t *testing.T) {
	t.Run("direct Human and PersonalityAgent calls", func(t *testing.T) {
		for _, kind := range []ParticipantKind{KindHuman, KindPersonalityAgent} {
			t.Run(string(kind), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				w := newWorld(t, ctx)
				participant := w.agent
				if kind == KindHuman {
					participant = w.humanA
				}
				if err := w.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
					t.Fatalf("initial default admission: %v", err)
				}
				if err := w.store.RemoveWorkspaceMember(ctx, DefaultWorkspaceID, participant); err != nil {
					t.Fatalf("remove default member: %v", err)
				}
				if err := w.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
					t.Fatalf("ensure after removal: %v", err)
				}
				assertDefaultMembershipTombstoned(t, ctx, w.store, participant)
			})
		}
	})

	t.Run("Human admission does not resurrect an owned PersonalityAgent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
			t.Fatalf("initial Human admission: %v", err)
		}
		if err := w.store.RemoveWorkspaceMember(ctx, DefaultWorkspaceID, w.agent); err != nil {
			t.Fatalf("remove owned PersonalityAgent: %v", err)
		}
		if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
			t.Fatalf("ensure Human after agent removal: %v", err)
		}
		assertDefaultMembershipTombstoned(t, ctx, w.store, w.agent)
	})
}

func TestEnsureDefaultWorkspaceMembershipAndRemovalCommitInLockOrder(t *testing.T) {
	for _, kind := range []ParticipantKind{KindHuman, KindPersonalityAgent} {
		for _, removalFirst := range []bool{true, false} {
			order := "ensure first"
			if removalFirst {
				order = "removal first"
			}
			t.Run(string(kind)+"/"+order, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				w := newWorld(t, ctx)
				participant := w.agent
				if kind == KindHuman {
					participant = w.humanA
				}
				if err := w.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
					t.Fatalf("initial default admission: %v", err)
				}

				blocker, err := w.store.pool.Begin(ctx)
				if err != nil {
					t.Fatalf("begin blocker: %v", err)
				}
				defer func() { _ = blocker.Rollback(ctx) }()
				if err := lockWorkspaceMembershipScope(ctx, blocker, DefaultWorkspaceID); err != nil {
					t.Fatalf("lock blocker: %v", err)
				}

				removeDone := make(chan error, 1)
				ensureDone := make(chan error, 1)
				first := func() { ensureDone <- w.store.EnsureDefaultWorkspaceMembership(ctx, participant) }
				second := func() {
					removeDone <- w.store.RemoveWorkspaceMember(ctx, DefaultWorkspaceID, participant)
				}
				if removalFirst {
					first, second = second, first
				}
				go first()
				key := workspaceMembershipScopeKey(DefaultWorkspaceID)
				waitForAdvisoryLocks(t, ctx, w.store, key, false, 1)
				go second()
				waitForAdvisoryLocks(t, ctx, w.store, key, false, 2)
				if err := blocker.Commit(ctx); err != nil {
					t.Fatalf("release blocker: %v", err)
				}
				if err := <-ensureDone; err != nil {
					t.Fatalf("ensure default membership: %v", err)
				}
				if err := <-removeDone; err != nil {
					t.Fatalf("remove default membership: %v", err)
				}
				assertDefaultMembershipTombstoned(t, ctx, w.store, participant)
			})
		}
	}
}

func TestAppendAndMembershipRemovalCommitInLockOrder(t *testing.T) {
	t.Run("removal first fences the send", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		ws, ch := w.workspaceWithChannel(t, ctx)

		blocker, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin blocker: %v", err)
		}
		defer func() { _ = blocker.Rollback(ctx) }()
		if err := lockWorkspaceMembershipScope(ctx, blocker, ws.WorkspaceID); err != nil {
			t.Fatalf("lock blocker: %v", err)
		}

		removeDone := make(chan error, 1)
		go func() { removeDone <- w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, w.humanA) }()
		key := workspaceMembershipScopeKey(ws.WorkspaceID)
		waitForAdvisoryLocks(t, ctx, w.store, key, false, 1)

		type appendResult struct {
			created bool
			err     error
		}
		appendDone := make(chan appendResult, 1)
		go func() {
			_, created, appendErr := w.store.AppendMessage(ctx, AppendInput{
				PlaceID: ch.PlaceID, Author: w.humanA, Content: "race",
				ClientNonce: "removal-first",
			})
			appendDone <- appendResult{created: created, err: appendErr}
		}()
		waitForAdvisoryLocks(t, ctx, w.store, key, false, 2)
		if err := blocker.Commit(ctx); err != nil {
			t.Fatalf("release blocker: %v", err)
		}
		if err := <-removeDone; err != nil {
			t.Fatalf("remove member: %v", err)
		}
		result := <-appendDone
		if !errors.Is(result.err, ErrPlaceNotFound) || result.created {
			t.Fatalf("post-removal append = created %v err %v, want fenced", result.created, result.err)
		}
		var messages int
		if err := w.store.pool.QueryRow(ctx,
			"SELECT count(*) FROM messages WHERE place_id = $1", ch.PlaceID).Scan(&messages); err != nil {
			t.Fatalf("count messages: %v", err)
		}
		if messages != 0 {
			t.Fatalf("messages = %d, revoked author committed content", messages)
		}
	})

	t.Run("send first commits its coherent pre-removal snapshot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		ws, ch := w.workspaceWithChannel(t, ctx)

		blocker, err := w.store.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin blocker: %v", err)
		}
		defer func() { _ = blocker.Rollback(ctx) }()
		if err := lockWorkspaceMembershipScope(ctx, blocker, ws.WorkspaceID); err != nil {
			t.Fatalf("lock blocker: %v", err)
		}

		type appendResult struct {
			message Message
			created bool
			err     error
		}
		appendDone := make(chan appendResult, 1)
		go func() {
			message, created, appendErr := w.store.AppendMessage(ctx, AppendInput{
				PlaceID: ch.PlaceID, Author: w.humanA, Content: "ordered before removal",
				ClientNonce: "send-first",
			})
			appendDone <- appendResult{message: message, created: created, err: appendErr}
		}()
		key := workspaceMembershipScopeKey(ws.WorkspaceID)
		waitForAdvisoryLocks(t, ctx, w.store, key, false, 1)

		removeDone := make(chan error, 1)
		go func() { removeDone <- w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, w.humanA) }()
		waitForAdvisoryLocks(t, ctx, w.store, key, false, 2)
		if err := blocker.Commit(ctx); err != nil {
			t.Fatalf("release blocker: %v", err)
		}
		result := <-appendDone
		if result.err != nil || !result.created {
			t.Fatalf("pre-removal append = created %v err %v", result.created, result.err)
		}
		if err := <-removeDone; err != nil {
			t.Fatalf("remove member: %v", err)
		}
		intents, err := w.store.NotificationIntentsForMessage(ctx, result.message.MessageID)
		if err != nil {
			t.Fatalf("load admission intents: %v", err)
		}
		if len(intents) != 2 {
			t.Fatalf("admission intents = %+v, want the two pre-removal recipients", intents)
		}
	})
}

func TestChannelAdmissionsShareScopeWhileRemovalWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, first := w.workspaceWithChannel(t, ctx)
	second, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "second", "", w.humanA)
	if err != nil {
		t.Fatalf("create second channel: %v", err)
	}

	// Hold each append at its per-place seq update, after it has acquired the
	// shared workspace admission lock. Different channel rows let both sends
	// reach that point concurrently.
	firstBlocker, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first place blocker: %v", err)
	}
	defer func() { _ = firstBlocker.Rollback(ctx) }()
	if _, err := firstBlocker.Exec(ctx,
		"UPDATE places SET last_seq = last_seq WHERE place_id = $1", first.PlaceID); err != nil {
		t.Fatalf("block first place: %v", err)
	}
	secondBlocker, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin second place blocker: %v", err)
	}
	defer func() { _ = secondBlocker.Rollback(ctx) }()
	if _, err := secondBlocker.Exec(ctx,
		"UPDATE places SET last_seq = last_seq WHERE place_id = $1", second.PlaceID); err != nil {
		t.Fatalf("block second place: %v", err)
	}

	type appendResult struct {
		created bool
		err     error
	}
	appendDone := make(chan appendResult, 2)
	for i, placeID := range []string{first.PlaceID, second.PlaceID} {
		go func(index int, target string) {
			_, created, appendErr := w.store.AppendMessage(ctx, AppendInput{
				PlaceID: target, Author: w.humanA, Content: "parallel admission",
				ClientNonce: fmt.Sprintf("parallel-%d", index),
			})
			appendDone <- appendResult{created: created, err: appendErr}
		}(i, placeID)
	}
	key := workspaceMembershipScopeKey(ws.WorkspaceID)
	waitForAdvisoryLocks(t, ctx, w.store, key, true, 2)

	removeDone := make(chan error, 1)
	go func() { removeDone <- w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, w.humanA) }()
	waitForAdvisoryLocks(t, ctx, w.store, key, false, 1)
	if err := firstBlocker.Commit(ctx); err != nil {
		t.Fatalf("release first place: %v", err)
	}
	if err := secondBlocker.Commit(ctx); err != nil {
		t.Fatalf("release second place: %v", err)
	}
	for range 2 {
		result := <-appendDone
		if result.err != nil || !result.created {
			t.Fatalf("parallel append = created %v err %v", result.created, result.err)
		}
	}
	if err := <-removeDone; err != nil {
		t.Fatalf("remove after shared admissions: %v", err)
	}
}

func TestMentionsBindAtAdmissionFromActiveMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)

	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） デプロイの様子どう？ @部外者 は無視")
	if len(msg.Mentions) != 1 || msg.Mentions[0] != w.agent {
		t.Fatalf("mentions = %+v, want exactly the agent", msg.Mentions)
	}

	// A member who left no longer binds: admission-time membership decides.
	if err := w.store.RemoveWorkspaceMember(ctx, ws.WorkspaceID, w.agent); err != nil {
		t.Fatalf("remove agent: %v", err)
	}
	after := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） もういないはず")
	if len(after.Mentions) != 0 {
		t.Fatalf("left member must not bind, got %+v", after.Mentions)
	}
}

func TestDMReachabilityAndPairUniqueness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	dm, created, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	if !created {
		t.Fatalf("first ensure must create the dm")
	}
	same, createdAgain, err := w.store.EnsureDM(ctx, w.agent, w.humanA)
	if err != nil {
		t.Fatalf("ensure dm again: %v", err)
	}
	if same.PlaceID != dm.PlaceID {
		t.Fatalf("a pair must have exactly one dm: %s vs %s", same.PlaceID, dm.PlaceID)
	}
	if createdAgain {
		t.Fatalf("second ensure must reuse the dm, not create one")
	}

	// No shared workspace, no reachability (v0 basis; Connection domain will
	// widen this).
	stranger, err := koseki.New(w.store.pool).MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint stranger: %v", err)
	}
	if _, _, err := w.store.EnsureDM(ctx, w.humanA, Human(stranger)); !errors.Is(err, ErrNotReachable) {
		t.Fatalf("unreachable dm: got %v, want ErrNotReachable", err)
	}

	// Group dm needs three distinct participants.
	if _, err := w.store.CreateGroupDM(ctx, w.humanA, []ParticipantRef{w.agent}); err == nil {
		t.Fatal("group dm with two participants must fail")
	}
	group, err := w.store.CreateGroupDM(ctx, w.humanA, []ParticipantRef{w.agent, w.humanB})
	if err != nil {
		t.Fatalf("create group dm: %v", err)
	}
	members, err := w.store.ActiveMembers(ctx, group.PlaceID, w.humanB)
	if err != nil {
		t.Fatalf("group members: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("group dm members = %d, want 3", len(members))
	}
}

func TestReadThroughIsMonotonicAndDrivesUnread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	w.send(t, ctx, ch.PlaceID, w.humanB, "一件目")           // seq 1
	w.send(t, ctx, ch.PlaceID, w.humanB, "二件目")           // seq 2
	w.send(t, ctx, ch.PlaceID, w.humanA, "自分の発言")         // seq 3
	w.send(t, ctx, ch.PlaceID, w.humanB, "@Yohaku 見てほしい") // seq 4

	summaries := func() UnreadSummary {
		t.Helper()
		all, err := w.store.UnreadSummaries(ctx, w.humanA)
		if err != nil {
			t.Fatalf("unread summaries: %v", err)
		}
		for _, s := range all {
			if s.Place.PlaceID == ch.PlaceID {
				return s
			}
		}
		t.Fatalf("channel missing from summaries: %+v", all)
		return UnreadSummary{}
	}

	sum := summaries()
	// Own message (seq 3) never counts as unread.
	if sum.UnreadCount != 3 || sum.MentionCount != 1 || sum.Place.LastSeq != 4 {
		t.Fatalf("before read: %+v", sum)
	}

	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.humanA, 2); err != nil {
		t.Fatalf("read through 2: %v", err)
	}
	sum = summaries()
	if sum.LastReadSeq != 2 || sum.UnreadCount != 1 || sum.MentionCount != 1 {
		t.Fatalf("after read 2: %+v", sum)
	}

	// Monotonic: a stale cursor cannot resurrect unread.
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.humanA, 4); err != nil {
		t.Fatalf("read through 4: %v", err)
	}
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.humanA, 1); err != nil {
		t.Fatalf("stale read through: %v", err)
	}
	sum = summaries()
	if sum.LastReadSeq != 4 || sum.UnreadCount != 0 || sum.MentionCount != 0 {
		t.Fatalf("after read 4 then stale 1: %+v", sum)
	}

	// The future is not readable.
	if err := w.store.ReadThrough(ctx, ch.PlaceID, w.humanA, 99); !errors.Is(err, ErrSeqBeyondLatest) {
		t.Fatalf("read beyond latest: got %v, want ErrSeqBeyondLatest", err)
	}
}

func TestEditAndDeleteAuthorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	msg := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 最初の本文") // seq 1
	if len(msg.Mentions) != 1 {
		t.Fatalf("setup: mention missing: %+v", msg.Mentions)
	}

	// Only the author edits.
	if _, err := w.store.EditMessage(ctx, ch.PlaceID, msg.MessageID, w.humanA, "書き換え"); !errors.Is(err, ErrNotAuthor) {
		t.Fatalf("non-author edit: got %v, want ErrNotAuthor", err)
	}
	edited, err := w.store.EditMessage(ctx, ch.PlaceID, msg.MessageID, w.humanB, "mention を消した本文")
	if err != nil {
		t.Fatalf("author edit: %v", err)
	}
	if edited.EditedAt == nil || len(edited.Mentions) != 0 {
		t.Fatalf("edit must re-resolve mentions: %+v", edited)
	}

	// A plain member cannot delete another's message; a workspace owner can.
	if _, err := w.store.DeleteMessage(ctx, ch.PlaceID, msg.MessageID, w.agent); !errors.Is(err, ErrForbidden) {
		t.Fatalf("member delete: got %v, want ErrForbidden", err)
	}
	if _, err := w.store.DeleteMessage(ctx, ch.PlaceID, msg.MessageID, w.humanA); err != nil {
		t.Fatalf("owner delete: %v", err)
	}
	// Idempotent: deleting a tombstone is a no-op.
	if _, err := w.store.DeleteMessage(ctx, ch.PlaceID, msg.MessageID, w.humanB); err != nil {
		t.Fatalf("delete tombstone: %v", err)
	}

	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 || !history[0].Deleted || history[0].Content != "" || history[0].Seq != 1 {
		t.Fatalf("tombstone must keep seq and lose content: %+v", history)
	}
	// A tombstone cannot be edited.
	if _, err := w.store.EditMessage(ctx, ch.PlaceID, msg.MessageID, w.humanB, "復活"); !errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("edit tombstone: got %v, want ErrMessageDeleted", err)
	}
}

func TestHistoryPaginatesBackwardsWithoutOverlap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)

	for i := 1; i <= 5; i++ {
		w.send(t, ctx, ch.PlaceID, w.humanA, fmt.Sprintf("メッセージ %d", i))
	}
	page, err := w.store.History(ctx, ch.PlaceID, w.humanB, HistoryOptions{Limit: 2})
	if err != nil {
		t.Fatalf("latest page: %v", err)
	}
	if len(page) != 2 || page[0].Seq != 4 || page[1].Seq != 5 {
		t.Fatalf("latest page seqs: %+v", seqsOf(page))
	}
	page, err = w.store.History(ctx, ch.PlaceID, w.humanB, HistoryOptions{Limit: 2, BeforeSeq: 4})
	if err != nil {
		t.Fatalf("older page: %v", err)
	}
	if len(page) != 2 || page[0].Seq != 2 || page[1].Seq != 3 {
		t.Fatalf("older page seqs: %+v", seqsOf(page))
	}
	page, err = w.store.History(ctx, ch.PlaceID, w.humanB, HistoryOptions{Limit: 2, BeforeSeq: 2})
	if err != nil {
		t.Fatalf("oldest page: %v", err)
	}
	if len(page) != 1 || page[0].Seq != 1 {
		t.Fatalf("oldest page seqs: %+v", seqsOf(page))
	}
}

func seqsOf(messages []Message) []int64 {
	out := make([]int64, len(messages))
	for i, m := range messages {
		out[i] = m.Seq
	}
	return out
}
