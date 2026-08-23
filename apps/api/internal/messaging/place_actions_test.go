package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestChannelWireCarriesPlaceRevision(t *testing.T) {
	wire := channelToWire(Place{PlaceID: "place-a", WorkspaceID: "workspace-a", Revision: 7})
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal channel wire: %v", err)
	}
	var projected map[string]any
	if err := json.Unmarshal(encoded, &projected); err != nil {
		t.Fatalf("unmarshal channel wire: %v", err)
	}
	if projected["revision"] != float64(7) {
		t.Fatalf("channel wire revision = %#v, want 7", projected["revision"])
	}
}

// A channel's name and its topic are the same one answer to「このチャンネルは
// 何か」. Editing one must not silently answer the other, and an edit that
// names nothing has to be refused rather than reported as done — a caller that
// reads a no-op as success believes a rename happened that did not.
func TestUpdateChannelChangesOnlyWhatWasNamed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	renamed := "設計"
	place, err := scoped.UpdateChannel(ctx, channel.PlaceID, &renamed, nil)
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if place.Name != "設計" || place.Topic != "日々のこと" {
		t.Fatalf("renamed place = %+v, want the topic left alone", place)
	}
	if place.Revision != 2 {
		t.Fatalf("renamed place revision = %d, want 2", place.Revision)
	}

	retopic := ""
	place, err = scoped.UpdateChannel(ctx, channel.PlaceID, nil, &retopic)
	if err != nil {
		t.Fatalf("clear topic: %v", err)
	}
	// Clearing a topic is a thing someone can mean; it is not the same as
	// omitting it, which is why the arguments are pointers.
	if place.Name != "設計" || place.Topic != "" {
		t.Fatalf("retopiced place = %+v", place)
	}
	if place.Revision != 3 {
		t.Fatalf("retopiced place revision = %d, want 3", place.Revision)
	}

	if _, err := scoped.UpdateChannel(ctx, channel.PlaceID, nil, nil); !errors.Is(err, ErrEmptyChannelUpdate) {
		t.Fatalf("empty edit error = %v, want ErrEmptyChannelUpdate", err)
	}

	empty := ""
	if _, err := scoped.UpdateChannel(ctx, channel.PlaceID, &empty, nil); !errors.Is(err, ErrInvalidChannelName) {
		t.Fatalf("empty name error = %v, want ErrInvalidChannelName", err)
	}
	overlong := strings.Repeat("あ", MaxChannelNameChars+1)
	if _, err := scoped.UpdateChannel(ctx, channel.PlaceID, &overlong, nil); !errors.Is(err, ErrInvalidChannelName) {
		t.Fatalf("overlong name error = %v, want ErrInvalidChannelName", err)
	}
}

// Two people editing the same channel at once are each editing a different
// field of one answer to「このチャンネルは何か」. The reply — and the
// place_updated built from it (http.go serveUpdatePlace) — has to be what the
// channel now is. An answer assembled from what this request happened to read
// on its way in would carry the other person's field as it was before their
// edit, and every open screen would take that as the current name.
func TestAConcurrentEditIsAnsweredWithWhatTheChannelNowIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	// Someone else's topic change, written but not yet committed. The rename
	// below reads the place before this lands and writes after it.
	other, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the other edit: %v", err)
	}
	defer func() { _ = other.Rollback(context.Background()) }()
	if _, err := other.Exec(ctx, `
		UPDATE places SET topic = $1 WHERE workspace_id = $2 AND place_id = $3`,
		"設計の話", workspace.WorkspaceID, channel.PlaceID); err != nil {
		t.Fatalf("other edit: %v", err)
	}

	type renameResult struct {
		place Place
		err   error
	}
	renamed := "設計"
	done := make(chan renameResult, 1)
	go func() {
		place, err := scoped.UpdateChannel(ctx, channel.PlaceID, &renamed, nil)
		done <- renameResult{place: place, err: err}
	}()
	waitForWaitingBackend(t, ctx, w.store.pool)
	if err := other.Commit(ctx); err != nil {
		t.Fatalf("commit the other edit: %v", err)
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("rename: %v", got.err)
	}
	if got.place.Name != "設計" || got.place.Topic != "設計の話" {
		t.Fatalf("renamed place = %+v, want both edits", got.place)
	}
	var name, topic string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT name, topic FROM places WHERE place_id = $1`,
		channel.PlaceID).Scan(&name, &topic); err != nil {
		t.Fatalf("read back the place: %v", err)
	}
	if name != got.place.Name || topic != got.place.Topic {
		t.Fatalf("stored place = %q/%q, answer said %+v", name, topic, got.place)
	}
}

// Duplicating carries the shape and nothing else. Messages, read state, and
// notification settings belong to the channel they were made in; a copy that
// dragged them along would be claiming things about the new place that nobody
// there ever did.
func TestDuplicateChannelCarriesTheShapeAndNotTheContents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	w.send(t, ctx, channel.PlaceID, w.humanA, "元のチャンネルの発言")
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	copied, err := scoped.DuplicateChannel(ctx, channel.PlaceID, "")
	if err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if copied.PlaceID == channel.PlaceID {
		t.Fatal("duplicate returned the original place")
	}
	if copied.Name != "general のコピー" || copied.Topic != channel.Topic {
		t.Fatalf("copy = %+v, want the derived name and the same topic", copied)
	}
	history, err := scoped.History(ctx, copied.PlaceID, HistoryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("copy has %d messages, want an empty place", len(history))
	}

	// Copying the copy does not stack suffixes forever.
	second, err := scoped.DuplicateChannel(ctx, copied.PlaceID, "")
	if err != nil {
		t.Fatalf("duplicate the copy: %v", err)
	}
	if second.Name != "general のコピー" {
		t.Fatalf("second copy = %q, want the suffix not to accumulate", second.Name)
	}

	// An explicit name wins over the derived one.
	named, err := scoped.DuplicateChannel(ctx, channel.PlaceID, "general-2")
	if err != nil {
		t.Fatalf("duplicate with a name: %v", err)
	}
	if named.Name != "general-2" {
		t.Fatalf("named copy = %q", named.Name)
	}
}

func TestPlaceCreationNonceReplaysTheCommittedPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, source := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	channel, created, err := scoped.CreateChannelOnce(ctx, "incident", "coordination", false, "create-once")
	if err != nil || !created {
		t.Fatalf("first channel create = (%+v, %v, %v), want created", channel, created, err)
	}
	replayed, created, err := scoped.CreateChannelOnce(ctx, "incident", "coordination", false, "create-once")
	if err != nil || created || replayed.PlaceID != channel.PlaceID {
		t.Fatalf("channel replay = (%+v, %v, %v), want same place and created=false", replayed, created, err)
	}
	if _, _, err := scoped.CreateChannelOnce(ctx, "different", "coordination", false, "create-once"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed channel request under nonce = %v, want ErrIdempotencyConflict", err)
	}

	copy, created, err := scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "duplicate-once")
	if err != nil || !created {
		t.Fatalf("first duplicate = (%+v, %v, %v), want created", copy, created, err)
	}
	replayed, created, err = scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "duplicate-once")
	if err != nil || created || replayed.PlaceID != copy.PlaceID {
		t.Fatalf("duplicate replay = (%+v, %v, %v), want same place and created=false", replayed, created, err)
	}

	group, created, err := scoped.CreateGroupDMOnce(ctx, []ParticipantRef{w.humanB, w.agent}, "group-once")
	if err != nil || !created {
		t.Fatalf("first group dm = (%+v, %v, %v), want created", group, created, err)
	}
	replayed, created, err = scoped.CreateGroupDMOnce(ctx, []ParticipantRef{w.humanB, w.agent}, "group-once")
	if err != nil || created || replayed.PlaceID != group.PlaceID {
		t.Fatalf("group DM replay = (%+v, %v, %v), want same place and created=false", replayed, created, err)
	}
	if _, _, err := scoped.CreateGroupDMOnce(ctx, []ParticipantRef{w.agent, w.humanB}, "group-once"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed group DM request under nonce = %v, want ErrIdempotencyConflict", err)
	}

	// Receipt identity is scoped by actor as well as Workspace. The same nonce
	// from another authenticated member is a different creation, not a replay.
	grantManageChannels(t, ctx, w, workspace.WorkspaceID, w.humanB)
	otherActor := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	otherChannel, created, err := otherActor.CreateChannelOnce(ctx, "incident", "coordination", false, "create-once")
	if err != nil || !created || otherChannel.PlaceID == channel.PlaceID {
		t.Fatalf("other actor create = (%+v, %v, %v), want an independent place", otherChannel, created, err)
	}

	// A new installation authority epoch is a new exact session scope. A nonce
	// retained by an old caller cannot reconcile a new-session mutation to the
	// stale session's place.
	if _, err := w.apps.SetEnabledByID(ctx, scoped.Scope.InstallationID, w.humanA, false); err != nil {
		t.Fatalf("disable Messaging installation: %v", err)
	}
	if _, err := w.apps.SetEnabledByID(ctx, scoped.Scope.InstallationID, w.humanA, true); err != nil {
		t.Fatalf("re-enable Messaging installation: %v", err)
	}
	newSession := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	newSessionChannel, created, err := newSession.CreateChannelOnce(ctx, "incident", "coordination", false, "create-once")
	if err != nil || !created || newSessionChannel.PlaceID == channel.PlaceID {
		t.Fatalf("new session create = (%+v, %v, %v), want an independent place", newSessionChannel, created, err)
	}

	// Workspace is also part of the durable identity.
	otherWorkspace, _ := w.workspaceWithChannel(t, ctx)
	otherWorkspaceStore := w.store.mustScope(t, ctx, otherWorkspace.WorkspaceID, w.humanA)
	otherWorkspaceChannel, created, err := otherWorkspaceStore.CreateChannelOnce(ctx, "incident", "coordination", false, "create-once")
	if err != nil || !created || otherWorkspaceChannel.PlaceID == channel.PlaceID {
		t.Fatalf("other Workspace create = (%+v, %v, %v), want an independent place", otherWorkspaceChannel, created, err)
	}
}

func TestChannelCreationReceiptsDoNotReplayAcrossWorkspaceTenures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, source := w.workspaceWithChannel(t, ctx)
	grantManageChannels(t, ctx, w, workspace.WorkspaceID, w.agent)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	membershipM1 := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
	var manageChannelsRoleID string
	if err := w.store.pool.QueryRow(ctx, `
		SELECT role_id FROM workspace_role_assignments
		WHERE workspace_id = $1 AND workspace_member_id = $2`,
		workspace.WorkspaceID, membershipM1,
	).Scan(&manageChannelsRoleID); err != nil {
		t.Fatalf("load M1 channel-management role: %v", err)
	}

	createdM1, fresh, err := scoped.CreateChannelOnce(ctx, "tenure-create", "", false, "tenure-create-nonce")
	if err != nil || !fresh {
		t.Fatalf("M1 create = (%+v, %v, %v), want fresh", createdM1, fresh, err)
	}
	duplicatedM1, fresh, err := scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "tenure-duplicate-nonce")
	if err != nil || !fresh {
		t.Fatalf("M1 duplicate = (%+v, %v, %v), want fresh", duplicatedM1, fresh, err)
	}

	if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
		t.Fatalf("remove M1: %v", err)
	}
	if err := w.store.addWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
		t.Fatalf("join M2: %v", err)
	}
	membershipM2 := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
	if membershipM2 == membershipM1 {
		t.Fatalf("rejoin reused Workspace membership %s", membershipM2)
	}
	if _, err := w.workspaces.SetMembershipRoles(
		ctx, workspace.WorkspaceID, membershipM2, w.humanA, []string{manageChannelsRoleID},
	); err != nil {
		t.Fatalf("grant M2 channel-management role: %v", err)
	}

	createdM2, fresh, err := scoped.CreateChannelOnce(ctx, "tenure-create", "", false, "tenure-create-nonce")
	if err != nil || !fresh || createdM2.PlaceID == createdM1.PlaceID {
		t.Fatalf("M2 create = (%+v, %v, %v), want independent fresh place from M1 %s", createdM2, fresh, err, createdM1.PlaceID)
	}
	duplicatedM2, fresh, err := scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "tenure-duplicate-nonce")
	if err != nil || !fresh || duplicatedM2.PlaceID == duplicatedM1.PlaceID {
		t.Fatalf("M2 duplicate = (%+v, %v, %v), want independent fresh place from M1 %s", duplicatedM2, fresh, err, duplicatedM1.PlaceID)
	}

	for name, replay := range map[string]func() (Place, bool, error){
		"create": func() (Place, bool, error) {
			return scoped.CreateChannelOnce(ctx, "tenure-create", "", false, "tenure-create-nonce")
		},
		"duplicate": func() (Place, bool, error) {
			return scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "tenure-duplicate-nonce")
		},
	} {
		got, replayFresh, replayErr := replay()
		wantPlaceID := createdM2.PlaceID
		if name == "duplicate" {
			wantPlaceID = duplicatedM2.PlaceID
		}
		if replayErr != nil || replayFresh || got.PlaceID != wantPlaceID {
			t.Fatalf("M2 %s replay = (%+v, %v, %v), want M2 place %s", name, got, replayFresh, replayErr, wantPlaceID)
		}
	}
}

func TestGroupDMCreationReceiptsDoNotReplayAcrossActorWorkspaceTenures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	requested := []ParticipantRef{w.humanA, w.humanB}
	membershipM1 := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)

	createdM1, fresh, err := scoped.CreateGroupDMOnce(ctx, requested, "group-tenure-nonce")
	if err != nil || !fresh {
		t.Fatalf("M1 group DM = (%+v, %v, %v), want fresh", createdM1, fresh, err)
	}
	if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
		t.Fatalf("remove M1: %v", err)
	}
	if err := w.store.addWorkspaceMember(ctx, workspace.WorkspaceID, w.agent); err != nil {
		t.Fatalf("join M2: %v", err)
	}
	membershipM2 := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
	if membershipM2 == membershipM1 {
		t.Fatalf("rejoin reused Workspace membership %s", membershipM2)
	}

	createdM2, fresh, err := scoped.CreateGroupDMOnce(ctx, requested, "group-tenure-nonce")
	if err != nil || !fresh || createdM2.PlaceID == createdM1.PlaceID {
		t.Fatalf("M2 group DM = (%+v, %v, %v), want independent fresh place from M1 %s", createdM2, fresh, err, createdM1.PlaceID)
	}
	replayedM2, fresh, err := scoped.CreateGroupDMOnce(ctx, requested, "group-tenure-nonce")
	if err != nil || fresh || replayedM2.PlaceID != createdM2.PlaceID {
		t.Fatalf("M2 group DM replay = (%+v, %v, %v), want M2 place %s", replayedM2, fresh, err, createdM2.PlaceID)
	}
}

func TestConcurrentGroupDMCreationReceiptIsStableWithinOneWorkspaceTenure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	requested := []ParticipantRef{w.humanA, w.humanB}

	type result struct {
		place Place
		fresh bool
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			place, fresh, err := scoped.CreateGroupDMOnce(ctx, requested, "concurrent-group-tenure-nonce")
			results <- result{place: place, fresh: fresh, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent group DM errors = (%v, %v)", first.err, second.err)
	}
	if first.place.PlaceID == "" || second.place.PlaceID != first.place.PlaceID {
		t.Fatalf("concurrent group DMs = (%+v, %+v), want one stable place", first.place, second.place)
	}
	if first.fresh == second.fresh {
		t.Fatalf("concurrent group DM freshness = (%v, %v), want one creator and one replay", first.fresh, second.fresh)
	}
}

func TestPlaceCreationReplayRevalidatesCurrentAuthorityAndPrivateTenure(t *testing.T) {
	t.Run("other participant Workspace tenure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, _ := w.workspaceWithChannel(t, ctx)
		scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		requested := []ParticipantRef{w.humanA, w.humanB}
		created, fresh, err := scoped.CreateGroupDMOnce(ctx, requested, "tenure-replay")
		if err != nil || !fresh {
			t.Fatalf("create group DM = (%+v, %v, %v)", created, fresh, err)
		}
		if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
			t.Fatalf("remove participant: %v", err)
		}
		if err := w.store.addWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
			t.Fatalf("rejoin participant: %v", err)
		}
		replayed, _, err := scoped.CreateGroupDMOnce(ctx, requested, "tenure-replay")
		if err == nil || replayed.PlaceID != "" {
			t.Fatalf("stale replay = (%+v, %v), want fail closed without place identity", replayed, err)
		}
	})

	t.Run("installation disable and epoch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, _ := w.workspaceWithChannel(t, ctx)
		scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		requested := []ParticipantRef{w.humanA, w.humanB}
		created, fresh, err := scoped.CreateGroupDMOnce(ctx, requested, "authority-replay")
		if err != nil || !fresh {
			t.Fatalf("create group DM = (%+v, %v, %v)", created, fresh, err)
		}
		if _, err := w.apps.SetEnabledByID(ctx, scoped.Scope.InstallationID, w.humanA, false); err != nil {
			t.Fatalf("disable installation: %v", err)
		}
		replayed, _, err := scoped.CreateGroupDMOnce(ctx, requested, "authority-replay")
		if err == nil || replayed.PlaceID != "" {
			t.Fatalf("disabled replay = (%+v, %v), want fail closed without place identity", replayed, err)
		}
		if _, err := w.apps.SetEnabledByID(ctx, scoped.Scope.InstallationID, w.humanA, true); err != nil {
			t.Fatalf("re-enable installation: %v", err)
		}
		replayed, _, err = scoped.CreateGroupDMOnce(ctx, requested, "authority-replay")
		if err == nil || replayed.PlaceID != "" {
			t.Fatalf("stale-epoch replay = (%+v, %v), want fail closed without place identity", replayed, err)
		}
		newSession := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		independent, fresh, err := newSession.CreateGroupDMOnce(ctx, requested, "authority-replay")
		if err != nil || !fresh || independent.PlaceID == created.PlaceID {
			t.Fatalf("new-epoch creation = (%+v, %v, %v), want independent place", independent, fresh, err)
		}
	})

	t.Run("channel role revoke", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w := newWorld(t, ctx)
		workspace, source := w.workspaceWithChannel(t, ctx)
		grantManageChannels(t, ctx, w, workspace.WorkspaceID, w.agent)
		scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
		created, fresh, err := scoped.CreateChannelOnce(ctx, "role-fenced", "", false, "role-replay")
		if err != nil || !fresh {
			t.Fatalf("create channel = (%+v, %v, %v)", created, fresh, err)
		}
		duplicated, fresh, err := scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "duplicate-role-replay")
		if err != nil || !fresh {
			t.Fatalf("duplicate channel = (%+v, %v, %v)", duplicated, fresh, err)
		}
		membershipID := activeMembershipID(t, ctx, w, workspace.WorkspaceID, w.agent)
		if _, err := w.workspaces.SetMembershipRoles(
			ctx, workspace.WorkspaceID, membershipID, w.humanA, nil,
		); err != nil {
			t.Fatalf("revoke channel role: %v", err)
		}
		replayed, _, err := scoped.CreateChannelOnce(ctx, "role-fenced", "", false, "role-replay")
		if !errors.Is(err, ErrForbidden) || replayed.PlaceID != "" {
			t.Fatalf("role-revoked replay = (%+v, %v), want forbidden without place identity", replayed, err)
		}
		replayed, _, err = scoped.DuplicateChannelOnce(ctx, source.PlaceID, "", "duplicate-role-replay")
		if !errors.Is(err, ErrForbidden) || replayed.PlaceID != "" {
			t.Fatalf("role-revoked duplicate replay = (%+v, %v), want forbidden without place identity", replayed, err)
		}
	})
}

// The first handler response is intentionally discarded, as when the peer
// loses the response after commit. A normal second local-control request must
// recover the canonical place without a PA-only manual retry API.
func TestLocalPlaceCreationRoutesRecoverACommittedLostResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, source := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	server.Hub = NewHub(w.store.core)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	grantManageChannels(t, ctx, w, workspace.WorkspaceID, w.agent)
	exactScope := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent).Scope

	retry := func(
		name, path string,
		handler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization),
		request map[string]any,
		identity func(map[string]any) string,
	) {
		t.Helper()
		firstStatus, first := callLocal(t, ctx, handler, path, request, authorization)
		if firstStatus != http.StatusCreated && firstStatus != http.StatusOK {
			t.Fatalf("%s committed attempt: status %d body %v", name, firstStatus, first)
		}
		// Drop first/firstStatus here: the retry has no receipt supplied by the
		// caller, only the same ordinary route request and nonce.
		secondStatus, second := callLocal(t, ctx, handler, path, request, authorization)
		if secondStatus != http.StatusOK || identity(first) == "" || identity(second) != identity(first) {
			t.Fatalf("%s retry: first=%v second status=%d body=%v", name, first, secondStatus, second)
		}
		for _, response := range []map[string]any{first, second} {
			if response["workspace_id"] != exactScope.WorkspaceID ||
				response["installation_id"] != exactScope.InstallationID ||
				response["authority_epoch"] != strconv.FormatInt(exactScope.AuthorityEpoch, 10) {
				t.Fatalf("%s response scope = %v, want exact local scope", name, response)
			}
		}
		if created, ok := second["created"]; ok && created != false {
			t.Fatalf("%s retry created=%v, want false", name, created)
		}
	}

	retry("create channel", LocalCreateChannelPath, server.localCreateChannel,
		map[string]any{"name": "lost-create", "client_nonce": "lost-create-nonce"},
		func(body map[string]any) string { return body["channel"].(map[string]any)["channel_id"].(string) })
	retry("duplicate channel", LocalDuplicateChannelPath, server.localDuplicateChannel,
		map[string]any{"place_id": source.PlaceID, "client_nonce": "lost-duplicate-nonce"},
		func(body map[string]any) string { return body["channel"].(map[string]any)["channel_id"].(string) })
	retry("one-to-one DM", LocalStartDMPath, server.localStartDM,
		map[string]any{"participants": []any{map[string]any{"kind": "human", "human_id": w.humanA.ID}}},
		func(body map[string]any) string { return body["dm"].(map[string]any)["dm_id"].(string) })
	groupRequest := map[string]any{
		"client_nonce": "lost-group-nonce",
		"participants": []any{
			map[string]any{"kind": "human", "human_id": w.humanA.ID},
			map[string]any{"kind": "human", "human_id": w.humanB.ID},
		},
	}
	retry("group DM", LocalStartDMPath, server.localStartDM, groupRequest,
		func(body map[string]any) string { return body["dm"].(map[string]any)["dm_id"].(string) })
	if err := w.store.removeWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatalf("remove group participant after reconciliation: %v", err)
	}
	if err := w.store.addWorkspaceMember(ctx, workspace.WorkspaceID, w.humanB); err != nil {
		t.Fatalf("rejoin group participant after reconciliation: %v", err)
	}
	status, stale := callLocal(t, ctx, server.localStartDM, LocalStartDMPath, groupRequest, authorization)
	if status != http.StatusNotFound || stale["dm"] != nil || stale["created"] != nil {
		t.Fatalf("stale group replay leaked a place projection: status %d body %v", status, stale)
	}

	status, body := callLocal(t, ctx, server.localCreateChannel, LocalCreateChannelPath,
		map[string]any{"name": "changed-after-commit", "client_nonce": "lost-create-nonce"}, authorization)
	if status != http.StatusConflict || body["error"] != "idempotency_conflict" {
		t.Fatalf("changed request under committed nonce: status %d body %v", status, body)
	}
}

func TestPlaceEditsOverHTTPRefuseANoOpAndAnnounceTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)

	conn := dialWS(t, ts, w.humanB.ID, nil)

	resp, body := call(t, ts, http.MethodPatch, "/messaging/places/"+channel.PlaceID, w.humanA.ID,
		map[string]any{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty edit: status %d body %v", resp.StatusCode, body)
	}

	resp, body = call(t, ts, http.MethodPatch, "/messaging/places/"+channel.PlaceID, w.humanA.ID,
		map[string]any{"name": "設計"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rename: status %d body %v", resp.StatusCode, body)
	}
	if body["name"] != "設計" || body["topic"] != "日々のこと" {
		t.Fatalf("renamed wire = %v", body)
	}

	resp, body = call(t, ts, http.MethodPost, "/messaging/places/"+channel.PlaceID+"/duplicate", w.humanA.ID,
		map[string]any{"client_nonce": "http-duplicate-place"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("duplicate: status %d body %v", resp.StatusCode, body)
	}
	copyID, _ := body["channel_id"].(string)
	if copyID == "" || copyID == channel.PlaceID {
		t.Fatalf("duplicate wire = %v", body)
	}

	// Someone else in the Workspace learns about both, in order.
	sawUpdated, sawCreated := false, false
	for range 6 {
		frame := readFrame(t, conn)
		event, ok := frame["event"].(map[string]any)
		if !ok {
			continue
		}
		switch event["type"] {
		case EventPlaceUpdated:
			sawUpdated = true
		case EventPlaceCreated:
			if event["place_id"] == copyID {
				sawCreated = true
			}
		}
		if sawUpdated && sawCreated {
			break
		}
	}
	if !sawUpdated || !sawCreated {
		t.Fatalf("place edits were not announced (updated=%v created=%v)", sawUpdated, sawCreated)
	}
}

func TestPlaceCreationHTTPRequiresClientNonceBeforeMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)

	var before int
	if err := w.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM places WHERE workspace_id = $1`, workspace.WorkspaceID,
	).Scan(&before); err != nil {
		t.Fatalf("count places before missing nonces: %v", err)
	}
	tests := []struct {
		name string
		path string
		body map[string]any
	}{
		{
			name: "create channel",
			path: "/messaging/channels",
			body: map[string]any{"workspace_id": workspace.WorkspaceID, "name": "missing-nonce"},
		},
		{
			name: "duplicate channel",
			path: "/messaging/places/" + channel.PlaceID + "/duplicate",
			body: map[string]any{},
		},
		{
			name: "create group DM",
			path: "/messaging/group-dms",
			body: map[string]any{"participants": []any{
				map[string]any{"kind": "human", "human_id": w.humanB.ID},
				map[string]any{"kind": "personality_agent", "personality_agent_id": w.agent.ID},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resp, body := call(t, ts, http.MethodPost, test.path, w.humanA.ID, test.body)
			if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_client_nonce" {
				t.Fatalf("missing nonce: status %d body %v", resp.StatusCode, body)
			}
		})
	}

	var after int
	if err := w.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM places WHERE workspace_id = $1`, workspace.WorkspaceID,
	).Scan(&after); err != nil {
		t.Fatalf("count places after missing nonces: %v", err)
	}
	if after != before {
		t.Fatalf("missing-nonce requests mutated places: before %d after %d", before, after)
	}
}

// The agent reaches the same operations through the same Store, and is refused
// in the same places. It gains no reach a person in that Workspace lacks: a
// plain member is refused channel management whichever lane it arrives on, and
// the sealed scope means there is no Workspace field to be talked into naming.
func TestLocalPlaceActionsMatchTheHumanLane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	hub := NewHub(w.store.core)
	server.Hub = hub
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// A member without the channel-management capability is refused, exactly as
	// the Human REST route refuses one.
	status, body := callLocal(t, ctx, server.localCreateChannel, LocalCreateChannelPath, map[string]any{
		"name": "設計", "client_nonce": "unprivileged-create-channel",
	}, authorization)
	if status != http.StatusForbidden {
		t.Fatalf("unprivileged create: status %d body %v", status, body)
	}

	grantManageChannels(t, ctx, w, workspace.WorkspaceID, w.agent)

	status, body = callLocal(t, ctx, server.localCreateChannel, LocalCreateChannelPath, map[string]any{
		"name": "設計", "topic": "構造の話", "voice": true, "client_nonce": "local-create-channel",
	}, authorization)
	if status != http.StatusCreated {
		t.Fatalf("create channel: status %d body %v", status, body)
	}
	created := body["channel"].(map[string]any)
	if created["name"] != "設計" || created["topic"] != "構造の話" || created["voice"] != true {
		t.Fatalf("created channel = %v", created)
	}

	status, body = callLocal(t, ctx, server.localUpdateChannel, LocalUpdateChannelPath, map[string]any{
		"place_id": created["channel_id"], "topic": "構造と実装の話",
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("update channel: status %d body %v", status, body)
	}
	updated := body["channel"].(map[string]any)
	if updated["name"] != "設計" || updated["topic"] != "構造と実装の話" {
		t.Fatalf("updated channel = %v", updated)
	}

	// Naming nothing is refused here too: the model must not be able to report
	// an edit it did not make.
	status, _ = callLocal(t, ctx, server.localUpdateChannel, LocalUpdateChannelPath, map[string]any{
		"place_id": created["channel_id"],
	}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("empty local edit: status %d, want 400", status)
	}

	status, body = callLocal(t, ctx, server.localDuplicateChannel, LocalDuplicateChannelPath, map[string]any{
		"place_id": channel.PlaceID, "client_nonce": "local-duplicate-channel",
	}, authorization)
	if status != http.StatusCreated {
		t.Fatalf("duplicate channel: status %d body %v", status, body)
	}
	if body["channel"].(map[string]any)["name"] != "general のコピー" {
		t.Fatalf("duplicated channel = %v", body)
	}

	// The actor can appear in an agent request, but it is not an "other": with
	// one actual other this remains a 1:1 DM, and both the response and event
	// describe each real member exactly once.
	observer := hub.subscribe(w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA))
	defer hub.unsubscribe(observer)
	status, body = callLocal(t, ctx, server.localStartDM, LocalStartDMPath, map[string]any{
		"participants": []any{
			map[string]any{"kind": "personality_agent", "personality_agent_id": w.agent.ID},
			map[string]any{"kind": "human", "human_id": w.humanA.ID},
		},
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("start dm: status %d body %v", status, body)
	}
	dm := body["dm"].(map[string]any)
	if dm["kind"] != "dm" || body["created"] != true {
		t.Fatalf("start dm = %v", body)
	}
	assertDMParticipantsOnce(t, dm["participants"], w.agent, w.humanA)
	select {
	case frame := <-observer.send:
		var eventFrame struct {
			Event struct {
				Type string `json:"type"`
				DM   struct {
					Participants any `json:"participants"`
				} `json:"dm"`
			} `json:"event"`
		}
		if err := json.Unmarshal(frame.payload, &eventFrame); err != nil {
			t.Fatalf("decode place-created event: %v", err)
		}
		if eventFrame.Event.Type != EventPlaceCreated {
			t.Fatalf("event type = %q, want %q", eventFrame.Event.Type, EventPlaceCreated)
		}
		assertDMParticipantsOnce(t, eventFrame.Event.DM.Participants, w.agent, w.humanA)
	case <-ctx.Done():
		t.Fatal("did not receive place_created for normalized DM")
	}
	status, again := callLocal(t, ctx, server.localStartDM, LocalStartDMPath, map[string]any{
		"participants": []any{map[string]any{"kind": "human", "human_id": w.humanA.ID}},
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("start dm again: status %d body %v", status, again)
	}
	if again["dm"].(map[string]any)["dm_id"] != dm["dm_id"] || again["created"] != false {
		t.Fatalf("second start dm = %v, want the same conversation and created=false", again)
	}

	// Several participants make a group conversation instead.
	status, body = callLocal(t, ctx, server.localStartDM, LocalStartDMPath, map[string]any{
		"client_nonce": "local-start-group-dm",
		"participants": []any{
			map[string]any{"kind": "human", "human_id": w.humanA.ID},
			map[string]any{"kind": "human", "human_id": w.humanB.ID},
		},
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("start group dm: status %d body %v", status, body)
	}
	if body["dm"].(map[string]any)["kind"] != "group_dm" {
		t.Fatalf("group dm = %v", body)
	}

	// Naming no one is refused rather than opening a conversation with nobody.
	status, _ = callLocal(t, ctx, server.localStartDM, LocalStartDMPath, map[string]any{
		"participants": []any{},
	}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("empty participants: status %d, want 400", status)
	}
}

func assertDMParticipantsOnce(t *testing.T, raw any, actor, other ParticipantRef) {
	t.Helper()
	participants, ok := raw.([]any)
	if !ok || len(participants) != 2 {
		t.Fatalf("dm participants = %v, want exactly two", raw)
	}
	seen := map[string]int{}
	for _, rawParticipant := range participants {
		participant, ok := rawParticipant.(map[string]any)
		if !ok {
			t.Fatalf("dm participant = %T, want object", rawParticipant)
		}
		kind, _ := participant["kind"].(string)
		id, _ := participant["human_id"].(string)
		if kind == string(KindPersonalityAgent) {
			id, _ = participant["personality_agent_id"].(string)
		}
		seen[kind+":"+id]++
	}
	if seen[actor.Key()] != 1 || seen[other.Key()] != 1 {
		t.Fatalf("dm participants = %v, want %s and %s once", raw, actor.Key(), other.Key())
	}
}

// grantManageChannels gives a member a role carrying the app-owned channel
// management capability, the same grant a Workspace owner would make.
func grantManageChannels(
	t *testing.T,
	ctx context.Context,
	w world,
	workspaceID string,
	member ParticipantRef,
) {
	t.Helper()
	role, err := w.workspaces.CreateRole(ctx, workspaceID, w.humanA,
		"チャンネル管理", "", map[string]bool{ManageChannelsCapability: true})
	if err != nil {
		t.Fatalf("create role: %v", err)
	}
	memberships, err := w.workspaces.Members(ctx, workspaceID, w.humanA)
	if err != nil {
		t.Fatalf("list members: %v", err)
	}
	for _, membership := range memberships {
		if membership.Participant.Kind != member.Kind || membership.Participant.ID != member.ID {
			continue
		}
		if _, err := w.workspaces.SetMembershipRoles(
			ctx, workspaceID, membership.WorkspaceMemberID, w.humanA, []string{role.RoleID},
		); err != nil {
			t.Fatalf("assign role: %v", err)
		}
		return
	}
	t.Fatalf("member %s is not in workspace %s", member.Key(), workspaceID)
}
