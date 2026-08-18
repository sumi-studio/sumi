package messaging

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

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
		map[string]any{})
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
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// A member without the channel-management capability is refused, exactly as
	// the Human REST route refuses one.
	status, body := callLocal(t, ctx, server.localCreateChannel, LocalCreateChannelPath, map[string]any{
		"name": "設計",
	}, authorization)
	if status != http.StatusForbidden {
		t.Fatalf("unprivileged create: status %d body %v", status, body)
	}

	grantManageChannels(t, ctx, w, workspace.WorkspaceID, w.agent)

	status, body = callLocal(t, ctx, server.localCreateChannel, LocalCreateChannelPath, map[string]any{
		"name": "設計", "topic": "構造の話",
	}, authorization)
	if status != http.StatusCreated {
		t.Fatalf("create channel: status %d body %v", status, body)
	}
	created := body["channel"].(map[string]any)
	if created["name"] != "設計" || created["topic"] != "構造の話" {
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
		"place_id": channel.PlaceID,
	}, authorization)
	if status != http.StatusCreated {
		t.Fatalf("duplicate channel: status %d body %v", status, body)
	}
	if body["channel"].(map[string]any)["name"] != "general のコピー" {
		t.Fatalf("duplicated channel = %v", body)
	}

	// One participant is a DM, and asking twice returns the same conversation.
	status, body = callLocal(t, ctx, server.localStartDM, LocalStartDMPath, map[string]any{
		"participants": []any{map[string]any{"kind": "human", "human_id": w.humanA.ID}},
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("start dm: status %d body %v", status, body)
	}
	dm := body["dm"].(map[string]any)
	if dm["kind"] != "dm" || body["created"] != true {
		t.Fatalf("start dm = %v", body)
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
