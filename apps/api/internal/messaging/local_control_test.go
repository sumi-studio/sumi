package messaging

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func totalWorkspaceMemberships(t *testing.T, ctx context.Context, store *Store) int {
	t.Helper()
	var count int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM workspace_members").Scan(&count); err != nil {
		t.Fatalf("count workspace memberships: %v", err)
	}
	return count
}

func workspaceMembershipEpisodes(
	t *testing.T, ctx context.Context, store *Store, workspaceID string, participant ParticipantRef,
) (active, historical int) {
	t.Helper()
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE left_at IS NULL),
		        count(*) FILTER (WHERE left_at IS NOT NULL)
		 FROM workspace_members
		 WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3`,
		workspaceID, participant.Kind, participant.ID).Scan(&active, &historical); err != nil {
		t.Fatalf("inspect membership episodes for %s: %v", participant.Key(), err)
	}
	return active, historical
}

func assertEmptyOverview(t *testing.T, body map[string]any) {
	t.Helper()
	for _, field := range []string{"workspaces", "channels", "dms", "members", "read_markers", "unread_summaries"} {
		values, ok := body[field].([]any)
		if !ok || len(values) != 0 {
			t.Fatalf("%s = %#v, want an empty array", field, body[field])
		}
	}
}

func TestBootstrapAndOverviewAreReadOnlyForZeroMembershipParticipants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	var workspaceCount int
	if err := w.store.pool.QueryRow(ctx, "SELECT count(*) FROM workspaces").Scan(&workspaceCount); err != nil {
		t.Fatalf("count initial workspaces: %v", err)
	}
	if workspaceCount != 0 {
		t.Fatalf("fresh schema has %d workspaces, want explicit provisioning only", workspaceCount)
	}

	for range 2 {
		resp, body := call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Human bootstrap: status %d body %v", resp.StatusCode, body)
		}
		assertEmptyOverview(t, body)

		status, localBody := callLocal(
			t, ctx, server.localOverview, LocalOverviewPath, map[string]any{}, authorization)
		if status != http.StatusOK {
			t.Fatalf("PersonalityAgent overview: status %d body %v", status, localBody)
		}
		assertEmptyOverview(t, localBody)

		status, setting := callLocal(
			t, ctx, server.localNotificationSettings, LocalNotificationSettingsPath,
			map[string]any{}, authorization)
		if status != http.StatusOK || setting["setting"] == nil {
			t.Fatalf("participant-scoped setting read: status %d body %v", status, setting)
		}
		if got := totalWorkspaceMemberships(t, ctx, w.store); got != 0 {
			t.Fatalf("reads created %d workspace memberships", got)
		}
	}
}

func TestReadsAndDirectOperationsCannotManufactureWorkspaceAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	workspace, err := w.store.CreateWorkspace(ctx, "explicit", w.humanA)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	channel, err := w.store.CreateChannel(ctx, workspace.WorkspaceID, "general", "", w.humanA)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if got := totalWorkspaceMemberships(t, ctx, w.store); got != 1 {
		t.Fatalf("explicit creator membership count = %d, want 1", got)
	}

	// Reading as the Human does not infer membership for that Human's agent.
	for _, path := range []string{
		"/messaging/bootstrap",
		"/messaging/places/" + channel.PlaceID,
		"/messaging/places/" + channel.PlaceID + "/messages",
	} {
		resp, body := call(t, ts, http.MethodGet, path, w.humanA.ID, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Human read %s: status %d body %v", path, resp.StatusCode, body)
		}
	}
	if active, historical := workspaceMembershipEpisodes(
		t, ctx, w.store, workspace.WorkspaceID, w.agent,
	); active != 0 || historical != 0 {
		t.Fatalf("Human reads admitted owned agent: active=%d historical=%d", active, historical)
	}

	// Explicit open/write by an unadmitted agent and a second Human both fail
	// without creating authority or content.
	status, _ := callLocal(t, ctx, server.localOpen, LocalOpenPath, map[string]any{
		"place_id": channel.PlaceID, "limit": 20,
	}, authorization)
	if status != http.StatusNotFound {
		t.Fatalf("unadmitted agent open status = %d, want 404", status)
	}
	status, _ = callLocal(t, ctx, server.localWrite, LocalWritePath, map[string]any{
		"place_id": channel.PlaceID, "content": "should not land", "urgency": "normal",
		"client_nonce": "unadmitted-agent",
	}, authorization)
	if status != http.StatusNotFound {
		t.Fatalf("unadmitted agent write status = %d, want 404", status)
	}
	resp, body := call(t, ts, http.MethodPost,
		"/messaging/places/"+channel.PlaceID+"/messages", w.humanB.ID,
		map[string]any{"content": "should not land", "client_nonce": "unadmitted-human"})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unadmitted Human write: status %d body %v", resp.StatusCode, body)
	}
	if got := totalWorkspaceMemberships(t, ctx, w.store); got != 1 {
		t.Fatalf("failed direct operations changed membership count to %d", got)
	}
	var messageCount int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM messages WHERE place_id = $1", channel.PlaceID).Scan(&messageCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("failed direct operations committed %d messages", messageCount)
	}
}

func TestExplicitMembershipWorksAndReadsNeverResurrectALeftEpisode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	first, err := w.store.CreateWorkspace(ctx, "first", w.humanA)
	if err != nil {
		t.Fatalf("create first workspace: %v", err)
	}
	firstChannel, err := w.store.CreateChannel(ctx, first.WorkspaceID, "first", "", w.humanA)
	if err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	second, err := w.store.CreateWorkspace(ctx, "second", w.humanA)
	if err != nil {
		t.Fatalf("create second workspace: %v", err)
	}
	secondChannel, err := w.store.CreateChannel(ctx, second.WorkspaceID, "second", "", w.humanA)
	if err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	if err := w.store.AddWorkspaceMember(ctx, second.WorkspaceID, w.agent, RoleAdmin); err != nil {
		t.Fatalf("explicitly add agent: %v", err)
	}

	overview, err := server.buildOverview(ctx, w.agent)
	if err != nil {
		t.Fatalf("agent overview: %v", err)
	}
	if len(overview.Workspaces) != 1 || overview.Workspaces[0].WorkspaceID != second.WorkspaceID ||
		len(overview.Channels) != 1 || overview.Channels[0].ChannelID != secondChannel.PlaceID {
		t.Fatalf("agent overview inferred another workspace: %#v", overview)
	}
	status, _ := callLocal(t, ctx, server.localOpen, LocalOpenPath, map[string]any{
		"place_id": firstChannel.PlaceID, "limit": 20,
	}, authorization)
	if status != http.StatusNotFound {
		t.Fatalf("first-workspace inference opened foreign channel: status %d", status)
	}
	status, _ = callLocal(t, ctx, server.localOpen, LocalOpenPath, map[string]any{
		"place_id": secondChannel.PlaceID, "limit": 20,
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("explicit membership did not open second channel: status %d", status)
	}

	if err := w.store.RemoveWorkspaceMember(ctx, second.WorkspaceID, w.agent); err != nil {
		t.Fatalf("remove agent: %v", err)
	}
	for range 2 {
		overview, err = server.buildOverview(ctx, w.agent)
		if err != nil {
			t.Fatalf("overview after leave: %v", err)
		}
		if len(overview.Workspaces) != 0 || len(overview.Channels) != 0 {
			t.Fatalf("left agent overview = %#v, want empty", overview)
		}
		status, _ = callLocal(t, ctx, server.localOpen, LocalOpenPath, map[string]any{
			"place_id": secondChannel.PlaceID, "limit": 20,
		}, authorization)
		if status != http.StatusNotFound {
			t.Fatalf("read resurrected left membership: open status %d", status)
		}
		if active, historical := workspaceMembershipEpisodes(
			t, ctx, w.store, second.WorkspaceID, w.agent,
		); active != 0 || historical != 1 {
			t.Fatalf("left episode changed: active=%d historical=%d", active, historical)
		}
	}

	// A deliberate rejoin is a new episode and receives the newly named role;
	// the earlier admin role is not restored by identity or history.
	if err := w.store.AddWorkspaceMember(ctx, second.WorkspaceID, w.agent, RoleMember); err != nil {
		t.Fatalf("explicitly rejoin agent: %v", err)
	}
	profiles, err := w.store.WorkspaceMemberProfiles(ctx, second.WorkspaceID, w.agent)
	if err != nil {
		t.Fatalf("list rejoined members: %v", err)
	}
	for _, profile := range profiles {
		if profile.Participant == w.agent && profile.Role != RoleMember {
			t.Fatalf("rejoin restored stale role %q", profile.Role)
		}
	}
	if active, historical := workspaceMembershipEpisodes(
		t, ctx, w.store, second.WorkspaceID, w.agent,
	); active != 1 || historical != 1 {
		t.Fatalf("rejoin episodes: active=%d historical=%d, want 1/1", active, historical)
	}
}
