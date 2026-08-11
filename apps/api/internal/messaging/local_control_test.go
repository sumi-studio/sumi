package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/testfs"
)

func TestLocalWriteBoundaryThroughAuthenticatedControlRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	workspace, channel := world.workspaceWithChannel(t, ctx)
	messagingServer := NewServer(world.store, nil)

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(testfs.PrivateDir(t), commandStore)
	if err != nil {
		t.Fatalf("open durable gateway: %v", err)
	}
	const bearer = "messaging-write-boundary-bearer-token"
	control, err := agentevents.NewLocalControlServer(
		gateway,
		[]byte("messaging-write-signing-secret-32-bytes"),
		[]agentevents.LocalRuntimeAuthorization{{
			BearerToken:           bearer,
			TenantID:              "tenant-local",
			PersonalityAgentID:    world.agent.ID,
			Generation:            1,
			RPCBootNonce:          "boot-write-boundary",
			Audience:              "sumi:agent:events",
			DeliveryAuthorization: agentevents.LocalDeliveryRaw,
		}},
	)
	if err != nil {
		t.Fatalf("new local control: %v", err)
	}
	if err := messagingServer.RegisterLocalControlRoutes(control); err != nil {
		t.Fatalf("register messaging routes: %v", err)
	}
	handler, err := control.HandlerForLocalRuntime(world.agent.ID)
	if err != nil {
		t.Fatalf("bind local runtime handler: %v", err)
	}
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	type durableState struct {
		workspaces          int64
		places              int64
		agentMemberships    int64
		messages            int64
		notificationIntents int64
		lastSeq             int64
	}
	snapshot := func() durableState {
		t.Helper()
		var state durableState
		queries := []struct {
			query string
			dest  *int64
			args  []any
		}{
			{`SELECT count(*) FROM workspaces WHERE workspace_id = $1`, &state.workspaces, []any{workspace.WorkspaceID}},
			{`SELECT count(*) FROM places WHERE place_id = $1`, &state.places, []any{channel.PlaceID}},
			{`SELECT count(*) FROM workspace_members WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL`, &state.agentMemberships, []any{workspace.WorkspaceID, world.agent.Kind, world.agent.ID}},
			{`SELECT count(*) FROM messages WHERE place_id = $1 AND author_kind = $2 AND author_id = $3`, &state.messages, []any{channel.PlaceID, world.agent.Kind, world.agent.ID}},
			{`SELECT count(*) FROM message_notification_intents WHERE message_id IN (SELECT message_id FROM messages WHERE place_id = $1 AND author_kind = $2 AND author_id = $3)`, &state.notificationIntents, []any{channel.PlaceID, world.agent.Kind, world.agent.ID}},
			{`SELECT last_seq FROM places WHERE place_id = $1`, &state.lastSeq, []any{channel.PlaceID}},
		}
		for _, query := range queries {
			if err := world.store.pool.QueryRow(ctx, query.query, query.args...).Scan(query.dest); err != nil {
				t.Fatalf("snapshot durable write state: %v", err)
			}
		}
		return state
	}

	type postResult struct {
		status int
		body   map[string]any
		raw    []byte
	}
	post := func(content, nonce, authorization string) postResult {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"place_id": channel.PlaceID,
			"content":  content, "urgency": "normal", "client_nonce": nonce,
		})
		if err != nil {
			t.Fatalf("marshal local write: %v", err)
		}
		request, err := http.NewRequestWithContext(
			ctx, http.MethodPost, httpServer.URL+LocalWritePath, bytes.NewReader(payload),
		)
		if err != nil {
			t.Fatalf("new local write request: %v", err)
		}
		request.Header.Set("Content-Type", "application/json")
		if authorization != "" {
			request.Header.Set("Authorization", "Bearer "+authorization)
		}
		response, err := httpServer.Client().Do(request)
		if err != nil {
			t.Fatalf("post local write: %v", err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read local write response: %v", err)
		}
		body := map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Fatalf("decode local write response %q: %v", raw, err)
			}
		}
		return postResult{status: response.StatusCode, body: body, raw: raw}
	}

	initial := snapshot()
	if initial.workspaces != 1 || initial.places != 1 || initial.agentMemberships != 1 {
		t.Fatalf("fixture did not explicitly provision authority: %#v", initial)
	}
	if result := post("not authorized", "nonce-unauthorized", "wrong-bearer-token-with-32-bytes-minimum"); result.status != http.StatusUnauthorized {
		t.Fatalf("wrong bearer status = %d, body %v", result.status, result.body)
	}
	if got := snapshot(); got != initial {
		t.Fatalf("unauthorized write mutated durable state: before %#v after %#v", initial, got)
	}

	for name, content := range map[string]string{
		"over-limit": strings.Repeat("x", MaxContentBytes+1),
		"nul":        strings.Repeat("\x00", MaxContentBytes),
	} {
		result := post(content, "nonce-invalid-"+name, bearer)
		if result.status != http.StatusBadRequest || result.body["error"] != "invalid_content" {
			t.Fatalf("%s write = %d %v, want 400 invalid_content", name, result.status, result.body)
		}
		if got := snapshot(); got != initial {
			t.Fatalf("%s write mutated durable state: before %#v after %#v", name, initial, got)
		}
	}
	escapedPayload, err := json.Marshal(map[string]any{"content": strings.Repeat("\x01", MaxContentBytes)})
	if err != nil {
		t.Fatal(err)
	}
	if len(escapedPayload) <= MaxContentBytes+64*1024 || len(escapedPayload) > maxRequestBytes {
		t.Fatalf("escaped boundary request size = %d, cap %d", len(escapedPayload), maxRequestBytes)
	}

	// U+0001 is storable but has the six-byte JSON escape expansion. This
	// proves the real bearer-authenticated route admits every legal byte shape.
	escaped := post(strings.Repeat("\x01", MaxContentBytes), "nonce-control", bearer)
	if escaped.status != http.StatusCreated || escaped.body["created"] != true ||
		escaped.body["client_nonce"] != "nonce-control" || len(escaped.raw) >= 1024 {
		t.Fatalf("escaped max write = %d %v (%d response bytes)", escaped.status, escaped.body, len(escaped.raw))
	}

	const quotedNonce = "nonce-\"quoted\"-\\slash"
	maxContent := "@Yohaku " + strings.Repeat("a", MaxContentBytes-len("@Yohaku "))
	created := post(maxContent, quotedNonce, bearer)
	if created.status != http.StatusCreated || created.body["created"] != true ||
		created.body["client_nonce"] != quotedNonce || len(created.body) != 4 {
		t.Fatalf("max write receipt = %d %v", created.status, created.body)
	}
	messageID, _ := created.body["message_id"].(string)
	seq, _ := created.body["seq"].(float64)
	if messageID == "" || seq != 2 || !bytes.Contains(created.raw, []byte(`\"quoted\"-\\slash`)) {
		t.Fatalf("max write identity/escaping = %v raw %q", created.body, created.raw)
	}
	afterCreated := snapshot()
	if afterCreated.messages != 2 || afterCreated.lastSeq != 2 ||
		afterCreated.notificationIntents <= initial.notificationIntents {
		t.Fatalf("created writes did not commit messages and notification intents atomically: %#v", afterCreated)
	}

	replayed := post(maxContent, quotedNonce, bearer)
	if replayed.status != http.StatusOK || replayed.body["created"] != false ||
		replayed.body["message_id"] != messageID || replayed.body["seq"] != seq ||
		replayed.body["client_nonce"] != quotedNonce {
		t.Fatalf("same-nonce replay = %d %v, first %v", replayed.status, replayed.body, created.body)
	}
	if got := snapshot(); got != afterCreated {
		t.Fatalf("same-nonce replay changed durable state: before %#v after %#v", afterCreated, got)
	}

	fresh := post(maxContent, "nonce-new-call", bearer)
	if fresh.status != http.StatusCreated || fresh.body["created"] != true ||
		fresh.body["message_id"] == messageID || fresh.body["seq"] != float64(3) {
		t.Fatalf("new-nonce write = %d %v, first %v", fresh.status, fresh.body, created.body)
	}
	final := snapshot()
	if final.workspaces != 1 || final.places != 1 || final.agentMemberships != 1 ||
		final.messages != 3 || final.lastSeq != 3 ||
		final.notificationIntents <= afterCreated.notificationIntents {
		t.Fatalf("final durable state = %#v, want a distinct third commit and its notification intents", final)
	}
}

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
	for _, field := range []string{"workspaces", "channels", "dms", "members", "read_markers", "unread_summaries", "reply_later_markers"} {
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
