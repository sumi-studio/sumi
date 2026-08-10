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
)

func TestLocalWriteBoundaryThroughAuthenticatedControlRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	messagingServer := NewServer(world.store, nil)

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(t.TempDir(), commandStore)
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
		workspaces  int64
		places      int64
		memberships int64
		messages    int64
		lastSeq     int64
	}
	snapshot := func() durableState {
		t.Helper()
		var state durableState
		queries := []struct {
			query string
			dest  *int64
			args  []any
		}{
			{`SELECT count(*) FROM workspaces WHERE workspace_id = $1`, &state.workspaces, []any{DefaultWorkspaceID}},
			{`SELECT count(*) FROM places WHERE place_id = $1`, &state.places, []any{DefaultGeneralChannelID}},
			{`SELECT count(*) FROM workspace_members WHERE member_kind = $1 AND member_id = $2`, &state.memberships, []any{world.agent.Kind, world.agent.ID}},
			{`SELECT count(*) FROM messages WHERE author_kind = $1 AND author_id = $2`, &state.messages, []any{world.agent.Kind, world.agent.ID}},
			{`SELECT COALESCE((SELECT last_seq FROM places WHERE place_id = $1), -1)`, &state.lastSeq, []any{DefaultGeneralChannelID}},
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
			"place_id": DefaultGeneralChannelID,
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
	nulPayload, err := json.Marshal(map[string]any{"content": strings.Repeat("\x00", MaxContentBytes)})
	if err != nil {
		t.Fatal(err)
	}
	if len(nulPayload) <= MaxContentBytes+64*1024 || len(nulPayload) > maxRequestBytes {
		t.Fatalf("escaped boundary request size = %d, cap %d", len(nulPayload), maxRequestBytes)
	}

	// U+0001 is storable but has the same six-byte JSON escape expansion as
	// NUL. It proves the widened request cap admits the largest legal escaped
	// payload through the real bearer-authenticated route.
	escaped := post(strings.Repeat("\x01", MaxContentBytes), "nonce-control", bearer)
	if escaped.status != http.StatusCreated || escaped.body["created"] != true ||
		escaped.body["client_nonce"] != "nonce-control" || len(escaped.raw) >= 1024 {
		t.Fatalf("escaped max write = %d %v (%d response bytes)", escaped.status, escaped.body, len(escaped.raw))
	}

	const escapedNonce = "nonce-\"quoted\"-\\slash"
	created := post(strings.Repeat("a", MaxContentBytes), escapedNonce, bearer)
	if created.status != http.StatusCreated || created.body["created"] != true ||
		created.body["client_nonce"] != escapedNonce || len(created.body) != 4 {
		t.Fatalf("max write receipt = %d %v", created.status, created.body)
	}
	messageID, _ := created.body["message_id"].(string)
	seq, _ := created.body["seq"].(float64)
	if messageID == "" || seq != 2 || !bytes.Contains(created.raw, []byte(`\"quoted\"-\\slash`)) {
		t.Fatalf("max write identity/escaping = %v raw %q", created.body, created.raw)
	}

	replayed := post(strings.Repeat("a", MaxContentBytes), escapedNonce, bearer)
	if replayed.status != http.StatusOK || replayed.body["created"] != false ||
		replayed.body["message_id"] != messageID || replayed.body["seq"] != seq ||
		replayed.body["client_nonce"] != escapedNonce {
		t.Fatalf("same-nonce replay = %d %v, first %v", replayed.status, replayed.body, created.body)
	}
	final := snapshot()
	if final.workspaces != 1 || final.places != 1 || final.memberships != 1 ||
		final.messages != 2 || final.lastSeq != 2 {
		t.Fatalf("final durable state = %#v, want two committed messages", final)
	}
}

func TestLocalOpenAdmitsAgentWithoutOverviewFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	server := NewServer(world.store, nil)

	request := httptest.NewRequest(
		http.MethodPost,
		LocalOpenPath,
		strings.NewReader(`{"place_id":"`+DefaultGeneralChannelID+`","limit":20}`),
	).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.localOpen(response, request, agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: world.agent.ID,
	})

	if response.Code != http.StatusOK {
		t.Fatalf("direct first-use open status = %d, body = %s", response.Code, response.Body.String())
	}
	var opened struct {
		Members []memberWire `json:"members"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &opened); err != nil {
		t.Fatalf("decode local open: %v", err)
	}
	if !hasProjectedMember(opened.Members, world.agent, "Kuro（Yohaku）") {
		t.Fatalf("local open members = %#v", opened.Members)
	}
	workspaces, err := world.store.WorkspacesFor(ctx, world.agent)
	if err != nil {
		t.Fatalf("list agent workspaces: %v", err)
	}
	if len(workspaces) != 1 || workspaces[0].WorkspaceID != DefaultWorkspaceID {
		t.Fatalf("agent workspaces = %#v, want only default Workspace", workspaces)
	}
	overview, err := server.buildOverview(ctx, world.agent)
	if err != nil {
		t.Fatalf("build overview: %v", err)
	}
	if !hasProjectedMember(overview.Members, world.agent, "Kuro（Yohaku）") {
		t.Fatalf("overview members = %#v", overview.Members)
	}
}

func hasProjectedMember(members []memberWire, participant ParticipantRef, name string) bool {
	for _, member := range members {
		ref, err := member.Participant.ref()
		if err == nil && ref == participant && member.DisplayName == name {
			return true
		}
	}
	return false
}

// The agent opens conversations through the same store calls the human
// sidebar uses: one participant reuses the single dm (idempotent, like
// EnsureDM behind POST /messaging/dms), several mint a group dm.
func TestLocalStartDMOpensTheSamePlacesAsTheHumanUI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	server := NewServer(world.store, nil)
	for _, participant := range []ParticipantRef{world.humanA, world.humanB} {
		if err := world.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
			t.Fatalf("admit %s: %v", participant.Key(), err)
		}
	}

	startDM := func(body string) (int, struct {
		DM      dmWire `json:"dm"`
		Created bool   `json:"created"`
	},
	) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, LocalStartDMPath, strings.NewReader(body)).
			WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.localStartDM(response, request, agentevents.LocalRuntimeAuthorization{
			PersonalityAgentID: world.agent.ID,
		})
		var decoded struct {
			DM      dmWire `json:"dm"`
			Created bool   `json:"created"`
		}
		if response.Code < 300 {
			if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
				t.Fatalf("decode start-dm: %v (%s)", err, response.Body.String())
			}
		}
		return response.Code, decoded
	}

	code, first := startDM(`{"participants":[{"kind":"human","human_id":"` + world.humanA.ID + `"}]}`)
	if code != http.StatusCreated || !first.Created || first.DM.Kind != PlaceDM {
		t.Fatalf("first start-dm = %d %#v", code, first)
	}
	// The pair has exactly one dm: asking again returns it rather than a second.
	code, again := startDM(`{"participants":[{"kind":"human","human_id":"` + world.humanA.ID + `"}]}`)
	if code != http.StatusOK || again.Created || again.DM.DMID != first.DM.DMID {
		t.Fatalf("repeat start-dm = %d %#v (first %s)", code, again, first.DM.DMID)
	}

	code, group := startDM(`{"participants":[
		{"kind":"human","human_id":"` + world.humanA.ID + `"},
		{"kind":"human","human_id":"` + world.humanB.ID + `"}]}`)
	if code != http.StatusCreated || group.DM.Kind != PlaceGroupDM {
		t.Fatalf("group start-dm = %d %#v", code, group)
	}
	// The place the agent just opened is one it can actually write in.
	if _, err := world.store.PlaceFor(ctx, group.DM.DMID, world.agent); err != nil {
		t.Fatalf("agent cannot see the group dm it opened: %v", err)
	}

	for _, body := range []string{
		`{"participants":[]}`,
		`{"participants":[{"kind":"app","human_id":"` + world.humanA.ID + `"}]}`,
	} {
		if code, _ := startDM(body); code != http.StatusBadRequest {
			t.Fatalf("start-dm %s status = %d, want 400", body, code)
		}
	}
}

// The agent's channel lifecycle is the human context menu's, through the same
// Store calls: create, rename/retopic, duplicate.
func TestLocalChannelLifecycleMatchesTheHumanMenu(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	server := NewServer(world.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: world.agent.ID}

	post := func(path, body string, handler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization)) (int, channelWire) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler(response, request, authorization)
		var decoded struct {
			Channel channelWire `json:"channel"`
		}
		if response.Code < 300 {
			if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
				t.Fatalf("decode %s: %v (%s)", path, err, response.Body.String())
			}
		}
		return response.Code, decoded.Channel
	}

	// workspace_id may be omitted: the agent is in exactly one workspace.
	code, created := post(LocalCreateChannelPath, `{"name":"設計","topic":"図面の相談"}`, server.localCreateChannel)
	if code != http.StatusCreated || created.Name != "設計" || created.WorkspaceID != DefaultWorkspaceID {
		t.Fatalf("create-channel = %d %#v", code, created)
	}

	code, renamed := post(LocalUpdateChannelPath,
		`{"place_id":"`+created.ChannelID+`","name":"設計と素材"}`, server.localUpdateChannel)
	if code != http.StatusOK || renamed.Name != "設計と素材" || renamed.Topic != "図面の相談" {
		t.Fatalf("update-channel = %d %#v, want the topic left alone", code, renamed)
	}

	code, copied := post(LocalDuplicateChannelPath,
		`{"place_id":"`+created.ChannelID+`"}`, server.localDuplicateChannel)
	if code != http.StatusCreated || copied.Name != "設計と素材 のコピー" ||
		copied.ChannelID == created.ChannelID {
		t.Fatalf("duplicate-channel = %d %#v", code, copied)
	}

	// An edit that names nothing is refused rather than silently succeeding.
	if code, _ := post(LocalUpdateChannelPath,
		`{"place_id":"`+created.ChannelID+`"}`, server.localUpdateChannel); code != http.StatusBadRequest {
		t.Fatalf("empty update status = %d, want 400", code)
	}
	if code, _ := post(LocalCreateChannelPath, `{"name":""}`, server.localCreateChannel); code != http.StatusBadRequest {
		t.Fatalf("empty name status = %d, want 400", code)
	}
}
