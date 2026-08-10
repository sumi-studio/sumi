package messaging

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const localMessagingTestBearer = "messaging-local-control-bearer-32-bytes-minimum"

func localMessagingAuthorization(bearer, personalityAgentID string, generation uint64, bootNonce string) agentevents.LocalRuntimeAuthorization {
	return agentevents.LocalRuntimeAuthorization{
		BearerToken:           bearer,
		TenantID:              "tenant-messaging-test",
		PersonalityAgentID:    personalityAgentID,
		Generation:            generation,
		RPCBootNonce:          bootNonce,
		Audience:              "sumi:agent:events",
		DeliveryAuthorization: agentevents.LocalDeliveryRaw,
	}
}

func newAuthorizedAttachmentLocalControlServer(t *testing.T, ctx context.Context) (world, Place, *Server, *httptest.Server) {
	t.Helper()
	w := newWorld(t, ctx)
	_, place := w.workspaceWithChannel(t, ctx)
	blobs, err := NewDiskAttachments(t.TempDir())
	if err != nil {
		t.Fatalf("disk attachments: %v", err)
	}
	messagingServer := NewServer(w.store, nil)
	messagingServer.Attachments = blobs

	commands, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open local control command store: %v", err)
	}
	t.Cleanup(func() { _ = commands.Close() })
	gateway, err := agentevents.OpenDurableGateway(t.TempDir(), commands)
	if err != nil {
		t.Fatalf("open local control gateway: %v", err)
	}
	control, err := agentevents.NewLocalControlServer(
		gateway,
		[]byte("messaging-local-control-signing-secret-32-bytes-minimum"),
		[]agentevents.LocalRuntimeAuthorization{
			localMessagingAuthorization(localMessagingTestBearer, w.agent.ID, 1, "messaging-test-boot"),
		},
	)
	if err != nil {
		t.Fatalf("new local control server: %v", err)
	}
	if err := messagingServer.RegisterLocalControlRoutes(control); err != nil {
		t.Fatalf("register messaging local control routes: %v", err)
	}
	handler, err := control.HandlerForLocalRuntime(w.agent.ID)
	if err != nil {
		t.Fatalf("bind local control handler: %v", err)
	}
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return w, place, messagingServer, ts
}

func postLocalMultipart(t *testing.T, ctx context.Context, ts *httptest.Server, bearer, filename string, payload []byte) (*http.Response, []byte) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
	header.Set("Content-Type", "application/octet-stream")
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatalf("create local upload part: %v", err)
	}
	if _, err := part.Write(payload); err != nil {
		t.Fatalf("write local upload part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close local upload body: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+LocalUploadAttachmentPath, &body)
	if err != nil {
		t.Fatalf("new local upload request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := ts.Client().Do(request)
	if err != nil {
		t.Fatalf("local upload request: %v", err)
	}
	return response, readLocalResponse(t, response)
}

func postLocalJSON(t *testing.T, ctx context.Context, ts *httptest.Server, path string, value any) (*http.Response, []byte) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal local request: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new local request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+localMessagingTestBearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := ts.Client().Do(request)
	if err != nil {
		t.Fatalf("local request: %v", err)
	}
	return response, readLocalResponse(t, response)
}

func readLocalResponse(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read local response: %v", err)
	}
	return raw
}

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

func TestAgentUploadsAndSendsAttachmentThroughPAIDBoundControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, place, _, ts := newAuthorizedAttachmentLocalControlServer(t, ctx)
	prior := w.send(t, ctx, place.PlaceID, w.humanA, "before attachment")
	payload := append([]byte("\x89PNG\r\n\x1a\n"), []byte{0, 1, 2, 0xff, 0, 3}...)

	response, raw := postLocalMultipart(
		t, ctx, ts, "wrong-local-control-bearer-32-bytes-minimum", "wrong.png", payload,
	)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized local upload = %d %s, want 401", response.StatusCode, raw)
	}

	response, raw = postLocalMultipart(
		t, ctx, ts, localMessagingTestBearer, "../../screen.png", payload,
	)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("local upload = %d %s, want 201", response.StatusCode, raw)
	}
	var uploaded attachmentWire
	if err := json.Unmarshal(raw, &uploaded); err != nil {
		t.Fatalf("decode local upload: %v", err)
	}
	if uploaded.AttachmentID == "" || uploaded.Filename != "screen.png" ||
		uploaded.MIME != "image/png" || uploaded.Size != int64(len(payload)) {
		t.Fatalf("local upload = %#v", uploaded)
	}

	response, raw = postLocalJSON(t, ctx, ts, LocalWritePath, map[string]any{
		"place_id":     place.PlaceID,
		"content":      "",
		"urgency":      UrgencyNormal,
		"client_nonce": "agent-attachment-1",
		"attachments":  []string{uploaded.AttachmentID},
	})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("local attachment-only write = %d %s, want 201", response.StatusCode, raw)
	}
	var receipt messageReceiptWire
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatalf("decode local write receipt: %v", err)
	}
	var receiptFields map[string]any
	if err := json.Unmarshal(raw, &receiptFields); err != nil {
		t.Fatalf("decode local write receipt fields: %v", err)
	}
	if receipt.MessageID == "" || receipt.Seq != prior.Seq+1 || !receipt.Created ||
		receipt.ClientNonce != "agent-attachment-1" || len(receiptFields) != 4 {
		t.Fatalf("compact local write receipt = %s", raw)
	}
	if _, hasMessage := receiptFields["message"]; hasMessage {
		t.Fatalf("local write returned a full message: %s", raw)
	}

	history, err := w.store.History(ctx, place.PlaceID, w.agent, HistoryOptions{Limit: 20})
	if err != nil || len(history) != 2 {
		t.Fatalf("stored local write history = %#v, error %v", history, err)
	}
	written := history[1]
	if written.MessageID != receipt.MessageID || written.Author != w.agent ||
		len(written.Attachments) != 1 || written.Attachments[0].AttachmentID != uploaded.AttachmentID ||
		written.Attachments[0].Uploader != w.agent {
		t.Fatalf("stored local attachment message = %#v", written)
	}

	response, raw = postLocalJSON(t, ctx, ts, LocalAttachmentPath, map[string]any{
		"attachment_id": uploaded.AttachmentID,
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("local attachment fetch = %d %s, want 200", response.StatusCode, raw)
	}
	var fetched struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(raw, &fetched); err != nil {
		t.Fatalf("decode local attachment fetch: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(fetched.Data)
	if err != nil || !bytes.Equal(decoded, payload) {
		t.Fatalf("local attachment bytes = %v, decode error %v", decoded, err)
	}
}

func localAttachmentFetch(t *testing.T, ctx context.Context, server *Server, agentID, attachmentID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, LocalAttachmentPath,
		strings.NewReader(`{"attachment_id":"`+attachmentID+`"}`)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.localAttachment(response, request, agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: agentID,
	})
	var decoded map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &decoded)
	return response, decoded
}

func TestLocalAttachmentAppliesVisibilityTombstonesAndInlineBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, server, ts := newAttachmentServerWithServer(t, ctx)
	_, shared := w.workspaceWithChannel(t, ctx)

	private, err := w.store.CreateWorkspace(ctx, "private", w.humanA)
	if err != nil {
		t.Fatalf("create private workspace: %v", err)
	}
	privateChannel, err := w.store.CreateChannel(ctx, private.WorkspaceID, "solo", "", w.humanA, false)
	if err != nil {
		t.Fatalf("create private channel: %v", err)
	}

	_, body := upload(t, ts, w.humanA.ID, "shot.png", "image/png", pngBytes)
	visibleID := attachmentID(t, body)
	resp, receipt := call(t, ts, http.MethodPost,
		"/messaging/places/"+shared.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "見て", "client_nonce": "n-see", "attachments": []string{visibleID}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send visible = %d %v, want 201", resp.StatusCode, receipt)
	}
	visibleMessageID, _ := receipt["message_id"].(string)

	response, fetched := localAttachmentFetch(t, ctx, server, w.agent.ID, visibleID)
	if response.Code != http.StatusOK {
		t.Fatalf("agent fetch visible attachment = %d %s", response.Code, response.Body.String())
	}
	encoded, _ := fetched["data"].(string)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || !bytes.Equal(decoded, pngBytes) {
		t.Fatalf("visible local attachment bytes = %v, error %v", decoded, err)
	}

	_, body = upload(t, ts, w.humanA.ID, "secret.png", "image/png", pngBytes)
	hiddenID := attachmentID(t, body)
	resp, receipt = call(t, ts, http.MethodPost,
		"/messaging/places/"+privateChannel.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "内緒", "client_nonce": "n-hide", "attachments": []string{hiddenID}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send hidden = %d %v, want 201", resp.StatusCode, receipt)
	}
	if response, _ := localAttachmentFetch(t, ctx, server, w.agent.ID, hiddenID); response.Code != http.StatusNotFound {
		t.Fatalf("agent fetch hidden attachment = %d, want 404", response.Code)
	}

	_, body = upload(t, ts, w.humanA.ID, "draft.png", "image/png", pngBytes)
	if response, _ := localAttachmentFetch(t, ctx, server, w.agent.ID, attachmentID(t, body)); response.Code != http.StatusNotFound {
		t.Fatalf("agent fetch another participant's unbound upload = %d, want 404", response.Code)
	}

	big := bytes.Repeat([]byte("x"), int(MaxLocalAttachmentFetchBytes)+1)
	_, body = upload(t, ts, w.humanA.ID, "dump.bin", "application/octet-stream", big)
	bigID := attachmentID(t, body)
	resp, receipt = call(t, ts, http.MethodPost,
		"/messaging/places/"+shared.PlaceID+"/messages", w.humanA.ID,
		map[string]any{"content": "log", "client_nonce": "n-big", "attachments": []string{bigID}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("send large attachment = %d %v, want 201", resp.StatusCode, receipt)
	}
	response, fetched = localAttachmentFetch(t, ctx, server, w.agent.ID, bigID)
	if response.Code != http.StatusRequestEntityTooLarge || fetched["error"] != "attachment_too_large" || fetched["data"] != nil {
		t.Fatalf("large local fetch = %d %v, want 413 without truncated data", response.Code, fetched)
	}

	resp, receipt = call(t, ts, http.MethodDelete,
		"/messaging/places/"+shared.PlaceID+"/messages/"+visibleMessageID, w.humanA.ID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete visible message = %d %v, want 204", resp.StatusCode, receipt)
	}
	if response, _ := localAttachmentFetch(t, ctx, server, w.agent.ID, visibleID); response.Code != http.StatusNotFound {
		t.Fatalf("agent fetch tombstoned attachment = %d, want 404", response.Code)
	}
}

func TestLocalWriteUsesSharedAttachmentValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, place, _, ts := newAuthorizedAttachmentLocalControlServer(t, ctx)
	for _, test := range []struct {
		name      string
		request   map[string]any
		wantError string
	}{
		{"empty", map[string]any{"content": "", "client_nonce": "empty"}, "invalid_content"},
		{"urgency", map[string]any{"content": "hi", "urgency": "emergency", "client_nonce": "urgency"}, "invalid_urgency"},
		{"too many", map[string]any{"content": "files", "client_nonce": "many", "attachments": make([]string, MaxAttachmentsPerMessage+1)}, "too_many_attachments"},
		{"nonce", map[string]any{"content": "hi", "client_nonce": strings.Repeat("n", 129)}, "invalid_client_nonce"},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.request["place_id"] = place.PlaceID
			response, raw := postLocalJSON(t, ctx, ts, LocalWritePath, test.request)
			var failure map[string]string
			_ = json.Unmarshal(raw, &failure)
			if response.StatusCode != http.StatusBadRequest || failure["error"] != test.wantError {
				t.Fatalf("local write = %d %s, want 400 %s", response.StatusCode, raw, test.wantError)
			}
		})
	}
}
