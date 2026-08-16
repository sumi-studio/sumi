package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func callLocalWithoutFixtureInference(
	t *testing.T,
	ctx context.Context,
	handler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization),
	path string,
	body map[string]any,
	authorization agentevents.LocalRuntimeAuthorization,
) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler(response, request, authorization)
	var decoded map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &decoded)
	return response.Code, decoded
}

func TestLocalWriteBoundaryThroughAuthenticatedUnixControlRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	messagingServer := NewServer(w.store.core, nil)

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(privateRuntimeDir(t), commandStore)
	if err != nil {
		t.Fatalf("open durable gateway: %v", err)
	}
	const bearer = "messaging-write-boundary-bearer-generation-one"
	control, err := agentevents.NewLocalControlServer(
		gateway,
		[]byte("messaging-write-boundary-signing-secret"),
		[]agentevents.LocalRuntimeAuthorization{{
			BearerToken: bearer, TenantID: "messaging-write-boundary",
			PersonalityAgentID: w.agent.ID, Generation: 1,
			RPCBootNonce:          "messaging-write-boundary-boot-1",
			Audience:              agentevents.DefaultAgentAudience(),
			DeliveryAuthorization: agentevents.LocalDeliveryRaw,
		}},
	)
	if err != nil {
		t.Fatalf("new local control: %v", err)
	}
	if err := messagingServer.RegisterLocalControlRoutes(control); err != nil {
		t.Fatalf("register messaging routes: %v", err)
	}
	handler, err := control.HandlerForLocalRuntime(w.agent.ID)
	if err != nil {
		t.Fatalf("bind local runtime handler: %v", err)
	}
	socketPath := filepath.Join(t.TempDir(), "local-control.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on local-control Unix socket: %v", err)
	}
	httpServer := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	client := &http.Client{Transport: transport}
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		if err := httpServer.Close(); err != nil {
			t.Errorf("close Unix local-control server: %v", err)
		}
		if err := <-serveDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve Unix local-control route: %v", err)
		}
	})

	type durableState struct {
		messages int64
		lastSeq  int64
		intents  int64
	}
	snapshot := func() durableState {
		t.Helper()
		var state durableState
		if err := w.store.core.pool.QueryRow(ctx, `
			SELECT count(*) FROM messages
			WHERE workspace_id=$1 AND place_id=$2`,
			workspace.WorkspaceID, channel.PlaceID).Scan(&state.messages); err != nil {
			t.Fatalf("count messages: %v", err)
		}
		if err := w.store.core.pool.QueryRow(ctx, `
			SELECT last_seq FROM places
			WHERE workspace_id=$1 AND place_id=$2`,
			workspace.WorkspaceID, channel.PlaceID).Scan(&state.lastSeq); err != nil {
			t.Fatalf("read place sequence: %v", err)
		}
		if err := w.store.core.pool.QueryRow(ctx, `
			SELECT count(*) FROM message_notification_intents i
			JOIN messages m ON m.message_id=i.message_id
			WHERE m.workspace_id=$1 AND m.place_id=$2`,
			workspace.WorkspaceID, channel.PlaceID).Scan(&state.intents); err != nil {
			t.Fatalf("count notification intents: %v", err)
		}
		return state
	}

	type postResult struct {
		status  int
		body    map[string]any
		raw     []byte
		request []byte
	}
	post := func(content, nonce, authorization string) postResult {
		t.Helper()
		payload, err := json.Marshal(map[string]any{
			"workspace_id": scoped.Scope.WorkspaceID, "installation_id": scoped.Scope.InstallationID,
			"authority_epoch": strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10),
			"place_id":        channel.PlaceID, "content": content, "urgency": "normal", "client_nonce": nonce,
		})
		if err != nil {
			t.Fatalf("marshal local write: %v", err)
		}
		request, err := http.NewRequestWithContext(
			ctx, http.MethodPost, "http://local-control.invalid"+LocalWritePath, bytes.NewReader(payload),
		)
		if err != nil {
			t.Fatalf("new local write request: %v", err)
		}
		request.Header.Set("Content-Type", "application/json")
		if authorization != "" {
			request.Header.Set("Authorization", "Bearer "+authorization)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("post local write: %v", err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read local write response: %v", err)
		}
		body := map[string]any{}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("decode local write response %q: %v", raw, err)
		}
		return postResult{status: response.StatusCode, body: body, raw: raw, request: payload}
	}

	initial := snapshot()
	unauthorized := post("must not commit", "nonce-unauthorized", "wrong-bearer-token-with-32-bytes")
	if unauthorized.status != http.StatusUnauthorized {
		t.Fatalf("wrong bearer write = %d %v, want 401", unauthorized.status, unauthorized.body)
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

	// Both content and nonce use the legal byte whose JSON representation has
	// the largest six-byte escape. The request is much larger than 64 KiB, but
	// its receipt must stay inside the unchanged generic response bound.
	maxContent := strings.Repeat("\x01", MaxContentBytes)
	worstEscapedNonce := strings.Repeat("\x01", 126) + "\"\\"
	if len(worstEscapedNonce) != 128 {
		t.Fatalf("worst escaped nonce = %d bytes, want 128", len(worstEscapedNonce))
	}
	created := post(maxContent, worstEscapedNonce, bearer)
	if created.status != http.StatusCreated || created.body["created"] != true ||
		created.body["client_nonce"] != worstEscapedNonce || len(created.body) != 4 ||
		created.body["message"] != nil {
		t.Fatalf("max escaped write receipt = %d %v", created.status, created.body)
	}
	if len(created.request) <= 2*MaxContentBytes || len(created.request) > maxRequestBytes {
		t.Fatalf("escaped request = %d bytes, cap %d", len(created.request), maxRequestBytes)
	}
	if len(created.raw) >= 1024 ||
		!bytes.Contains(created.raw, []byte(`\u0001`)) ||
		!bytes.Contains(created.raw, []byte(`\"\\`)) {
		t.Fatalf("receipt is not compact or escaped correctly: %d bytes %q", len(created.raw), created.raw)
	}

	messageID, messageIDOK := created.body["message_id"].(string)
	seq, seqOK := created.body["seq"].(float64)
	if !messageIDOK || messageID == "" || !seqOK || seq != 1 {
		t.Fatalf("first receipt identity = message_id %#v seq %#v", created.body["message_id"], created.body["seq"])
	}
	createdState := snapshot()
	if createdState.messages != 1 || createdState.lastSeq != 1 || createdState.intents == 0 {
		t.Fatalf("first durable write state = %#v, want one message at seq 1 with intents", createdState)
	}
	replayed := post(maxContent, worstEscapedNonce, bearer)
	if replayed.status != http.StatusOK || replayed.body["created"] != false ||
		replayed.body["message_id"] != messageID || replayed.body["seq"] != seq ||
		replayed.body["client_nonce"] != worstEscapedNonce || len(replayed.body) != 4 {
		t.Fatalf("same-nonce replay = %d %v, first %v", replayed.status, replayed.body, created.body)
	}
	if got := snapshot(); got != createdState {
		t.Fatalf("same-nonce replay mutated durable state: before %#v after %#v", createdState, got)
	}

	fresh := post("fresh write", "nonce-new-call", bearer)
	if fresh.status != http.StatusCreated || fresh.body["created"] != true ||
		fresh.body["message_id"] == messageID || fresh.body["seq"] == seq {
		t.Fatalf("new-nonce write = %d %v, first %v", fresh.status, fresh.body, created.body)
	}
	final := snapshot()
	if final.messages != 2 || final.lastSeq != 2 || final.intents <= createdState.intents {
		t.Fatalf("fresh durable write state = %#v, first %#v", final, createdState)
	}
}

func TestLocalControlRequiresAuthenticatedExactScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// Use an unregistered ID so the test helper cannot inject fixture scope.
	missingAuth := agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: "01900000-0000-7000-8000-000000000099",
	}
	status, _ := callLocal(t, ctx, server.localOverview, LocalOverviewPath, map[string]any{}, missingAuth)
	if status != http.StatusBadRequest {
		t.Fatalf("local control missing exact scope=%d, want 400", status)
	}
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	status, body := callLocal(t, ctx, server.localOverview, LocalOverviewPath, map[string]any{
		"workspace_id": scoped.Scope.WorkspaceID, "installation_id": scoped.Scope.InstallationID,
		"authority_epoch": strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10),
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("local control exact scope=%d body=%v", status, body)
	}
}

func TestLocalExactCallStateReconcilesAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "agent-call-state", w.agent)
	owner := fixture.scope(t, w, w.humanA)
	channel, err := owner.CreateChannel(ctx, "通話", "", true)
	if err != nil {
		t.Fatal(err)
	}
	scoped := fixture.scope(t, w, w.agent)
	server := NewServer(w.store.core, nil)
	calls := NewCallService(server, testLiveKit())
	calls.RoomService = stubLiveKitRoomService{
		rooms: []liveKitRoom{{Name: channel.PlaceID, CreatedAt: time.Now().Unix()}},
		participants: map[string][]liveKitParticipant{
			channel.PlaceID: {{Identity: w.humanA.Key(), JoinedAt: time.Now().Unix()}},
		},
	}
	status, body := callLocalWithoutFixtureInference(t, ctx, calls.localCallState, LocalCallStatePath, map[string]any{
		"workspace_id": scoped.Scope.WorkspaceID, "installation_id": scoped.Scope.InstallationID,
		"authority_epoch": strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10), "place_id": channel.PlaceID,
	}, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID})
	if status != http.StatusOK {
		t.Fatalf("exact call state status=%d body=%v", status, body)
	}
	callsWire, ok := body["calls"].([]any)
	if !ok || len(callsWire) != 1 || callsWire[0].(map[string]any)["active"] != true {
		t.Fatalf("exact call state did not reconcile: %v", body)
	}
}

func TestLocalWriteReauthorizesASealedInstallationAtCommit(t *testing.T) {
	for _, test := range []struct {
		name       string
		wantStatus int
		wantError  string
		retire     func(context.Context, world, *ScopedStore) error
	}{
		{
			name: "disabled after bind", wantStatus: http.StatusNotFound, wantError: "installation_not_found",
			retire: func(ctx context.Context, w world, scoped *ScopedStore) error {
				_, err := w.apps.SetEnabledByID(ctx, scoped.Scope.InstallationID, w.humanA, false)
				return err
			},
		},
		{
			name: "uninstalled after bind", wantStatus: http.StatusNotFound, wantError: "installation_not_found",
			retire: func(ctx context.Context, w world, scoped *ScopedStore) error {
				return w.apps.UninstallByID(ctx, scoped.Scope.InstallationID, w.humanA)
			},
		},
		{
			name: "disabled and re-enabled after bind", wantStatus: http.StatusNotFound, wantError: "installation_not_found",
			retire: func(ctx context.Context, w world, scoped *ScopedStore) error {
				if _, err := w.apps.SetEnabledByID(
					ctx, scoped.Scope.InstallationID, w.humanA, false,
				); err != nil {
					return err
				}
				_, err := w.apps.SetEnabledByID(
					ctx, scoped.Scope.InstallationID, w.humanA, true,
				)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			workspace, channel := w.workspaceWithChannel(t, ctx)
			scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
			sealedWorkspaceID := scoped.Scope.WorkspaceID
			sealedInstallationID := scoped.Scope.InstallationID
			sealedAuthorityEpoch := scoped.Scope.AuthorityEpoch
			if err := test.retire(ctx, w, scoped); err != nil {
				t.Fatal(err)
			}
			server := NewServer(w.store.core, nil)
			status, body := callLocalWithoutFixtureInference(t, ctx, server.localWrite, LocalWritePath, map[string]any{
				"workspace_id": sealedWorkspaceID, "installation_id": sealedInstallationID,
				"authority_epoch": strconv.FormatInt(sealedAuthorityEpoch, 10),
				"place_id":        channel.PlaceID, "content": "must not commit", "urgency": "normal",
				"client_nonce": "stale-exact-installation",
			}, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID})
			if status != test.wantStatus || body["error"] != test.wantError {
				t.Fatalf("stale exact write = %d %v, want %d %s",
					status, body, test.wantStatus, test.wantError)
			}
			var messages int
			if err := w.store.core.pool.QueryRow(ctx,
				"SELECT count(*) FROM messages WHERE workspace_id=$1 AND place_id=$2",
				workspace.WorkspaceID, channel.PlaceID).Scan(&messages); err != nil {
				t.Fatal(err)
			}
			if messages != 0 {
				t.Fatalf("stale exact scope committed %d messages", messages)
			}
		})
	}
}

func TestLocalAuthorityEpochWireIsCanonical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	prefix := `{"workspace_id":"` + scoped.Scope.WorkspaceID +
		`","installation_id":"` + scoped.Scope.InstallationID + `"`

	invoke := func(raw string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(
			http.MethodPost, LocalOverviewPath, strings.NewReader(raw),
		).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.localOverview(response, request, authorization)
		return response
	}
	for name, raw := range map[string]string{
		"missing epoch":          prefix + `}`,
		"missing workspace":      `{"installation_id":"` + scoped.Scope.InstallationID + `","authority_epoch":"1"}`,
		"missing installation":   `{"workspace_id":"` + scoped.Scope.WorkspaceID + `","authority_epoch":"1"}`,
		"duplicate workspace":    `{"workspace_id":"shadow","workspace_id":"` + scoped.Scope.WorkspaceID + `","installation_id":"` + scoped.Scope.InstallationID + `","authority_epoch":"1"}`,
		"duplicate installation": `{"workspace_id":"` + scoped.Scope.WorkspaceID + `","installation_id":"shadow","installation_id":"` + scoped.Scope.InstallationID + `","authority_epoch":"1"}`,
		"null workspace":         `{"workspace_id":null,"installation_id":"` + scoped.Scope.InstallationID + `","authority_epoch":"1"}`,
		"number installation":    `{"workspace_id":"` + scoped.Scope.WorkspaceID + `","installation_id":1,"authority_epoch":"1"}`,
		"duplicate epoch":        prefix + `,"authority_epoch":"1","authority_epoch":"1"}`,
		"null":                   prefix + `,"authority_epoch":null}`,
		"empty":                  prefix + `,"authority_epoch":""}`,
		"plus":                   prefix + `,"authority_epoch":"+1"}`,
		"leading zero":           prefix + `,"authority_epoch":"01"}`,
		"zero":                   prefix + `,"authority_epoch":"0"}`,
		"overflow":               prefix + `,"authority_epoch":"9223372036854775808"}`,
		"number":                 prefix + `,"authority_epoch":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := invoke(raw)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s, want 400", response.Code, response.Body.String())
			}
		})
	}
	response := invoke(prefix + `,"authority_epoch":"1"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("canonical epoch = %d body=%s", response.Code, response.Body.String())
	}
}
