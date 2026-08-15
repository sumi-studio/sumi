package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestLocalWritePreservesBoundedReceiptUnderExactScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)

	post := func(content, nonce string) (int, map[string]any) {
		t.Helper()
		return callLocal(t, ctx, server.localWrite, LocalWritePath, map[string]any{
			"workspace_id": scoped.Scope.WorkspaceID, "installation_id": scoped.Scope.InstallationID,
			"place_id": channel.PlaceID, "content": content, "urgency": "normal", "client_nonce": nonce,
		}, authorization)
	}
	for name, content := range map[string]string{
		"over-limit": strings.Repeat("x", MaxContentBytes+1),
		"nul":        strings.Repeat("\x00", MaxContentBytes),
	} {
		status, body := post(content, "invalid-"+name)
		if status != http.StatusBadRequest || body["error"] != "invalid_content" {
			t.Fatalf("%s write = %d %v, want 400 invalid_content", name, status, body)
		}
	}

	const quotedNonce = "nonce-\"quoted\"-\\slash"
	maxContent := "@Yohaku " + strings.Repeat("a", MaxContentBytes-len("@Yohaku "))
	status, created := post(maxContent, quotedNonce)
	if status != http.StatusCreated || created["created"] != true ||
		created["client_nonce"] != quotedNonce || len(created) != 4 || created["message"] != nil {
		t.Fatalf("max write receipt = %d %v", status, created)
	}
	createdJSON, err := json.Marshal(created)
	if err != nil {
		t.Fatal(err)
	}
	if len(createdJSON) >= 1024 || !bytes.Contains(createdJSON, []byte(`\"quoted\"-\\slash`)) {
		t.Fatalf("receipt is not compact or escaped correctly: %d bytes %q", len(createdJSON), createdJSON)
	}

	messageID := created["message_id"]
	seq := created["seq"]
	status, replayed := post(maxContent, quotedNonce)
	if status != http.StatusOK || replayed["created"] != false ||
		replayed["message_id"] != messageID || replayed["seq"] != seq ||
		replayed["client_nonce"] != quotedNonce {
		t.Fatalf("same-nonce replay = %d %v, first %v", status, replayed, created)
	}

	status, fresh := post(maxContent, "nonce-new-call")
	if status != http.StatusCreated || fresh["created"] != true ||
		fresh["message_id"] == messageID || fresh["seq"] == seq {
		t.Fatalf("new-nonce write = %d %v, first %v", status, fresh, created)
	}
	var messages, intents int
	if err := w.store.core.pool.QueryRow(ctx,
		"SELECT count(*) FROM messages WHERE workspace_id=$1 AND place_id=$2",
		workspace.WorkspaceID, channel.PlaceID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT count(*) FROM message_notification_intents i
		JOIN messages m ON m.message_id=i.message_id
		WHERE m.workspace_id=$1 AND m.place_id=$2`, workspace.WorkspaceID, channel.PlaceID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if messages != 2 || intents == 0 {
		t.Fatalf("durable writes/intents = %d/%d, want two messages and transactional intents", messages, intents)
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
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("local control exact scope=%d body=%v", status, body)
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
			name: "disabled after bind", wantStatus: http.StatusForbidden, wantError: "app_disabled",
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
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			workspace, channel := w.workspaceWithChannel(t, ctx)
			scoped := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.agent)
			sealedWorkspaceID := scoped.Scope.WorkspaceID
			sealedInstallationID := scoped.Scope.InstallationID
			if err := test.retire(ctx, w, scoped); err != nil {
				t.Fatal(err)
			}
			server := NewServer(w.store.core, nil)
			status, body := callLocalWithoutFixtureInference(t, ctx, server.localWrite, LocalWritePath, map[string]any{
				"workspace_id": sealedWorkspaceID, "installation_id": sealedInstallationID,
				"place_id": channel.PlaceID, "content": "must not commit", "urgency": "normal",
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
