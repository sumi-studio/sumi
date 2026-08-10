package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

type transportTestSessions struct {
	claims         agentevents.UserSessionClaims
	authorizeCalls int
}

func (s *transportTestSessions) VerifySession(_ context.Context, cookie string) (agentevents.UserSessionClaims, error) {
	if cookie != "valid" {
		return agentevents.UserSessionClaims{}, errors.New("invalid session")
	}
	return s.claims, nil
}

func (s *transportTestSessions) AuthorizeSession(_ context.Context, _ agentevents.UserSessionClaims, operation func() error) error {
	s.authorizeCalls++
	return operation()
}

func TestHumanAndAgentTransportsConvergeOnWorkspaceOperations(t *testing.T) {
	w := newTestWorld(t)
	sessions := &transportTestSessions{
		claims: agentevents.UserSessionClaims{UserID: w.humanA.ID},
	}
	server := NewServer(w.store, applicationapps.New(w.pool, w.store), sessions)
	server.AllowedOrigins = []string{"https://sumi.test"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	humanCreated := browserWorkspaceMutation(t, mux, http.MethodPost, "/workspaces",
		`{"name":"Human home"}`, http.StatusCreated)
	agentCreated := localWorkspaceMutation(t, server, server.localCreateWorkspace,
		`{"name":"Agent home"}`, http.StatusCreated, w.agentA.ID)
	if humanCreated.Name != "Human home" || agentCreated.Name != "Agent home" {
		t.Fatalf("transport-created Workspaces = %#v / %#v", humanCreated, agentCreated)
	}
	assertOwnerParticipant(t, w, humanCreated, w.humanA)
	assertOwnerParticipant(t, w, agentCreated, w.agentA)

	// Neither transport accepts a caller-authored identity override. The actor
	// is the signed browser Human or PAID-bound local authorization only.
	response := browserCall(mux, http.MethodPost, "/workspaces",
		`{"name":"spoof","personality_agent_id":"`+w.agentA.ID+`"}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("browser actor override status = %d, body=%s", response.Code, response.Body.String())
	}
	localResponse := invokeLocal(server.localCreateWorkspace,
		`{"name":"spoof","human_id":"`+w.humanB.ID+`"}`, w.agentA.ID)
	if localResponse.Code != http.StatusBadRequest {
		t.Fatalf("local actor override status = %d, body=%s", localResponse.Code, localResponse.Body.String())
	}

	// Each owner can issue through its own transport and the other principal
	// can redeem through the other transport. Both land on the same base-only
	// Workspace membership operation.
	humanInviteResponse := browserCall(mux, http.MethodPost,
		"/workspaces/"+humanCreated.WorkspaceID+"/invites", `{}`)
	if humanInviteResponse.Code != http.StatusCreated {
		t.Fatalf("Human invite status = %d, body=%s", humanInviteResponse.Code, humanInviteResponse.Body.String())
	}
	var humanInvite inviteWire
	decodeRecorder(t, humanInviteResponse, &humanInvite)
	agentRedeem := invokeLocal(server.localRedeemInvite,
		fmt.Sprintf(`{"code":%q}`, humanInvite.Code), w.agentA.ID)
	if agentRedeem.Code != http.StatusOK {
		t.Fatalf("Agent redeem status = %d, body=%s", agentRedeem.Code, agentRedeem.Body.String())
	}
	var agentMembership membershipWire
	decodeRecorder(t, agentRedeem, &agentMembership)
	if agentMembership.Participant.PersonalityAgentID != w.agentA.ID || len(agentMembership.RoleIDs) != 0 {
		t.Fatalf("Agent redeemed membership = %#v", agentMembership)
	}

	agentInviteResponse := invokeLocal(server.localCreateInvite,
		fmt.Sprintf(`{"workspace_id":%q}`, agentCreated.WorkspaceID), w.agentA.ID)
	if agentInviteResponse.Code != http.StatusCreated {
		t.Fatalf("Agent invite status = %d, body=%s", agentInviteResponse.Code, agentInviteResponse.Body.String())
	}
	var agentInvite inviteWire
	decodeRecorder(t, agentInviteResponse, &agentInvite)
	humanRedeem := browserCall(mux, http.MethodPost, "/workspace-invites/redeem",
		fmt.Sprintf(`{"code":%q}`, agentInvite.Code))
	if humanRedeem.Code != http.StatusOK {
		t.Fatalf("Human redeem status = %d, body=%s", humanRedeem.Code, humanRedeem.Body.String())
	}
	var humanMembership membershipWire
	decodeRecorder(t, humanRedeem, &humanMembership)
	if humanMembership.Participant.HumanID != w.humanA.ID || len(humanMembership.RoleIDs) != 0 {
		t.Fatalf("Human redeemed membership = %#v", humanMembership)
	}

	if sessions.authorizeCalls != 3 { // create, invite creation, redemption
		t.Fatalf("browser mutation admission calls = %d", sessions.authorizeCalls)
	}
}

func TestHumanAndAgentTransportsConvergeOnRoleAndAppLifecycle(t *testing.T) {
	w := newTestWorld(t)
	sessions := &transportTestSessions{
		claims: agentevents.UserSessionClaims{UserID: w.humanA.ID},
	}
	appStore := applicationapps.New(w.pool, w.store)
	server := NewServer(w.store, appStore, sessions)
	server.AllowedOrigins = []string{"https://sumi.test"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	humanWorkspace := browserWorkspaceMutation(t, mux, http.MethodPost, "/workspaces",
		`{"name":"Human apps"}`, http.StatusCreated)
	agentWorkspace := localWorkspaceMutation(t, server, server.localCreateWorkspace,
		`{"name":"Agent apps"}`, http.StatusCreated, w.agentA.ID)

	roleResponse := invokeLocal(server.localCreateRole, fmt.Sprintf(
		`{"workspace_id":%q,"name":"Inviter","permissions":["manage_members"]}`,
		agentWorkspace.WorkspaceID), w.agentA.ID)
	if roleResponse.Code != http.StatusCreated {
		t.Fatalf("Agent role create status = %d, body=%s", roleResponse.Code, roleResponse.Body.String())
	}
	var role roleWire
	decodeRecorder(t, roleResponse, &role)
	if role.Name != "Inviter" || len(role.Permissions) != 1 || role.Permissions[0] != PermissionManageMembers {
		t.Fatalf("Agent-created role = %#v", role)
	}

	humanInstall := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging"}`,
		humanWorkspace.WorkspaceID))
	if humanInstall.Code != http.StatusCreated {
		t.Fatalf("Human app install status = %d, body=%s", humanInstall.Code, humanInstall.Body.String())
	}
	agentInstall := invokeLocal(server.localInstallApp, fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging"}`,
		agentWorkspace.WorkspaceID), w.agentA.ID)
	if agentInstall.Code != http.StatusCreated {
		t.Fatalf("Agent app install status = %d, body=%s", agentInstall.Code, agentInstall.Body.String())
	}
	installations := make(map[string]appInstallationWire, 2)
	for label, response := range map[string]*httptest.ResponseRecorder{
		"Human": humanInstall, "Agent": agentInstall,
	} {
		var installation appInstallationWire
		decodeRecorder(t, response, &installation)
		if installation.AppID != "messaging" || installation.State != string(applicationapps.StateEnabled) {
			t.Fatalf("%s installation = %#v", label, installation)
		}
		installations[label] = installation
	}
	humanDisable := browserCall(mux, http.MethodPut,
		"/app-installations/"+installations["Human"].InstallationID+"/state",
		`{"state":"disabled"}`)
	if humanDisable.Code != http.StatusOK {
		t.Fatalf("Human app disable status = %d, body=%s", humanDisable.Code, humanDisable.Body.String())
	}
	agentDisable := invokeLocal(server.localSetAppEnabled, fmt.Sprintf(
		`{"installation_id":%q,"enabled":false}`, installations["Agent"].InstallationID),
		w.agentA.ID)
	if agentDisable.Code != http.StatusOK {
		t.Fatalf("Agent app disable status = %d, body=%s", agentDisable.Code, agentDisable.Body.String())
	}
	humanUninstall := browserCall(mux, http.MethodDelete,
		"/app-installations/"+installations["Human"].InstallationID, "")
	if humanUninstall.Code != http.StatusNoContent {
		t.Fatalf("Human uninstall status = %d, body=%s", humanUninstall.Code, humanUninstall.Body.String())
	}
	agentUninstall := invokeLocal(server.localUninstallApp, fmt.Sprintf(
		`{"installation_id":%q}`, installations["Agent"].InstallationID), w.agentA.ID)
	if agentUninstall.Code != http.StatusNoContent {
		t.Fatalf("Agent uninstall status = %d, body=%s", agentUninstall.Code, agentUninstall.Body.String())
	}
	personalAgentInstall := invokeLocal(server.localInstallApp, fmt.Sprintf(
		`{"owner":{"kind":"participant","participant":{"kind":"personality_agent","personality_agent_id":%q}},"app_id":"alarm"}`,
		w.agentA.ID), w.agentA.ID)
	if personalAgentInstall.Code != http.StatusCreated {
		t.Fatalf("Agent participant app install status = %d, body=%s",
			personalAgentInstall.Code, personalAgentInstall.Body.String())
	}
	var personalInstallation appInstallationWire
	decodeRecorder(t, personalAgentInstall, &personalInstallation)
	if personalInstallation.Owner.Kind != string(applicationapps.OwnerParticipant) ||
		personalInstallation.Owner.Participant == nil ||
		personalInstallation.Owner.Participant.PersonalityAgentID != w.agentA.ID {
		t.Fatalf("nested Agent Participant owner wire = %#v", personalInstallation.Owner)
	}
}

type localWorkspaceHandler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization)

func localWorkspaceMutation(t *testing.T, server *Server, handler localWorkspaceHandler, body string, status int, agentID string) workspaceWire {
	t.Helper()
	response := invokeLocal(handler, body, agentID)
	if response.Code != status {
		t.Fatalf("local Workspace mutation status = %d, body=%s", response.Code, response.Body.String())
	}
	var item workspaceWire
	decodeRecorder(t, response, &item)
	return item
}

func browserWorkspaceMutation(t *testing.T, mux http.Handler, method, path, body string, status int) workspaceWire {
	t.Helper()
	response := browserCall(mux, method, path, body)
	if response.Code != status {
		t.Fatalf("browser Workspace mutation status = %d, body=%s", response.Code, response.Body.String())
	}
	var item workspaceWire
	decodeRecorder(t, response, &item)
	return item
}

func browserCall(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "https://sumi.test")
	request.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: "valid"})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func invokeLocal(handler localWorkspaceHandler, body, agentID string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler(response, request, agentevents.LocalRuntimeAuthorization{PersonalityAgentID: agentID})
	return response
}

func decodeRecorder(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func assertOwnerParticipant(t *testing.T, w testWorld, item workspaceWire, want participant.Ref) {
	t.Helper()
	members, err := w.store.Members(context.Background(), item.WorkspaceID, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || !members[0].Owner || members[0].Participant.Key() != want.Key() {
		t.Fatalf("owner membership for %s = %#v", want.Key(), members)
	}
}
