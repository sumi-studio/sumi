package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/testfs"
)

type transportTestSessions struct {
	claims         agentevents.UserSessionClaims
	verifyCalls    int
	authorizeCalls int
	denyMutation   bool
}

func TestWorkspaceDomainErrorsExposeCanonicalCodes(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"authority containment", ErrForbidden, http.StatusForbidden, "forbidden"},
		{"owner protected", ErrOwnerProtected, http.StatusForbidden, "owner_protected"},
		{"closed membership tenure", ErrMemberNotFound, http.StatusNotFound, "membership_not_active"},
		{"last administrator", ErrLastAdministrator, http.StatusConflict, "last_administrator"},
		{"generic conflict", ErrRoleNameTaken, http.StatusConflict, "conflict"},
		{"invalid Workspace list cursor", ErrInvalidWorkspaceListCursor, http.StatusBadRequest, "invalid_request"},
		{"install intent existing", applicationapps.ErrInstallIntentAlreadyInstalled, http.StatusConflict, "install_intent_already_installed"},
		{"install intent mismatch", applicationapps.ErrInstallIntentMismatch, http.StatusConflict, "idempotency_conflict"},
		{"install intent incomplete", applicationapps.ErrInstallIntentIncomplete, http.StatusServiceUnavailable, "unavailable"},
		{"invalid install operation", applicationapps.ErrInstallOperationInvalid, http.StatusBadRequest, "invalid_request"},
		{"stale app authority", applicationapps.ErrAuthorityEpochStale, http.StatusConflict, "stale_authority"},
		{"direct-chat lifecycle unavailable", directchat.ErrLifecycleFenceUnavailable, http.StatusServiceUnavailable, "unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeDomainError(response, test.err)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			var body struct {
				Error string `json:"error"`
			}
			decodeRecorder(t, response, &body)
			if body.Error != test.code {
				t.Fatalf("error = %q, want %q", body.Error, test.code)
			}
		})
	}
}

func (s *transportTestSessions) VerifySession(_ context.Context, cookie string) (agentevents.UserSessionClaims, error) {
	s.verifyCalls++
	if cookie != "valid" {
		return agentevents.UserSessionClaims{}, errors.New("invalid session")
	}
	return s.claims, nil
}

func (s *transportTestSessions) AuthorizeSession(_ context.Context, _ agentevents.UserSessionClaims, operation func() error) error {
	s.authorizeCalls++
	if s.denyMutation {
		return nil
	}
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
	humanInviteList := browserCall(mux, http.MethodGet,
		"/workspaces/"+humanCreated.WorkspaceID+"/invites", "")
	if humanInviteList.Code != http.StatusOK || strings.Contains(humanInviteList.Body.String(), humanInvite.Code) {
		t.Fatalf("Human invite list status=%d body=%s", humanInviteList.Code, humanInviteList.Body.String())
	}
	var humanListed struct {
		Invites []inviteRecordWire `json:"invites"`
	}
	decodeRecorder(t, humanInviteList, &humanListed)
	if len(humanListed.Invites) != 1 || humanListed.Invites[0].InviteID != humanInvite.InviteID {
		t.Fatalf("Human invite list = %#v", humanListed.Invites)
	}
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
	agentInviteList := invokeLocal(server.localInvites,
		fmt.Sprintf(`{"workspace_id":%q}`, agentCreated.WorkspaceID), w.agentA.ID)
	if agentInviteList.Code != http.StatusOK || strings.Contains(agentInviteList.Body.String(), agentInvite.Code) {
		t.Fatalf("Agent invite list status=%d body=%s", agentInviteList.Code, agentInviteList.Body.String())
	}
	var agentListed struct {
		Invites []inviteRecordWire `json:"invites"`
	}
	decodeRecorder(t, agentInviteList, &agentListed)
	if len(agentListed.Invites) != 1 || agentListed.Invites[0].InviteID != agentInvite.InviteID {
		t.Fatalf("Agent invite list = %#v", agentListed.Invites)
	}
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

	// Ownership is the same exact-tenure operation through both transports.
	// The browser Human can hand its Workspace to a PersonalityAgent member;
	// the local PersonalityAgent can hand its Workspace to a Human member.
	humanTransfer := browserCall(mux, http.MethodPut,
		"/workspaces/"+humanCreated.WorkspaceID+"/owner",
		fmt.Sprintf(`{"workspace_member_id":%q}`, agentMembership.WorkspaceMemberID))
	if humanTransfer.Code != http.StatusOK {
		t.Fatalf("Human owner transfer status = %d, body=%s",
			humanTransfer.Code, humanTransfer.Body.String())
	}
	var humanTransferred workspaceWire
	decodeRecorder(t, humanTransfer, &humanTransferred)
	if humanTransferred.OwnerWorkspaceMemberID != agentMembership.WorkspaceMemberID {
		t.Fatalf("Human owner transfer = %#v", humanTransferred)
	}

	agentTransfer := invokeLocal(server.localTransferWorkspaceOwnership,
		fmt.Sprintf(`{"workspace_id":%q,"workspace_member_id":%q}`,
			agentCreated.WorkspaceID, humanMembership.WorkspaceMemberID), w.agentA.ID)
	if agentTransfer.Code != http.StatusOK {
		t.Fatalf("Agent owner transfer status = %d, body=%s",
			agentTransfer.Code, agentTransfer.Body.String())
	}
	var agentTransferred workspaceWire
	decodeRecorder(t, agentTransfer, &agentTransferred)
	if agentTransferred.OwnerWorkspaceMemberID != humanMembership.WorkspaceMemberID {
		t.Fatalf("Agent owner transfer = %#v", agentTransferred)
	}

	if sessions.authorizeCalls != 4 { // create, invite creation, redemption, owner transfer
		t.Fatalf("browser mutation admission calls = %d", sessions.authorizeCalls)
	}
}

func TestCurrentAgentInviteBrowserResourceDerivesTargetAndNeverExposesIt(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	lifecycle := directchat.NewLifecycleFence()
	authority := koseki.New(w.pool, lifecycle)
	sessions := &transportTestSessions{claims: agentevents.UserSessionClaims{
		UserID:             w.humanA.ID,
		PersonalityAgentID: w.agentA.ID,
	}}
	server := NewServer(w.store, applicationapps.New(w.pool, w.store), sessions, authority)
	server.AllowedOrigins = []string{"https://sumi.test"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	workspace, err := w.store.CreateWorkspace(ctx, "session-targeted invite", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	path := "/workspaces/" + workspace.WorkspaceID + "/invites/current-agent"

	invalidBodies := []string{
		"",
		"null",
		"[]",
		`{"personality_agent_id":"` + w.agentA.ID + `"}`,
		"{} {}",
		strings.Repeat(" ", maxControlPlaneRequestBytes) + "{}",
	}
	for _, body := range invalidBodies {
		response := browserCall(mux, http.MethodPost, path, body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid exact-empty body prefix %q status=%d body=%s",
				body[:min(len(body), 80)], response.Code, response.Body.String())
		}
	}
	var targetedRows int
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM workspace_invites WHERE invite_kind='targeted_personality_agent'",
	).Scan(&targetedRows); err != nil {
		t.Fatal(err)
	}
	if targetedRows != 0 || sessions.authorizeCalls != 0 {
		t.Fatalf("invalid bodies reached mutation: rows=%d authorizations=%d",
			targetedRows, sessions.authorizeCalls)
	}
	if response := browserCall(mux, http.MethodGet, path, ""); response.Code != http.StatusNotFound {
		t.Fatalf("missing current-agent invite GET = %d: %s", response.Code, response.Body.String())
	}
	verifyCallsBeforeWrongOrigin := sessions.verifyCalls
	wrongOriginRequest := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
	wrongOriginRequest.Header.Set("Content-Type", "application/json")
	wrongOriginRequest.Header.Set("Origin", "https://attacker.test")
	wrongOriginRequest.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: "valid"})
	wrongOrigin := httptest.NewRecorder()
	mux.ServeHTTP(wrongOrigin, wrongOriginRequest)
	if wrongOrigin.Code != http.StatusForbidden || sessions.authorizeCalls != 0 ||
		sessions.verifyCalls != verifyCallsBeforeWrongOrigin {
		t.Fatalf("wrong Origin = %d verifications=%d/%d authorizations=%d body=%s",
			wrongOrigin.Code, sessions.verifyCalls, verifyCallsBeforeWrongOrigin,
			sessions.authorizeCalls, wrongOrigin.Body.String())
	}
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM workspace_invites WHERE invite_kind='targeted_personality_agent'",
	).Scan(&targetedRows); err != nil {
		t.Fatal(err)
	}
	if targetedRows != 0 {
		t.Fatalf("wrong Origin created %d targeted rows", targetedRows)
	}
	deniedSessions := &transportTestSessions{
		claims:       sessions.claims,
		denyMutation: true,
	}
	deniedServer := NewServer(w.store, nil, deniedSessions, authority)
	deniedServer.AllowedOrigins = []string{"https://sumi.test"}
	deniedMux := http.NewServeMux()
	deniedServer.RegisterRoutes(deniedMux)
	if response := browserCall(deniedMux, http.MethodPost, path, `{}`); response.Code != http.StatusUnauthorized {
		t.Fatalf("uncommitted session authorization = %d: %s", response.Code, response.Body.String())
	}
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM workspace_invites WHERE invite_kind='targeted_personality_agent'",
	).Scan(&targetedRows); err != nil {
		t.Fatal(err)
	}
	if targetedRows != 0 || deniedSessions.authorizeCalls != 1 {
		t.Fatalf("denied session mutated target ledger: rows=%d authorizations=%d",
			targetedRows, deniedSessions.authorizeCalls)
	}

	// Even a query-string attempt cannot author the target. The handler derives
	// it exclusively from the verified session claim and ignores this unrelated
	// parameter rather than turning it into a second targeting seam.
	created := browserCall(
		mux,
		http.MethodPost,
		path+"?personality_agent_id="+w.agentB.ID,
		`{}`,
	)
	if created.Code != http.StatusCreated {
		t.Fatalf("create current-agent invite = %d: %s", created.Code, created.Body.String())
	}
	assertTargetedInviteResponseIsNonSecret(t, created)
	var first inviteRecordWire
	decodeRecorder(t, created, &first)
	if first.Kind != string(InviteKindTargetedPersonalityAgent) {
		t.Fatalf("targeted discriminator = %q", first.Kind)
	}
	var recordedTargetID string
	if err := w.pool.QueryRow(ctx,
		"SELECT target_id FROM workspace_invites WHERE invite_id=$1",
		first.InviteID,
	).Scan(&recordedTargetID); err != nil {
		t.Fatal(err)
	}
	if recordedTargetID != sessions.claims.PersonalityAgentID || recordedTargetID == w.agentB.ID {
		t.Fatalf("query-authored target escaped signed session: recorded=%s claim=%s",
			recordedTargetID, sessions.claims.PersonalityAgentID)
	}
	replayed := browserCall(mux, http.MethodPost, path, `{}`)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay current-agent invite = %d: %s", replayed.Code, replayed.Body.String())
	}
	assertTargetedInviteResponseIsNonSecret(t, replayed)
	var second inviteRecordWire
	decodeRecorder(t, replayed, &second)
	if second != first {
		t.Fatalf("current-agent replay diverged: first=%#v second=%#v", first, second)
	}
	read := browserCall(mux, http.MethodGet, path, "")
	if read.Code != http.StatusOK {
		t.Fatalf("get current-agent invite = %d: %s", read.Code, read.Body.String())
	}
	assertTargetedInviteResponseIsNonSecret(t, read)
	revokePath := "/workspaces/" + workspace.WorkspaceID + "/invites/" + first.InviteID
	if revoked := browserCall(mux, http.MethodDelete, revokePath, ""); revoked.Code != http.StatusNoContent {
		t.Fatalf("revoke targeted invite = %d: %s", revoked.Code, revoked.Body.String())
	}
	if response := browserCall(mux, http.MethodGet, path, ""); response.Code != http.StatusNotFound {
		t.Fatalf("revoked current-agent invite GET = %d: %s", response.Code, response.Body.String())
	}
	registryPath := "/workspaces/" + workspace.WorkspaceID + "/invites"
	var registryBody struct {
		Invites []inviteRecordWire `json:"invites"`
	}
	registry := browserCall(mux, http.MethodGet, registryPath, "")
	decodeRecorder(t, registry, &registryBody)
	if len(registryBody.Invites) != 0 {
		t.Fatalf("revoked targeted invite remained listed: %#v", registryBody.Invites)
	}
	recreated := browserCall(mux, http.MethodPost, path, `{}`)
	if recreated.Code != http.StatusCreated {
		t.Fatalf("recreate current-agent invite = %d: %s", recreated.Code, recreated.Body.String())
	}
	decodeRecorder(t, recreated, &first)

	share, err := w.store.CreateInvite(ctx, workspace.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	registry = browserCall(mux, http.MethodGet, registryPath, "")
	if registry.Code != http.StatusOK {
		t.Fatalf("mixed invite registry = %d: %s", registry.Code, registry.Body.String())
	}
	decodeRecorder(t, registry, &registryBody)
	if len(registryBody.Invites) != 2 {
		t.Fatalf("mixed discriminated invite registry = %#v", registryBody.Invites)
	}
	kinds := map[string]bool{}
	for _, item := range registryBody.Invites {
		kinds[item.Kind] = true
	}
	if !kinds[string(InviteKindShareCode)] ||
		!kinds[string(InviteKindTargetedPersonalityAgent)] {
		t.Fatalf("mixed discriminated invite registry = %#v", registryBody.Invites)
	}

	agentMembership, err := w.store.RedeemInvite(ctx, share.Code, w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	joined := browserCall(mux, http.MethodGet, path, "")
	if joined.Code != http.StatusConflict {
		t.Fatalf("active exact PA GET = %d: %s", joined.Code, joined.Body.String())
	}
	registry = browserCall(mux, http.MethodGet, registryPath, "")
	decodeRecorder(t, registry, &registryBody)
	for _, item := range registryBody.Invites {
		if item.InviteID == first.InviteID {
			t.Fatal("obsolete targeted invitation remained in the manager registry")
		}
	}
	if err := w.store.RemoveMember(
		ctx, workspace.WorkspaceID, agentMembership.WorkspaceMemberID, w.humanA,
	); err != nil {
		t.Fatal(err)
	}
	reissued := browserCall(mux, http.MethodPost, path, `{}`)
	if reissued.Code != http.StatusCreated {
		t.Fatalf("post-leave current-agent invite = %d: %s", reissued.Code, reissued.Body.String())
	}
	var third inviteRecordWire
	decodeRecorder(t, reissued, &third)
	if third.InviteID == first.InviteID {
		t.Fatal("post-leave issuance revived the obsolete invitation")
	}

	if err := authority.TransferEmployment(
		ctx, w.agentA.ID, koseki.EmployerHuman, w.humanB.ID,
	); err != nil {
		t.Fatal(err)
	}
	formerEmployer := browserCall(mux, http.MethodPost, path, `{}`)
	if formerEmployer.Code != http.StatusForbidden {
		t.Fatalf("former Employer issuance = %d: %s", formerEmployer.Code, formerEmployer.Body.String())
	}

	missingPA := &transportTestSessions{claims: agentevents.UserSessionClaims{UserID: w.humanA.ID}}
	missingPAServer := NewServer(w.store, nil, missingPA, authority)
	missingPAServer.AllowedOrigins = []string{"https://sumi.test"}
	missingPAMux := http.NewServeMux()
	missingPAServer.RegisterRoutes(missingPAMux)
	if response := browserCall(missingPAMux, http.MethodPost, path, `{}`); response.Code != http.StatusUnauthorized {
		t.Fatalf("session without PA binding = %d: %s", response.Code, response.Body.String())
	}

	unavailableServer := NewServer(w.store, nil, sessions)
	unavailableServer.AllowedOrigins = []string{"https://sumi.test"}
	unavailableMux := http.NewServeMux()
	unavailableServer.RegisterRoutes(unavailableMux)
	if response := browserCall(unavailableMux, http.MethodPost, path, `{}`); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing Employer authority seam = %d: %s", response.Code, response.Body.String())
	}
}

func assertTargetedInviteResponseIsNonSecret(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	var body map[string]json.RawMessage
	decodeRecorder(t, response, &body)
	for _, forbidden := range []string{
		"personality_agent_id", "target_id", "target_kind", "code", "code_hash",
	} {
		if _, exists := body[forbidden]; exists {
			t.Fatalf("targeted invite response exposed %q: %s", forbidden, response.Body.String())
		}
	}
	if len(body) != 5 {
		t.Fatalf("targeted invite response shape = %s", response.Body.String())
	}
}

func TestLocalResolveEnabledAppBindsAuthenticatedActorWithoutInferenceOrSideEffects(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	appStore := applicationapps.New(w.pool, w.store)
	server := NewServer(w.store, appStore, nil)
	created, err := w.store.CreateWorkspace(ctx, "resolver", w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	installed, err := appStore.Install(ctx,
		applicationapps.WorkspaceOwner(created.WorkspaceID), w.agentA, "messaging")
	if err != nil {
		t.Fatal(err)
	}

	var before struct {
		workspaces, memberships, installations int
		enabled                                bool
		updatedAt                              time.Time
	}
	if err := w.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM workspaces),
		       (SELECT count(*) FROM workspace_members),
		       (SELECT count(*) FROM app_installations), enabled, updated_at
		FROM app_installations WHERE installation_id=$1`, installed.InstallationID,
	).Scan(&before.workspaces, &before.memberships, &before.installations,
		&before.enabled, &before.updatedAt); err != nil {
		t.Fatal(err)
	}

	requestBody := fmt.Sprintf(`{"workspace_id":%q,"app_id":"messaging"}`, created.WorkspaceID)
	resolved := invokeLocal(server.localResolveEnabledApp, requestBody, w.agentA.ID)
	if resolved.Code != http.StatusOK {
		t.Fatalf("resolve enabled app = %d: %s", resolved.Code, resolved.Body.String())
	}
	var resolution map[string]any
	decodeRecorder(t, resolved, &resolution)
	if len(resolution) != 1 || resolution["installation_id"] != installed.InstallationID {
		t.Fatalf("resolver exposed unexpected fields: %#v", resolution)
	}

	var after struct {
		workspaces, memberships, installations int
		enabled                                bool
		updatedAt                              time.Time
	}
	if err := w.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM workspaces),
		       (SELECT count(*) FROM workspace_members),
		       (SELECT count(*) FROM app_installations), enabled, updated_at
		FROM app_installations WHERE installation_id=$1`, installed.InstallationID,
	).Scan(&after.workspaces, &after.memberships, &after.installations,
		&after.enabled, &after.updatedAt); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("bind-time app resolution mutated state: before=%#v after=%#v", before, after)
	}

	for name, body := range map[string]string{
		"missing Workspace": `{"app_id":"messaging"}`,
		"invalid Workspace": `{"workspace_id":"not-a-workspace","app_id":"messaging"}`,
		"missing app":       fmt.Sprintf(`{"workspace_id":%q}`, created.WorkspaceID),
		"unknown field":     fmt.Sprintf(`{"workspace_id":%q,"app_id":"messaging","installation_id":%q}`, created.WorkspaceID, installed.InstallationID),
	} {
		response := invokeLocal(server.localResolveEnabledApp, body, w.agentA.ID)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d: %s", name, response.Code, response.Body.String())
		}
	}
	nonMember := invokeLocal(server.localResolveEnabledApp, requestBody, w.agentB.ID)
	if nonMember.Code != http.StatusNotFound || !strings.Contains(nonMember.Body.String(), `"not_found"`) {
		t.Fatalf("non-member resolution = %d: %s", nonMember.Code, nonMember.Body.String())
	}
	missingApp := invokeLocal(server.localResolveEnabledApp,
		fmt.Sprintf(`{"workspace_id":%q,"app_id":"alarm"}`, created.WorkspaceID), w.agentA.ID)
	if missingApp.Code != http.StatusNotFound || !strings.Contains(missingApp.Body.String(), `"installation_not_found"`) {
		t.Fatalf("missing installation = %d: %s", missingApp.Code, missingApp.Body.String())
	}

	forbiddenServer := NewServer(w.store, applicationapps.New(w.pool, nil), nil)
	forbidden := invokeLocal(forbiddenServer.localResolveEnabledApp, requestBody, w.agentA.ID)
	if forbidden.Code != http.StatusForbidden || !strings.Contains(forbidden.Body.String(), `"forbidden"`) {
		t.Fatalf("unavailable Workspace authority = %d: %s", forbidden.Code, forbidden.Body.String())
	}
	if _, err := appStore.SetEnabledByID(ctx, installed.InstallationID, w.agentA, false); err != nil {
		t.Fatal(err)
	}
	disabled := invokeLocal(server.localResolveEnabledApp, requestBody, w.agentA.ID)
	if disabled.Code != http.StatusConflict || !strings.Contains(disabled.Body.String(), `"app_disabled"`) {
		t.Fatalf("disabled installation = %d: %s", disabled.Code, disabled.Body.String())
	}
	if _, err := appStore.SetEnabledByID(ctx, installed.InstallationID, w.agentA, true); err != nil {
		t.Fatal(err)
	}
	if err := appStore.UninstallByID(ctx, installed.InstallationID, w.agentA); err != nil {
		t.Fatal(err)
	}
	uninstalled := invokeLocal(server.localResolveEnabledApp, requestBody, w.agentA.ID)
	if uninstalled.Code != http.StatusNotFound || !strings.Contains(uninstalled.Body.String(), `"installation_not_found"`) {
		t.Fatalf("uninstalled installation = %d: %s", uninstalled.Code, uninstalled.Body.String())
	}
	unavailable := invokeLocal(NewServer(w.store, nil, nil).localResolveEnabledApp,
		requestBody, w.agentA.ID)
	if unavailable.Code != http.StatusServiceUnavailable || !strings.Contains(unavailable.Body.String(), `"apps_unavailable"`) {
		t.Fatalf("unavailable app lifecycle = %d: %s", unavailable.Code, unavailable.Body.String())
	}
}

func TestWorkspaceMembersProjectCanonicalNamesAndExactRoleTargets(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	sessions := &transportTestSessions{
		claims: agentevents.UserSessionClaims{UserID: w.humanA.ID},
	}
	server := NewServer(w.store, applicationapps.New(w.pool, w.store), sessions)
	server.AllowedOrigins = []string{"https://sumi.test"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	created, err := w.store.CreateWorkspace(ctx, "Named members", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	admit := func(ref participant.Ref) Membership {
		t.Helper()
		invite, err := w.store.CreateInvite(ctx, created.WorkspaceID, w.humanA)
		if err != nil {
			t.Fatal(err)
		}
		membership, err := w.store.RedeemInvite(ctx, invite.Code, ref)
		if err != nil {
			t.Fatal(err)
		}
		return membership
	}
	humanB := admit(w.humanB)
	_ = admit(w.agentA)
	agentB := admit(w.agentB)
	role, err := w.store.CreateRole(ctx, created.WorkspaceID, w.humanA,
		"Selected member", "", map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}

	for _, membershipID := range []string{humanB.WorkspaceMemberID, agentB.WorkspaceMemberID} {
		response := browserCall(mux, http.MethodPut,
			"/workspaces/"+created.WorkspaceID+"/members/"+membershipID+"/roles",
			fmt.Sprintf(`{"role_ids":[%q]}`, role.RoleID))
		if response.Code != http.StatusOK {
			t.Fatalf("assign exact membership %s: status=%d body=%s",
				membershipID, response.Code, response.Body.String())
		}
	}

	response := browserCall(mux, http.MethodGet,
		"/workspaces/"+created.WorkspaceID+"/members", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list named members: status=%d body=%s", response.Code, response.Body.String())
	}
	var listed struct {
		Members []membershipWire `json:"members"`
	}
	decodeRecorder(t, response, &listed)
	wantNames := map[string]string{
		w.humanA.Key(): "Yohaku",
		w.humanB.Key(): "Haru",
		w.agentA.Key(): "Kuro",
		w.agentB.Key(): "Shiro",
	}
	wantRole := map[string]bool{w.humanB.Key(): true, w.agentB.Key(): true}
	if len(listed.Members) != len(wantNames) {
		t.Fatalf("named member count = %d, want %d: %#v", len(listed.Members), len(wantNames), listed.Members)
	}
	for _, member := range listed.Members {
		ref, err := member.Participant.ref()
		if err != nil {
			t.Fatal(err)
		}
		key := ref.Key()
		if member.DisplayName != wantNames[key] {
			t.Fatalf("member %s display name = %q, want %q", key, member.DisplayName, wantNames[key])
		}
		assigned := len(member.RoleIDs) == 1 && member.RoleIDs[0] == role.RoleID
		if assigned != wantRole[key] {
			t.Fatalf("member %s role ids = %#v, selected=%v", key, member.RoleIDs, wantRole[key])
		}
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
		`{"workspace_id":%q,"name":"Inviter","position":7,"permissions":["manage_members"]}`,
		agentWorkspace.WorkspaceID), w.agentA.ID)
	if roleResponse.Code != http.StatusCreated {
		t.Fatalf("Agent role create status = %d, body=%s", roleResponse.Code, roleResponse.Body.String())
	}
	var role roleWire
	decodeRecorder(t, roleResponse, &role)
	if role.Name != "Inviter" || role.Position != 7 || len(role.Permissions) != 1 || role.Permissions[0] != PermissionManageMembers {
		t.Fatalf("Agent-created role = %#v", role)
	}
	browserRole := browserCall(mux, http.MethodPost,
		"/workspaces/"+humanWorkspace.WorkspaceID+"/roles",
		`{"name":"Ordered","position":9,"permissions":[]}`)
	if browserRole.Code != http.StatusCreated {
		t.Fatalf("Human role create status = %d, body=%s", browserRole.Code, browserRole.Body.String())
	}
	var humanRole roleWire
	decodeRecorder(t, browserRole, &humanRole)
	if humanRole.Position != 9 {
		t.Fatalf("Human-created role position = %d", humanRole.Position)
	}
	browserRoleUpdate := browserCall(mux, http.MethodPatch,
		"/workspaces/"+humanWorkspace.WorkspaceID+"/roles/"+humanRole.RoleID,
		`{"name":"Ordered browser","position":10,"permissions":[]}`)
	if browserRoleUpdate.Code != http.StatusOK {
		t.Fatalf("Human role position update = %d, body=%s", browserRoleUpdate.Code, browserRoleUpdate.Body.String())
	}
	decodeRecorder(t, browserRoleUpdate, &humanRole)
	if humanRole.Position != 10 {
		t.Fatalf("Human-updated role position = %d", humanRole.Position)
	}
	if invalid := browserCall(mux, http.MethodPost,
		"/workspaces/"+humanWorkspace.WorkspaceID+"/roles",
		`{"name":"invalid null create","position":null,"permissions":[]}`); invalid.Code != http.StatusBadRequest {
		t.Fatalf("null browser role position create = %d: %s", invalid.Code, invalid.Body.String())
	}
	if invalid := browserCall(mux, http.MethodPatch,
		"/workspaces/"+humanWorkspace.WorkspaceID+"/roles/"+humanRole.RoleID,
		`{"name":"invalid null update","position":null,"permissions":[]}`); invalid.Code != http.StatusBadRequest {
		t.Fatalf("null browser role position update = %d: %s", invalid.Code, invalid.Body.String())
	}
	preservedRole := invokeLocal(server.localUpdateRole, fmt.Sprintf(
		`{"workspace_id":%q,"role_id":%q,"name":"Inviter preserved","permissions":["manage_members"]}`,
		agentWorkspace.WorkspaceID, role.RoleID), w.agentA.ID)
	if preservedRole.Code != http.StatusOK {
		t.Fatalf("Agent role preserve status = %d, body=%s", preservedRole.Code, preservedRole.Body.String())
	}
	decodeRecorder(t, preservedRole, &role)
	if role.Position != 7 {
		t.Fatalf("omitted role position was not preserved: %#v", role)
	}
	positionedRole := invokeLocal(server.localUpdateRole, fmt.Sprintf(
		`{"workspace_id":%q,"role_id":%q,"name":"Inviter moved","position":5,"permissions":["manage_members"]}`,
		agentWorkspace.WorkspaceID, role.RoleID), w.agentA.ID)
	if positionedRole.Code != http.StatusOK {
		t.Fatalf("Agent role position update = %d, body=%s", positionedRole.Code, positionedRole.Body.String())
	}
	decodeRecorder(t, positionedRole, &role)
	if role.Position != 5 {
		t.Fatalf("Agent-updated role position = %d", role.Position)
	}
	if invalid := invokeLocal(server.localCreateRole, fmt.Sprintf(
		`{"workspace_id":%q,"name":"invalid null create","position":null,"permissions":[]}`,
		agentWorkspace.WorkspaceID), w.agentA.ID); invalid.Code != http.StatusBadRequest {
		t.Fatalf("null Agent role position create = %d: %s", invalid.Code, invalid.Body.String())
	}
	if invalid := invokeLocal(server.localUpdateRole, fmt.Sprintf(
		`{"workspace_id":%q,"role_id":%q,"name":"invalid null update","position":null,"permissions":[]}`,
		agentWorkspace.WorkspaceID, role.RoleID), w.agentA.ID); invalid.Code != http.StatusBadRequest {
		t.Fatalf("null Agent role position update = %d: %s", invalid.Code, invalid.Body.String())
	}
	if invalid := browserCall(mux, http.MethodPatch,
		"/workspaces/"+humanWorkspace.WorkspaceID+"/roles/"+humanRole.RoleID,
		`{"name":"invalid","position":-1,"permissions":[]}`); invalid.Code != http.StatusBadRequest {
		t.Fatalf("negative browser role position = %d: %s", invalid.Code, invalid.Body.String())
	}
	if invalid := invokeLocal(server.localUpdateRole, fmt.Sprintf(
		`{"workspace_id":%q,"role_id":%q,"name":"invalid","position":1000001,"permissions":[]}`,
		agentWorkspace.WorkspaceID, role.RoleID), w.agentA.ID); invalid.Code != http.StatusBadRequest {
		t.Fatalf("oversized Agent role position = %d: %s", invalid.Code, invalid.Body.String())
	}

	for label, operation := range map[string]string{
		"empty": `""`,
		"null":  `null`,
	} {
		invalid := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
			`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging","operation_id":%s}`,
			humanWorkspace.WorkspaceID, operation))
		if invalid.Code != http.StatusBadRequest {
			t.Fatalf("%s install operation id = %d, body=%s",
				label, invalid.Code, invalid.Body.String())
		}
	}
	const humanInstallOperation = "00000000-0000-4000-8000-000000000201"
	humanInstall := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging","operation_id":%q}`,
		humanWorkspace.WorkspaceID, humanInstallOperation))
	if humanInstall.Code != http.StatusCreated {
		t.Fatalf("Human app install status = %d, body=%s", humanInstall.Code, humanInstall.Body.String())
	}
	humanInstallReplay := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging","operation_id":%q}`,
		humanWorkspace.WorkspaceID, humanInstallOperation))
	if humanInstallReplay.Code != http.StatusCreated ||
		humanInstallReplay.Body.String() != humanInstall.Body.String() {
		t.Fatalf("Human app install replay = %d, body=%s; original=%s",
			humanInstallReplay.Code, humanInstallReplay.Body.String(), humanInstall.Body.String())
	}
	humanInstallMismatch := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"missing-app","operation_id":%q}`,
		humanWorkspace.WorkspaceID, humanInstallOperation))
	if humanInstallMismatch.Code != http.StatusConflict ||
		!strings.Contains(humanInstallMismatch.Body.String(), `"idempotency_conflict"`) {
		t.Fatalf("Human app install operation mismatch = %d, body=%s",
			humanInstallMismatch.Code, humanInstallMismatch.Body.String())
	}
	for _, test := range []struct {
		name           string
		operationField string
	}{
		{name: "omitted"},
		{name: "null", operationField: `,"operation_id":null`},
		{name: "empty", operationField: `,"operation_id":""`},
		{name: "malformed", operationField: `,"operation_id":"not-a-uuid"`},
	} {
		invalid := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
			`{"owner":{"kind":"participant","participant":{"kind":"human","human_id":%q}},"app_id":"alarm"%s}`,
			w.humanA.ID, test.operationField))
		if invalid.Code != http.StatusBadRequest {
			t.Fatalf("Participant install with %s operation id = %d, body=%s",
				test.name, invalid.Code, invalid.Body.String())
		}
	}
	const participantInstallOperation = "00000000-0000-4000-8000-000000000202"
	participantInstall := browserCall(mux, http.MethodPost, "/app-installations", fmt.Sprintf(
		`{"owner":{"kind":"participant","participant":{"kind":"human","human_id":%q}},"app_id":"alarm","operation_id":%q}`,
		w.humanA.ID, participantInstallOperation))
	if participantInstall.Code != http.StatusCreated {
		t.Fatalf("Participant install with canonical operation id = %d, body=%s",
			participantInstall.Code, participantInstall.Body.String())
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
	for label, epoch := range map[string]string{
		"empty": `""`,
		"null":  `null`,
	} {
		invalid := browserCall(mux, http.MethodPut,
			"/app-installations/"+installations["Human"].InstallationID+"/state",
			fmt.Sprintf(`{"state":"disabled","expected_authority_epoch":%s}`, epoch))
		if invalid.Code != http.StatusBadRequest {
			t.Fatalf("%s authority epoch = %d, body=%s",
				label, invalid.Code, invalid.Body.String())
		}
	}
	humanDisable := browserCall(mux, http.MethodPut,
		"/app-installations/"+installations["Human"].InstallationID+"/state",
		`{"state":"disabled","expected_authority_epoch":"1"}`)
	if humanDisable.Code != http.StatusOK {
		t.Fatalf("Human app disable status = %d, body=%s", humanDisable.Code, humanDisable.Body.String())
	}
	humanDisableReplay := browserCall(mux, http.MethodPut,
		"/app-installations/"+installations["Human"].InstallationID+"/state",
		`{"state":"disabled","expected_authority_epoch":"1"}`)
	if humanDisableReplay.Code != http.StatusConflict ||
		!strings.Contains(humanDisableReplay.Body.String(), `"stale_authority"`) {
		t.Fatalf("stale Human app replay status = %d, body=%s",
			humanDisableReplay.Code, humanDisableReplay.Body.String())
	}
	humanLegacyEnable := browserCall(mux, http.MethodPut,
		"/app-installations/"+installations["Human"].InstallationID+"/state",
		`{"state":"enabled"}`)
	if humanLegacyEnable.Code != http.StatusOK ||
		!strings.Contains(humanLegacyEnable.Body.String(), `"state":"enabled"`) {
		t.Fatalf("omitted Human authority epoch status = %d, body=%s",
			humanLegacyEnable.Code, humanLegacyEnable.Body.String())
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

func TestAppCatalogWireCarriesCapabilityVocabularyWithoutMentionAll(t *testing.T) {
	w := newTestWorld(t)
	sessions := &transportTestSessions{
		claims: agentevents.UserSessionClaims{UserID: w.humanA.ID},
	}
	server := NewServer(w.store, applicationapps.New(w.pool, w.store), sessions)
	server.AllowedOrigins = []string{"https://sumi.test"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	response := browserCall(mux, http.MethodGet, "/apps/catalog", "")
	if response.Code != http.StatusOK {
		t.Fatalf("catalog status = %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Apps []appDescriptorWire `json:"apps"`
	}
	decodeRecorder(t, response, &body)
	if len(body.Apps) != 4 {
		t.Fatalf("catalog wire = %#v", body.Apps)
	}
	for _, descriptor := range body.Apps {
		if descriptor.WorkspaceRoleCapabilities == nil {
			t.Fatalf("%s emitted null workspace_role_capabilities", descriptor.AppID)
		}
		if descriptor.AppID == "messaging" {
			if len(descriptor.WorkspaceRoleCapabilities) != 1 ||
				descriptor.WorkspaceRoleCapabilities[0].Ref != testMessagingManageChannels {
				t.Fatalf("Messaging descriptor wire = %#v", descriptor)
			}
		} else if len(descriptor.WorkspaceRoleCapabilities) != 0 {
			t.Fatalf("unexpected %s capability wire = %#v",
				descriptor.AppID, descriptor.WorkspaceRoleCapabilities)
		}
		for _, capability := range descriptor.WorkspaceRoleCapabilities {
			if capability.Ref == "app.messaging.mention_all" {
				t.Fatal("UI-facing app catalog promised unimplemented mention_all")
			}
		}
	}
}

func TestInvitePreviewAndRequiredRequestPresenceAcrossTransports(t *testing.T) {
	w := newTestWorld(t)
	sessions := &transportTestSessions{
		claims: agentevents.UserSessionClaims{UserID: w.humanA.ID},
	}
	appStore := applicationapps.New(w.pool, w.store)
	server := NewServer(w.store, appStore, sessions)
	server.AllowedOrigins = []string{"https://sumi.test"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	created := browserWorkspaceMutation(t, mux, http.MethodPost, "/workspaces",
		`{"name":"Presence"}`, http.StatusCreated)
	inviteResponse := browserCall(mux, http.MethodPost,
		"/workspaces/"+created.WorkspaceID+"/invites", `{}`)
	if inviteResponse.Code != http.StatusCreated {
		t.Fatalf("create invite = %d: %s", inviteResponse.Code, inviteResponse.Body.String())
	}
	var invite inviteWire
	decodeRecorder(t, inviteResponse, &invite)

	// Public scanner-style GET and authenticated Agent preview converge on the
	// same non-consuming minimal projection.
	publicPreview := httptest.NewRecorder()
	publicPreviewRequest := httptest.NewRequest(http.MethodGet,
		"/workspace-invites/preview?code="+invite.Code, nil)
	mux.ServeHTTP(publicPreview, publicPreviewRequest)
	if publicPreview.Code != http.StatusOK {
		t.Fatalf("public preview = %d: %s", publicPreview.Code, publicPreview.Body.String())
	}
	localPreview := invokeLocal(server.localPreviewInvite,
		fmt.Sprintf(`{"code":%q}`, invite.Code), w.agentA.ID)
	if localPreview.Code != http.StatusOK || localPreview.Body.String() != publicPreview.Body.String() {
		t.Fatalf("preview parity public=%d %s local=%d %s", publicPreview.Code,
			publicPreview.Body.String(), localPreview.Code, localPreview.Body.String())
	}
	var preview invitePreviewWire
	decodeRecorder(t, publicPreview, &preview)
	if preview.WorkspaceID != created.WorkspaceID || preview.WorkspaceName != "Presence" {
		t.Fatalf("invite preview leaked or omitted identity: %#v", preview)
	}

	for label, response := range map[string]*httptest.ResponseRecorder{
		"browser missing redeem code": browserCall(mux, http.MethodPost, "/workspace-invites/redeem", `{}`),
		"browser empty redeem code":   browserCall(mux, http.MethodPost, "/workspace-invites/redeem", `{"code":""}`),
		"agent missing redeem code":   invokeLocal(server.localRedeemInvite, `{}`, w.agentA.ID),
		"agent empty redeem code":     invokeLocal(server.localRedeemInvite, `{"code":""}`, w.agentA.ID),
		"browser missing permissions": browserCall(mux, http.MethodPost, "/workspaces/"+created.WorkspaceID+"/roles", `{"name":"missing"}`),
		"agent missing permissions": invokeLocal(server.localCreateRole,
			fmt.Sprintf(`{"workspace_id":%q,"name":"missing"}`, created.WorkspaceID), w.agentA.ID),
		"browser missing role ids": browserCall(mux, http.MethodPut,
			"/workspaces/"+created.WorkspaceID+"/members/"+created.OwnerWorkspaceMemberID+"/roles", `{}`),
		"agent missing role ids": invokeLocal(server.localSetMemberRoles,
			fmt.Sprintf(`{"workspace_id":%q,"workspace_member_id":%q}`,
				created.WorkspaceID, created.OwnerWorkspaceMemberID), w.agentA.ID),
		"agent missing enabled": invokeLocal(server.localSetAppEnabled,
			`{"installation_id":"0198f0f4-9b72-7000-8000-000000000199"}`, w.agentA.ID),
	} {
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s = %d, body=%s", label, response.Code, response.Body.String())
		}
	}

	// Explicit empty arrays and false remain real values rather than being
	// confused with omission.
	emptyRole := browserCall(mux, http.MethodPost,
		"/workspaces/"+created.WorkspaceID+"/roles",
		`{"name":"Empty","permissions":[]}`)
	if emptyRole.Code != http.StatusCreated {
		t.Fatalf("explicit empty permissions = %d: %s", emptyRole.Code, emptyRole.Body.String())
	}
	var omittedPositionRole roleWire
	decodeRecorder(t, emptyRole, &omittedPositionRole)
	if omittedPositionRole.Position != 0 {
		t.Fatalf("omitted browser role position = %d, want 0", omittedPositionRole.Position)
	}
	explicitZeroRole := browserCall(mux, http.MethodPost,
		"/workspaces/"+created.WorkspaceID+"/roles",
		`{"name":"Zero","position":0,"permissions":[]}`)
	if explicitZeroRole.Code != http.StatusCreated {
		t.Fatalf("explicit zero browser role position = %d: %s", explicitZeroRole.Code, explicitZeroRole.Body.String())
	}
	var zeroPositionRole roleWire
	decodeRecorder(t, explicitZeroRole, &zeroPositionRole)
	if zeroPositionRole.Position != 0 {
		t.Fatalf("explicit zero browser role position response = %d, want 0", zeroPositionRole.Position)
	}
	emptyAssignments := browserCall(mux, http.MethodPut,
		"/workspaces/"+created.WorkspaceID+"/members/"+created.OwnerWorkspaceMemberID+"/roles",
		`{"role_ids":[]}`)
	if emptyAssignments.Code != http.StatusOK {
		t.Fatalf("explicit empty role_ids = %d: %s", emptyAssignments.Code, emptyAssignments.Body.String())
	}
	agentWorkspace := localWorkspaceMutation(t, server, server.localCreateWorkspace,
		`{"name":"Agent presence"}`, http.StatusCreated, w.agentA.ID)
	install := invokeLocal(server.localInstallApp, fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging"}`,
		agentWorkspace.WorkspaceID), w.agentA.ID)
	if install.Code != http.StatusCreated {
		t.Fatalf("install app = %d: %s", install.Code, install.Body.String())
	}
	var installation appInstallationWire
	decodeRecorder(t, install, &installation)
	disabled := invokeLocal(server.localSetAppEnabled, fmt.Sprintf(
		`{"installation_id":%q,"enabled":false}`, installation.InstallationID), w.agentA.ID)
	if disabled.Code != http.StatusOK {
		t.Fatalf("explicit false was not admitted as a value: %d %s", disabled.Code, disabled.Body.String())
	}
}

func TestRegisteredLocalControlWorkspaceRoutesAuthenticateAndBindGeneration(t *testing.T) {
	w := newTestWorld(t)
	appStore := applicationapps.New(w.pool, w.store)
	server := NewServer(w.store, appStore, nil)

	commandStore, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commandStore.Close() })
	gateway, err := agentevents.OpenDurableGateway(testfs.PrivateDir(t), commandStore)
	if err != nil {
		t.Fatal(err)
	}
	authorization := agentevents.LocalRuntimeAuthorization{
		BearerToken:           "workspace-local-control-bearer-generation-one",
		TenantID:              "workspace-test",
		PersonalityAgentID:    w.agentA.ID,
		Generation:            1,
		RPCBootNonce:          "workspace-boot-1",
		Audience:              agentevents.DefaultAgentAudience(),
		DeliveryAuthorization: agentevents.LocalDeliveryRaw,
	}
	otherAuthorization := authorization
	otherAuthorization.BearerToken = "workspace-local-control-other-agent-bearer"
	otherAuthorization.PersonalityAgentID = "0198f0f4-9b72-7000-8000-0000000001a2"
	control, err := agentevents.NewLocalControlServer(gateway,
		[]byte("workspace-local-control-signing-secret-32-bytes"),
		[]agentevents.LocalRuntimeAuthorization{authorization, otherAuthorization})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterLocalControlRoutes(control); err != nil {
		t.Fatal(err)
	}
	handler, err := control.HandlerForLocalRuntime(w.agentA.ID)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	call := func(path, bearer, body string) *httptest.ResponseRecorder {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			request.Header.Set("Authorization", "Bearer "+bearer)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		recorder.Code = response.StatusCode
		_, _ = recorder.Body.Write(raw)
		return recorder
	}
	if response := call(LocalWorkspaceCreatePath, "", `{"name":"no auth"}`); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated registered route = %d: %s", response.Code, response.Body.String())
	}
	if response := call(LocalWorkspaceCreatePath, otherAuthorization.BearerToken,
		`{"name":"wrong PAID"}`); response.Code != http.StatusUnauthorized {
		t.Fatalf("cross-PAID bearer on bound handler = %d: %s", response.Code, response.Body.String())
	}
	createdResponse := call(LocalWorkspaceCreatePath, authorization.BearerToken,
		`{"name":"Registered Agent Workspace"}`)
	if createdResponse.Code != http.StatusCreated {
		t.Fatalf("registered create route = %d: %s", createdResponse.Code, createdResponse.Body.String())
	}
	var created workspaceWire
	decodeRecorder(t, createdResponse, &created)
	assertOwnerParticipant(t, w, created, w.agentA)
	humanOnly, err := w.store.CreateWorkspace(context.Background(), "Human-only Workspace", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if invalid := call(LocalWorkspacesPath, authorization.BearerToken,
		fmt.Sprintf(`{"personality_agent_id":%q}`, otherAuthorization.PersonalityAgentID)); invalid.Code != http.StatusBadRequest {
		t.Fatalf("registered list accepted caller actor = %d: %s", invalid.Code, invalid.Body.String())
	}
	listedResponse := call(LocalWorkspacesPath, authorization.BearerToken, `{}`)
	if listedResponse.Code != http.StatusOK {
		t.Fatalf("registered list route = %d: %s", listedResponse.Code, listedResponse.Body.String())
	}
	var listed struct {
		Workspaces []workspaceWire `json:"workspaces"`
	}
	decodeRecorder(t, listedResponse, &listed)
	if len(listed.Workspaces) != 1 || listed.Workspaces[0].WorkspaceID != created.WorkspaceID {
		t.Fatalf("authenticated actor Workspace list = %#v, want only %s", listed.Workspaces, created.WorkspaceID)
	}
	if listed.Workspaces[0].WorkspaceID == humanOnly.WorkspaceID {
		t.Fatalf("authenticated actor list exposed Human-only Workspace %s", humanOnly.WorkspaceID)
	}
	roleCreateOmitted := call(LocalRoleCreatePath, authorization.BearerToken, fmt.Sprintf(
		`{"workspace_id":%q,"name":"Registered omitted position","permissions":[]}`,
		created.WorkspaceID))
	if roleCreateOmitted.Code != http.StatusCreated {
		t.Fatalf("registered role create with omitted position = %d: %s",
			roleCreateOmitted.Code, roleCreateOmitted.Body.String())
	}
	var registeredRole roleWire
	decodeRecorder(t, roleCreateOmitted, &registeredRole)
	if registeredRole.Position != 0 {
		t.Fatalf("registered omitted role position = %d, want 0", registeredRole.Position)
	}
	roleCreateZero := call(LocalRoleCreatePath, authorization.BearerToken, fmt.Sprintf(
		`{"workspace_id":%q,"name":"Registered zero position","position":0,"permissions":[]}`,
		created.WorkspaceID))
	if roleCreateZero.Code != http.StatusCreated {
		t.Fatalf("registered role create with explicit zero = %d: %s",
			roleCreateZero.Code, roleCreateZero.Body.String())
	}
	var registeredZeroRole roleWire
	decodeRecorder(t, roleCreateZero, &registeredZeroRole)
	if registeredZeroRole.Position != 0 {
		t.Fatalf("registered explicit zero role position = %d, want 0", registeredZeroRole.Position)
	}
	if invalid := call(LocalRoleCreatePath, authorization.BearerToken, fmt.Sprintf(
		`{"workspace_id":%q,"name":"Registered null create","position":null,"permissions":[]}`,
		created.WorkspaceID)); invalid.Code != http.StatusBadRequest {
		t.Fatalf("registered null role position create = %d: %s", invalid.Code, invalid.Body.String())
	}
	if invalid := call(LocalRoleUpdatePath, authorization.BearerToken, fmt.Sprintf(
		`{"workspace_id":%q,"role_id":%q,"name":"Registered null update","position":null,"permissions":[]}`,
		created.WorkspaceID, registeredRole.RoleID)); invalid.Code != http.StatusBadRequest {
		t.Fatalf("registered null role position update = %d: %s", invalid.Code, invalid.Body.String())
	}
	inviteResponse := call(LocalInviteCreatePath, authorization.BearerToken,
		fmt.Sprintf(`{"workspace_id":%q}`, created.WorkspaceID))
	if inviteResponse.Code != http.StatusCreated {
		t.Fatalf("registered invite-create route = %d: %s", inviteResponse.Code, inviteResponse.Body.String())
	}
	var invite inviteWire
	decodeRecorder(t, inviteResponse, &invite)
	if preview := call(LocalInvitePreviewPath, authorization.BearerToken,
		fmt.Sprintf(`{"code":%q}`, invite.Code)); preview.Code != http.StatusOK {
		t.Fatalf("registered invite-preview route = %d: %s", preview.Code, preview.Body.String())
	}
	installResponse := call(LocalAppInstallPath, authorization.BearerToken, fmt.Sprintf(
		`{"owner":{"kind":"workspace","workspace_id":%q},"app_id":"messaging"}`,
		created.WorkspaceID))
	if installResponse.Code != http.StatusCreated {
		t.Fatalf("registered app-install route = %d: %s", installResponse.Code, installResponse.Body.String())
	}
	var installation appInstallationWire
	decodeRecorder(t, installResponse, &installation)
	resolveResponse := call(LocalAppResolvePath, authorization.BearerToken, fmt.Sprintf(
		`{"workspace_id":%q,"app_id":"messaging"}`, created.WorkspaceID))
	if resolveResponse.Code != http.StatusOK {
		t.Fatalf("registered app resolver = %d: %s", resolveResponse.Code, resolveResponse.Body.String())
	}
	var resolved struct {
		InstallationID string `json:"installation_id"`
	}
	decodeRecorder(t, resolveResponse, &resolved)
	if resolved.InstallationID != installation.InstallationID {
		t.Fatalf("registered app resolver id = %q, want %q",
			resolved.InstallationID, installation.InstallationID)
	}

	replacement := authorization
	replacement.BearerToken = "workspace-local-control-bearer-generation-two"
	replacement.Generation = 2
	replacement.RPCBootNonce = "workspace-boot-2"
	if err := control.InstallLocalRuntimeAuthorization(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if response := call(LocalWorkspaceUpdatePath, authorization.BearerToken,
		fmt.Sprintf(`{"workspace_id":%q,"name":"stale"}`, created.WorkspaceID)); response.Code != http.StatusUnauthorized {
		t.Fatalf("stale generation bearer = %d: %s", response.Code, response.Body.String())
	}
	updated := call(LocalWorkspaceUpdatePath, replacement.BearerToken,
		fmt.Sprintf(`{"workspace_id":%q,"name":"Current generation"}`, created.WorkspaceID))
	if updated.Code != http.StatusOK {
		t.Fatalf("current generation mutation route = %d: %s", updated.Code, updated.Body.String())
	}
	var updatedWorkspace workspaceWire
	decodeRecorder(t, updated, &updatedWorkspace)
	if updatedWorkspace.Name != "Current generation" {
		t.Fatalf("registered mutation result = %#v", updatedWorkspace)
	}
	disabled := call(LocalAppSetEnabledPath, replacement.BearerToken, fmt.Sprintf(
		`{"installation_id":%q,"enabled":false}`, installation.InstallationID))
	if disabled.Code != http.StatusOK {
		t.Fatalf("registered app mutation route = %d: %s", disabled.Code, disabled.Body.String())
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
