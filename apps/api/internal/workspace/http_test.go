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
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
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
	foundMessaging := false
	for _, descriptor := range body.Apps {
		if descriptor.WorkspaceRoleCapabilities == nil {
			t.Fatalf("%s emitted null workspace_role_capabilities", descriptor.AppID)
		}
		if descriptor.AppID == "feedback" {
			t.Fatal("built-in Feedback must not appear in the installable app catalog")
		}
		if descriptor.AppID == "messaging" {
			foundMessaging = true
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
	if !foundMessaging {
		t.Fatal("catalog omitted Messaging")
	}
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
