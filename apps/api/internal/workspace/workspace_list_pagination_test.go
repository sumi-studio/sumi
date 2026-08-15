package workspace

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const testAgentC = "0198f0f4-9b72-7000-8000-0000000001a3"

type workspaceListPageWire struct {
	Workspaces []workspaceWire `json:"workspaces"`
	NextCursor string          `json:"next_cursor"`
}

func TestWorkspaceListCursorIsOpaqueTamperEvidentAndActorBound(t *testing.T) {
	authorization := agentevents.LocalRuntimeAuthorization{
		BearerToken:        "workspace-list-cursor-test-bearer-secret-a",
		PersonalityAgentID: testAgentA,
	}
	position := workspaceListCursorPosition{
		WorkspaceID: "0198f0f4-9b72-7000-8000-000000000711",
	}

	cursor, err := encodeWorkspaceListCursor(position, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if len(cursor) != workspaceListCursorEncodedBytes {
		t.Fatalf("cursor length = %d, want %d", len(cursor), workspaceListCursorEncodedBytes)
	}
	decoded, err := decodeWorkspaceListCursor(cursor, authorization)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.WorkspaceID != position.WorkspaceID {
		t.Fatalf("cursor round trip = %#v, want %#v", decoded, position)
	}

	tampered := []byte(cursor)
	if tampered[len(tampered)/2] == 'A' {
		tampered[len(tampered)/2] = 'B'
	} else {
		tampered[len(tampered)/2] = 'A'
	}
	if _, err := decodeWorkspaceListCursor(string(tampered), authorization); err == nil {
		t.Fatal("tampered cursor was accepted")
	}
	otherActor := authorization
	otherActor.PersonalityAgentID = testAgentB
	if _, err := decodeWorkspaceListCursor(cursor, otherActor); err == nil {
		t.Fatal("cursor crossed authenticated actor")
	}
	otherBearer := authorization
	otherBearer.BearerToken = "workspace-list-cursor-test-bearer-secret-b"
	if _, err := decodeWorkspaceListCursor(cursor, otherBearer); err == nil {
		t.Fatal("cursor crossed local runtime authorization")
	}
	if _, err := decodeWorkspaceListCursor(cursor+"A", authorization); err == nil {
		t.Fatal("oversized cursor was accepted")
	}

	// Even a correctly re-MACed unknown version is rejected. This locks the
	// fixed binary format rather than merely testing accidental corruption.
	wire, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatal(err)
	}
	wire[0] = workspaceListCursorVersion + 1
	copy(wire[workspaceListCursorPayloadBytes:],
		workspaceListCursorMAC(authorization, wire[:workspaceListCursorPayloadBytes]))
	unknownVersion := base64.RawURLEncoding.EncodeToString(wire)
	if _, err := decodeWorkspaceListCursor(unknownVersion, authorization); err == nil {
		t.Fatal("unknown cursor version was accepted")
	}

	wire[0] = workspaceListCursorVersion
	wire[1] = 1
	copy(wire[workspaceListCursorPayloadBytes:],
		workspaceListCursorMAC(authorization, wire[:workspaceListCursorPayloadBytes]))
	nonzeroReserved := base64.RawURLEncoding.EncodeToString(wire)
	if _, err := decodeWorkspaceListCursor(nonzeroReserved, authorization); err == nil {
		t.Fatal("cursor with nonzero reserved payload was accepted")
	}

	wire[1] = 0
	clear(wire[9:workspaceListCursorPayloadBytes])
	copy(wire[workspaceListCursorPayloadBytes:],
		workspaceListCursorMAC(authorization, wire[:workspaceListCursorPayloadBytes]))
	invalidWorkspace := base64.RawURLEncoding.EncodeToString(wire)
	if _, err := decodeWorkspaceListCursor(invalidWorkspace, authorization); err == nil {
		t.Fatal("cursor with a non-UUIDv7 Workspace identity was accepted")
	}
}

func TestLocalWorkspaceListPaginationIsBoundedStableAndActorScoped(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	var clockTick int64
	w.store.now = func() time.Time {
		clockTick++
		return baseTime.Add(time.Duration(clockTick) * time.Microsecond)
	}
	if _, err := w.pool.Exec(ctx, `
		INSERT INTO agents (personality_agent_id, human_id, display_name)
		VALUES ($1, $2, 'Murasaki')`, testAgentC, testHumanC); err != nil {
		t.Fatal(err)
	}
	agentC := participant.PersonalityAgent(testAgentC)

	server := NewServer(w.store, nil, nil)
	authA := agentevents.LocalRuntimeAuthorization{
		BearerToken:        "workspace-list-agent-a-bearer-secret-0001",
		PersonalityAgentID: w.agentA.ID,
	}
	authB := agentevents.LocalRuntimeAuthorization{
		BearerToken:        "workspace-list-agent-b-bearer-secret-0002",
		PersonalityAgentID: w.agentB.ID,
	}
	authC := agentevents.LocalRuntimeAuthorization{
		BearerToken:        "workspace-list-agent-c-bearer-secret-0003",
		PersonalityAgentID: agentC.ID,
	}

	call := func(authorization agentevents.LocalRuntimeAuthorization, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, LocalWorkspacesPath, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.localWorkspaces(response, request, authorization)
		return response
	}
	decodePage := func(response *httptest.ResponseRecorder) workspaceListPageWire {
		t.Helper()
		if response.Code != http.StatusOK {
			t.Fatalf("Workspace page status = %d, body=%s", response.Code, response.Body.String())
		}
		var page workspaceListPageWire
		decodeRecorder(t, response, &page)
		return page
	}
	createMany := func(actor participant.Ref, count int, name func(int) string) []Workspace {
		t.Helper()
		created := make([]Workspace, 0, count)
		for i := 0; i < count; i++ {
			item, err := w.store.CreateWorkspace(ctx, name(i), actor)
			if err != nil {
				t.Fatalf("create Workspace %d for %s: %v", i, actor.Key(), err)
			}
			created = append(created, item)
		}
		return created
	}

	// Zero is a valid page and does not synthesize a default Workspace.
	empty := decodePage(call(authC, `{}`))
	if len(empty.Workspaces) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty page = %#v", empty)
	}

	// This Workspace predates the traversal but Agent A does not join it until
	// after page 1. Its low logical key proves that live keyset pagination does
	// not retroactively inject newly active lower identities into later pages.
	earlyWorkspace, err := w.store.CreateWorkspace(ctx, "Early human Workspace", w.humanA)
	if err != nil {
		t.Fatal(err)
	}

	agentAWorkspaces := createMany(w.agentA, 65, func(i int) string {
		return fmt.Sprintf("Agent A Workspace %03d", i)
	})
	// This higher-key non-owner membership is active on page 1 and closes
	// before later pages, proving that each page uses fresh active truth.
	removableWorkspace, err := w.store.CreateWorkspace(ctx, "Removable human Workspace", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	removableInvite, err := w.store.CreateInvite(ctx, removableWorkspace.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.RedeemInvite(ctx, removableInvite.Code, w.agentA); err != nil {
		t.Fatal(err)
	}

	firstResponse := call(authA, `{}`)
	first := decodePage(firstResponse)
	if len(first.Workspaces) != localWorkspaceListPageSize || first.NextCursor == "" {
		t.Fatalf("first page len/cursor = %d/%q", len(first.Workspaces), first.NextCursor)
	}
	firstRetry := call(authA, `{}`)
	if firstRetry.Body.String() != firstResponse.Body.String() {
		t.Fatal("unchanged first-page retry was not deterministic")
	}

	decodedFirstCursor, err := decodeWorkspaceListCursor(first.NextCursor, authA)
	if err != nil {
		t.Fatal(err)
	}
	if decodedFirstCursor.WorkspaceID != first.Workspaces[len(first.Workspaces)-1].WorkspaceID {
		t.Fatalf("cursor Workspace = %s, want page boundary %s",
			decodedFirstCursor.WorkspaceID, first.Workspaces[len(first.Workspaces)-1].WorkspaceID)
	}
	if earlyWorkspace.WorkspaceID >= decodedFirstCursor.WorkspaceID {
		t.Fatalf("early Workspace key %s did not precede cursor %s",
			earlyWorkspace.WorkspaceID, decodedFirstCursor.WorkspaceID)
	}
	if removableWorkspace.WorkspaceID <= decodedFirstCursor.WorkspaceID {
		t.Fatalf("removable Workspace key %s did not follow cursor %s",
			removableWorkspace.WorkspaceID, decodedFirstCursor.WorkspaceID)
	}

	// Recreate the membership tenure of a Workspace already emitted on page 1.
	// The logical Workspace key is unchanged, so it must never reappear later.
	duplicateTargetID := first.Workspaces[0].WorkspaceID
	successorInvite, err := w.store.CreateInvite(ctx, duplicateTargetID, w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	successor, err := w.store.RedeemInvite(ctx, successorInvite.Code, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.TransferOwnership(ctx, duplicateTargetID,
		successor.WorkspaceMemberID, w.agentA); err != nil {
		t.Fatal(err)
	}
	if err := w.store.Leave(ctx, duplicateTargetID, w.agentA); err != nil {
		t.Fatal(err)
	}
	rejoinInvite, err := w.store.CreateInvite(ctx, duplicateTargetID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.RedeemInvite(ctx, rejoinInvite.Code, w.agentA); err != nil {
		t.Fatal(err)
	}
	if err := w.store.Leave(ctx, removableWorkspace.WorkspaceID, w.agentA); err != nil {
		t.Fatal(err)
	}

	// Fresh memberships sort according to their Workspace identity. A lower
	// identity is visible only on restart; a newly-created higher identity is
	// observed by the remainder of this traversal.
	earlyInvite, err := w.store.CreateInvite(ctx, earlyWorkspace.WorkspaceID, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.RedeemInvite(ctx, earlyInvite.Code, w.agentA); err != nil {
		t.Fatal(err)
	}
	newWorkspace, err := w.store.CreateWorkspace(ctx, "Agent A newly joined", w.agentA)
	if err != nil {
		t.Fatal(err)
	}
	if newWorkspace.WorkspaceID <= decodedFirstCursor.WorkspaceID {
		t.Fatalf("new Workspace key %s did not follow cursor %s",
			newWorkspace.WorkspaceID, decodedFirstCursor.WorkspaceID)
	}
	agentAWorkspaces = append(agentAWorkspaces, newWorkspace)

	if _, err := w.store.workspacePageFor(ctx, w.agentA, decodedFirstCursor); err != nil {
		t.Fatalf("direct second-page store read: %v", err)
	}
	secondResponse := call(authA, fmt.Sprintf(`{"cursor":%q}`, first.NextCursor))
	second := decodePage(secondResponse)
	if len(second.Workspaces) != localWorkspaceListPageSize || second.NextCursor == "" {
		t.Fatalf("second page len/cursor = %d/%q", len(second.Workspaces), second.NextCursor)
	}
	secondRetry := call(authA, fmt.Sprintf(`{"cursor":%q}`, first.NextCursor))
	if secondRetry.Body.String() != secondResponse.Body.String() {
		t.Fatal("unchanged later-page retry was not deterministic")
	}
	third := decodePage(call(authA, fmt.Sprintf(`{"cursor":%q}`, second.NextCursor)))
	if len(third.Workspaces) != 2 || third.NextCursor != "" {
		t.Fatalf("third page = len %d, cursor %q", len(third.Workspaces), third.NextCursor)
	}

	seen := make(map[string]struct{}, len(agentAWorkspaces))
	for _, page := range [][]workspaceWire{first.Workspaces, second.Workspaces, third.Workspaces} {
		withinPage := make(map[string]struct{}, len(page))
		for _, item := range page {
			if _, duplicate := withinPage[item.WorkspaceID]; duplicate {
				t.Fatalf("Workspace %s duplicated within one page", item.WorkspaceID)
			}
			withinPage[item.WorkspaceID] = struct{}{}
			if _, duplicate := seen[item.WorkspaceID]; duplicate {
				t.Fatalf("Workspace %s duplicated across stable keyset pages", item.WorkspaceID)
			}
			seen[item.WorkspaceID] = struct{}{}
		}
	}
	if len(seen) != len(agentAWorkspaces) {
		t.Fatalf("paged active Workspace count = %d, want %d", len(seen), len(agentAWorkspaces))
	}
	for _, item := range agentAWorkspaces {
		if _, ok := seen[item.WorkspaceID]; !ok {
			t.Fatalf("active Workspace %s was omitted", item.WorkspaceID)
		}
	}
	if _, injected := seen[earlyWorkspace.WorkspaceID]; injected {
		t.Fatalf("new lower-key membership %s was retroactively injected", earlyWorkspace.WorkspaceID)
	}
	if _, leaked := seen[removableWorkspace.WorkspaceID]; leaked {
		t.Fatalf("closed membership %s remained visible", removableWorkspace.WorkspaceID)
	}
	if _, included := seen[newWorkspace.WorkspaceID]; !included {
		t.Fatalf("new higher-key membership %s was omitted", newWorkspace.WorkspaceID)
	}
	duplicateCount := 0
	for _, page := range [][]workspaceWire{first.Workspaces, second.Workspaces, third.Workspaces} {
		for _, item := range page {
			if item.WorkspaceID == duplicateTargetID {
				duplicateCount++
			}
		}
	}
	if duplicateCount != 1 {
		t.Fatalf("leave/rejoin Workspace %s appeared %d times, want once",
			duplicateTargetID, duplicateCount)
	}
	fresh := decodePage(call(authA, `{}`))
	freshHasEarly := false
	for _, item := range fresh.Workspaces {
		if item.WorkspaceID == earlyWorkspace.WorkspaceID {
			freshHasEarly = true
			break
		}
	}
	if !freshHasEarly {
		t.Fatalf("new lower-key membership %s was not visible after restart",
			earlyWorkspace.WorkspaceID)
	}

	// Worst-case legal name escaping, including a continuation cursor, remains
	// below the 64 KiB local-control wire cap. '<' uses Go JSON's six-byte
	// \u003c escape.
	worstName := strings.Repeat("<", maxWorkspaceNameChars)
	createMany(w.agentB, localWorkspaceListPageSize+1, func(int) string { return worstName })
	worstResponse := call(authB, `{}`)
	worst := decodePage(worstResponse)
	if len(worst.Workspaces) != localWorkspaceListPageSize || worst.NextCursor == "" {
		t.Fatalf("worst-case page len/cursor = %d/%q", len(worst.Workspaces), worst.NextCursor)
	}
	for _, item := range worst.Workspaces {
		if item.Name != worstName {
			t.Fatalf("actor-scoped page exposed unexpected Workspace %s (%q)",
				item.WorkspaceID, item.Name)
		}
	}
	if worstResponse.Body.Len() >= localWorkspaceListResponseBytes {
		t.Fatalf("worst-case escaped response = %d bytes, cap %d",
			worstResponse.Body.Len(), localWorkspaceListResponseBytes)
	}
	if !strings.Contains(worstResponse.Body.String(), `\u003c`) {
		t.Fatal("worst-case fixture did not exercise JSON escaping")
	}

	// Exact-page returns no speculative cursor. Adding one later membership
	// then makes the page+1 boundary produce one continuation item.
	createMany(agentC, localWorkspaceListPageSize, func(i int) string {
		return fmt.Sprintf("Agent C Workspace %03d", i)
	})
	cExact := decodePage(call(authC, `{}`))
	if len(cExact.Workspaces) != localWorkspaceListPageSize || cExact.NextCursor != "" {
		t.Fatalf("exact page = %d/%q", len(cExact.Workspaces), cExact.NextCursor)
	}
	createMany(agentC, 1, func(int) string { return "Agent C Workspace extra" })
	cFirst := decodePage(call(authC, `{}`))
	if len(cFirst.Workspaces) != localWorkspaceListPageSize || cFirst.NextCursor == "" {
		t.Fatalf("page+1 first page = %d/%q", len(cFirst.Workspaces), cFirst.NextCursor)
	}
	cLast := decodePage(call(authC, fmt.Sprintf(`{"cursor":%q}`, cFirst.NextCursor)))
	if len(cLast.Workspaces) != 1 || cLast.NextCursor != "" {
		t.Fatalf("page+1 final page = %d/%q", len(cLast.Workspaces), cLast.NextCursor)
	}

	// Invalid, oversized, nullable, tampered, and cross-actor cursors all fail
	// before the store query and cannot mutate Workspace tables.
	tampered := []byte(first.NextCursor)
	if tampered[10] == 'A' {
		tampered[10] = 'B'
	} else {
		tampered[10] = 'A'
	}
	beforeInvalid := tableCounts(t, ctx, w.pool)
	invalidRequests := []struct {
		authorization agentevents.LocalRuntimeAuthorization
		body          string
	}{
		{authA, fmt.Sprintf(`{"cursor":%q}`, string(tampered))},
		{authA, fmt.Sprintf(`{"cursor":%q}`, strings.Repeat("A", workspaceListCursorEncodedBytes+1))},
		{authA, fmt.Sprintf(`{"cursor":%q}`, strings.Repeat("A", workspaceListCursorEncodedBytes-1)+"!")},
		{authA, `{"cursor":null}`},
		{authB, fmt.Sprintf(`{"cursor":%q}`, first.NextCursor)},
	}
	for _, test := range invalidRequests {
		response := call(test.authorization, test.body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid cursor status = %d, body=%s", response.Code, response.Body.String())
		}
	}
	if afterInvalid := tableCounts(t, ctx, w.pool); afterInvalid != beforeInvalid {
		t.Fatalf("invalid cursor changed Workspace state: before=%v after=%v", beforeInvalid, afterInvalid)
	}
}
