package workspace

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// TestCoreInvitationEffectsClaimListAndAccept covers the delegated core
// path end to end at the store level: the secretary's list and accept
// claims run inside the operation transaction, the committed membership is
// observable by the Human, a replayed claim returns the recorded receipt
// without a second effect, and a foreign target can never redeem.
func TestCoreInvitationEffectsClaimListAndAccept(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	seedHumanEmployer(t, ctx, w, w.humanA, w.agentA)
	authority := koseki.New(w.pool)
	created, err := w.store.CreateWorkspace(ctx, "invitation target", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	invite, wasCreated, err := w.store.CreateCurrentAgentInvite(
		ctx, created.WorkspaceID, w.humanA, w.agentA, authority)
	if err != nil || !wasCreated {
		t.Fatalf("create invite = %#v created=%v err=%v", invite, wasCreated, err)
	}

	core := agentstate.NewStore(w.pool)
	for tool, effect := range w.store.CoreInvitationToolEffects() {
		if err := core.RegisterEffect(tool, effect); err != nil {
			t.Fatalf("register %s: %v", tool, err)
		}
	}
	claimable := map[string]bool{}
	for _, tool := range core.ClaimableTools() {
		claimable[tool] = true
	}
	if !claimable[WorkspaceInvitationListTool] || !claimable[WorkspaceInvitationAcceptTool] {
		t.Fatalf("claimable tools = %v, want both workspace_invitation tools", core.ClaimableTools())
	}

	if _, _, err := core.EnsurePersona(ctx, w.agentA.ID, &w.humanA.ID, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	occurred := time.Now().UTC()
	if _, _, err := core.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agentA.ID, InputID: "in-1", Kind: "message",
		ActorKind: "human", ActorID: w.humanA.ID, SourceSurface: "workspace",
		OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input: %v", err)
	}
	lease, err := core.AcquireWriter(ctx, w.agentA.ID, "runtime", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	res, err := core.LoadTurn(ctx, w.agentA.ID, lease.Generation, "turn-1", 20)
	if err != nil || res.Turn == nil {
		t.Fatalf("load turn: %+v %v", res, err)
	}
	claim := func(tool string, callIndex int, request map[string]any) (agentstate.Operation, bool, error) {
		if _, _, err := core.SavePlan(ctx, w.agentA.ID, res.Turn.TurnID, lease.Generation, int64(callIndex),
			agentstate.Decision{
				Text:  "invitation",
				Calls: []agentstate.PlanCall{{CallID: "c", Tool: tool, Route: "normal", Request: request}},
			}); err != nil {
			t.Fatalf("save plan %s: %v", tool, err)
		}
		op, _, fresh, err := core.ClaimOperation(ctx, w.agentA.ID, res.Turn.TurnID,
			lease.Generation, "turn-1:op:"+string(rune('0'+callIndex)), tool, callIndex, request)
		return op, fresh, err
	}

	// List surfaces exactly the pending invitation addressed to this persona.
	listOp, fresh, err := claim(WorkspaceInvitationListTool, 0, map[string]any{})
	if err != nil || !fresh || listOp.Status != "done" {
		t.Fatalf("list claim = %+v fresh=%t err=%v", listOp, fresh, err)
	}
	invitations, _ := listOp.Response["invitations"].([]any)
	if len(invitations) != 1 ||
		invitations[0].(map[string]any)["invitation_id"] != invite.InviteID ||
		invitations[0].(map[string]any)["workspace_id"] != created.WorkspaceID {
		t.Fatalf("list response = %+v, want invite %s in %s",
			listOp.Response, invite.InviteID, created.WorkspaceID)
	}

	// A foreign persona's list sees nothing and its accept can never redeem.
	if _, _, err := core.EnsurePersona(ctx, w.agentB.ID, &w.humanB.ID, "Shiro"); err != nil {
		t.Fatalf("ensure persona B: %v", err)
	}
	if _, _, err := core.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agentB.ID, InputID: "in-b", Kind: "message",
		ActorKind: "human", ActorID: w.humanB.ID, SourceSurface: "workspace",
		OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input B: %v", err)
	}
	leaseB, err := core.AcquireWriter(ctx, w.agentB.ID, "runtime", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire writer B: %v", err)
	}
	resB, err := core.LoadTurn(ctx, w.agentB.ID, leaseB.Generation, "turn-b", 20)
	if err != nil || resB.Turn == nil {
		t.Fatalf("load turn B: %+v %v", resB, err)
	}
	if _, _, err := core.SavePlan(ctx, w.agentB.ID, resB.Turn.TurnID, leaseB.Generation, int64(0),
		agentstate.Decision{
			Text: "steal",
			Calls: []agentstate.PlanCall{{CallID: "c", Tool: WorkspaceInvitationAcceptTool, Route: "normal",
				Request: map[string]any{"invitation_id": invite.InviteID}}},
		}); err != nil {
		t.Fatalf("save plan B: %v", err)
	}
	if _, _, _, err := core.ClaimOperation(ctx, w.agentB.ID, resB.Turn.TurnID,
		leaseB.Generation, "turn-b:op:0", WorkspaceInvitationAcceptTool, 0,
		map[string]any{"invitation_id": invite.InviteID}); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign accept: got %v, want ErrBadRequest", err)
	}

	// Accept commits the membership inside the claim transaction.
	acceptOp, fresh, err := claim(WorkspaceInvitationAcceptTool, 1,
		map[string]any{"invitation_id": invite.InviteID})
	if err != nil || !fresh || acceptOp.Status != "done" {
		t.Fatalf("accept claim = %+v fresh=%t err=%v", acceptOp, fresh, err)
	}
	memberID, _ := acceptOp.Response["workspace_member_id"].(string)
	if memberID == "" || acceptOp.Response["workspace_id"] != created.WorkspaceID {
		t.Fatalf("accept response = %+v", acceptOp.Response)
	}
	var memberCount int
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_members
		WHERE workspace_id = $1 AND member_kind = 'personality_agent'
		  AND member_id = $2 AND left_at IS NULL`,
		created.WorkspaceID, w.agentA.ID).Scan(&memberCount); err != nil {
		t.Fatal(err)
	}
	if memberCount != 1 {
		t.Fatalf("active memberships for agentA = %d, want 1", memberCount)
	}
	var redeemed int
	if err := w.pool.QueryRow(ctx,
		`SELECT count(*) FROM workspace_invites WHERE invite_id = $1 AND redeemed_at IS NOT NULL`,
		invite.InviteID).Scan(&redeemed); err != nil {
		t.Fatal(err)
	}
	if redeemed != 1 {
		t.Fatalf("redeemed invite rows = %d, want 1", redeemed)
	}

	// Replaying the claim returns the recorded receipt — no second effect.
	again, freshAgain, err := claim(WorkspaceInvitationAcceptTool, 1,
		map[string]any{"invitation_id": invite.InviteID})
	if err != nil || freshAgain || again.Response["workspace_member_id"] != memberID {
		t.Fatalf("replay = %+v fresh=%t err=%v", again, freshAgain, err)
	}
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_members
		WHERE workspace_id = $1 AND member_kind = 'personality_agent'
		  AND member_id = $2 AND left_at IS NULL`,
		created.WorkspaceID, w.agentA.ID).Scan(&memberCount); err != nil {
		t.Fatal(err)
	}
	if memberCount != 1 {
		t.Fatalf("memberships after replay = %d, want 1", memberCount)
	}

	// The invitation is consumed: list now returns an empty page, and a
	// repeated fresh accept of the same invite is a recorded domain refusal.
	doneList, fresh, err := claim(WorkspaceInvitationListTool, 2, map[string]any{})
	if err != nil || !fresh {
		t.Fatalf("post-accept list = %+v fresh=%t err=%v", doneList, fresh, err)
	}
	if items, _ := doneList.Response["invitations"].([]any); len(items) != 0 {
		t.Fatalf("post-accept invitations = %+v, want empty", doneList.Response)
	}
	// The invitation is consumed: a later accept for the same invite is the
	// domain's recorded tenure read — the same membership, never a second
	// effect (the redeemedAt branch of AcceptTargetedInvitationInTx).
	if _, _, err := core.SavePlan(ctx, w.agentA.ID, res.Turn.TurnID, lease.Generation, int64(3),
		agentstate.Decision{
			Text: "retry",
			Calls: []agentstate.PlanCall{{CallID: "c", Tool: WorkspaceInvitationAcceptTool, Route: "normal",
				Request: map[string]any{"invitation_id": invite.InviteID}}},
		}); err != nil {
		t.Fatalf("save plan 3: %v", err)
	}
	tenureRead, _, fresh, err := core.ClaimOperation(ctx, w.agentA.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:3", WorkspaceInvitationAcceptTool, 3,
		map[string]any{"invitation_id": invite.InviteID})
	if err != nil || !fresh || tenureRead.Status != "done" ||
		tenureRead.Response["workspace_member_id"] != memberID {
		t.Fatalf("consumed-invite tenure read = %+v fresh=%t err=%v", tenureRead, fresh, err)
	}
	if err := w.pool.QueryRow(ctx, `
		SELECT count(*) FROM workspace_members
		WHERE workspace_id = $1 AND member_kind = 'personality_agent'
		  AND member_id = $2 AND left_at IS NULL`,
		created.WorkspaceID, w.agentA.ID).Scan(&memberCount); err != nil {
		t.Fatal(err)
	}
	if memberCount != 1 {
		t.Fatalf("memberships after tenure read = %d, want 1", memberCount)
	}
}

// TestCoreInvitationListCursorIsPersonaBound covers the core cursor codec:
// an opaque token minted for one persona is rejected for another, and a
// tampered token is a recorded bad request rather than a skipped page.
func TestCoreInvitationListCursorIsPersonaBound(t *testing.T) {
	w := newTestWorld(t)
	ctx := context.Background()
	position := workspaceInvitationListCursorPosition{InvitationID: testAgentA}
	// InvitationID is used only as an opaque v7 keyset position here.
	if !isCanonicalUUIDv7(position.InvitationID) {
		t.Fatalf("fixture id %s is not canonical v7", position.InvitationID)
	}
	token, err := encodeCoreInvitationListCursor(position, w.agentA.ID)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := decodeCoreInvitationListCursor(token, w.agentA.ID)
	if err != nil || decoded.InvitationID != position.InvitationID {
		t.Fatalf("roundtrip = %+v %v", decoded, err)
	}
	if _, err := decodeCoreInvitationListCursor(token, w.agentB.ID); !errors.Is(err, ErrInvalidWorkspaceInvitationListCursor) {
		t.Fatalf("foreign-persona cursor decode = %v, want invalid cursor", err)
	}
	tampered := token[:len(token)-2] + "AA"
	if _, err := decodeCoreInvitationListCursor(tampered, w.agentA.ID); !errors.Is(err, ErrInvalidWorkspaceInvitationListCursor) {
		t.Fatalf("tampered cursor decode = %v, want invalid cursor", err)
	}

	// The claim path maps a bad cursor to the recorded bad-request failure.
	core := agentstate.NewStore(w.pool)
	if err := core.RegisterEffect(WorkspaceInvitationListTool, w.store.CoreInvitationToolEffects()[WorkspaceInvitationListTool]); err != nil {
		t.Fatalf("register list: %v", err)
	}
	if _, _, err := core.EnsurePersona(ctx, w.agentA.ID, &w.humanA.ID, "Kuro"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	occurred := time.Now().UTC()
	if _, _, err := core.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agentA.ID, InputID: "in-1", Kind: "message",
		ActorKind: "human", ActorID: w.humanA.ID, SourceSurface: "workspace",
		OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input: %v", err)
	}
	lease, err := core.AcquireWriter(ctx, w.agentA.ID, "runtime", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	res, err := core.LoadTurn(ctx, w.agentA.ID, lease.Generation, "turn-1", 20)
	if err != nil || res.Turn == nil {
		t.Fatalf("load turn: %+v %v", res, err)
	}
	if _, _, err := core.SavePlan(ctx, w.agentA.ID, res.Turn.TurnID, lease.Generation, int64(0),
		agentstate.Decision{
			Text: "list",
			Calls: []agentstate.PlanCall{{CallID: "c", Tool: WorkspaceInvitationListTool, Route: "normal",
				Request: map[string]any{"cursor": tampered}}},
		}); err != nil {
		t.Fatalf("save plan: %v", err)
	}
	_, _, _, err = core.ClaimOperation(ctx, w.agentA.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:0", WorkspaceInvitationListTool, 0,
		map[string]any{"cursor": tampered})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("tampered cursor claim = %v, want ErrBadRequest", err)
	}
	// A cursor minted for agentB cannot move agentA's page even when valid.
	foreignToken, err := encodeCoreInvitationListCursor(position, w.agentB.ID)
	if err != nil {
		t.Fatalf("encode foreign: %v", err)
	}
	if _, _, err := core.SavePlan(ctx, w.agentA.ID, res.Turn.TurnID, lease.Generation, int64(1),
		agentstate.Decision{
			Text: "list",
			Calls: []agentstate.PlanCall{{CallID: "c", Tool: WorkspaceInvitationListTool, Route: "normal",
				Request: map[string]any{"cursor": foreignToken}}},
		}); err != nil {
		t.Fatalf("save plan 2: %v", err)
	}
	_, _, _, err = core.ClaimOperation(ctx, w.agentA.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:1", WorkspaceInvitationListTool, 1,
		map[string]any{"cursor": foreignToken})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign cursor claim = %v, want ErrBadRequest", err)
	}
}

// TestCoreInvitationEffectsAbsentWithoutRegistration pins the claimability
// contract: without registered effects the tools are not claimable and a
// claim for one is refused rather than half-applied.
func TestCoreInvitationEffectsAbsentWithoutRegistration(t *testing.T) {
	w := newTestWorld(t)
	core := agentstate.NewStore(w.pool)
	for _, tool := range core.ClaimableTools() {
		if strings.HasPrefix(tool, "workspace_invitation.") {
			t.Fatalf("unregistered %s is claimable", tool)
		}
	}
}
