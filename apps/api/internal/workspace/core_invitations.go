package workspace

// Core state-service tool effects for Workspace invitations. When these
// effects are registered on the agentstate store, the secretary's
// workspace_invitation.list / workspace_invitation.accept calls execute
// inside the operation-claim transaction, so the operation record and the
// membership/invite-commit commit or roll back together. Local-control gave
// the runtime membership+invite atomicity; the core path additionally pairs
// that commit with the durable operation receipt — a record of the tenure
// at the time of the result, not a promise of current membership.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	// WorkspaceInvitationListTool and WorkspaceInvitationAcceptTool are the
	// model-facing names the core advertises once the effects are
	// registered — the same list-then-accept contract the runtime executor
	// exposed to the secretary.
	WorkspaceInvitationListTool   = "workspace_invitation.list"
	WorkspaceInvitationAcceptTool = "workspace_invitation.accept"
)

// workspaceQuerier is satisfied by *pgxpool.Pool (local-control handlers and
// tests) and by pgx.Tx (core claim transactions), letting the targeted
// invitation page run on either.
type workspaceQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// CoreInvitationToolEffects returns the delegated ToolEffects for the
// workspace_invitation.* tools. Registration alone makes them claimable on
// the agentstate store; the core then advertises them to the model.
func (s *Store) CoreInvitationToolEffects() map[string]agentstate.ToolEffect {
	return map[string]agentstate.ToolEffect{
		WorkspaceInvitationListTool:   {Apply: s.applyInvitationList},
		WorkspaceInvitationAcceptTool: {Apply: s.applyInvitationAccept},
	}
}

func (s *Store) applyInvitationList(
	ctx context.Context,
	tx pgx.Tx,
	personaID, _ string,
	request map[string]any,
) (map[string]any, error) {
	var after *workspaceInvitationListCursorPosition
	if raw, present := request["cursor"]; present && raw != nil {
		rawCursor, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("%w: workspace_invitation.list cursor must be a string", agentstate.ErrBadRequest)
		}
		position, err := decodeCoreInvitationListCursor(rawCursor, personaID)
		if err != nil {
			return nil, coreInvitationFailure(err)
		}
		after = position
	}
	page, err := s.targetedInvitationPageFor(ctx, tx, participant.PersonalityAgent(personaID), after)
	if err != nil {
		return nil, coreInvitationFailure(err)
	}
	items := make([]map[string]any, 0, len(page.Items))
	for _, item := range page.Items {
		items = append(items, map[string]any{
			"invitation_id":  item.Invitation.InvitationID,
			"workspace_id":   item.Invitation.WorkspaceID,
			"workspace_name": item.Invitation.WorkspaceName,
			"expires_at":     item.Invitation.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"created_at":     item.Invitation.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	response := map[string]any{"invitations": items}
	if page.HasMore {
		next, encodeErr := encodeCoreInvitationListCursor(page.Items[len(page.Items)-1].Position, personaID)
		if encodeErr != nil {
			return nil, coreInvitationFailure(encodeErr)
		}
		response["next_cursor"] = next
	}
	return response, nil
}

func (s *Store) applyInvitationAccept(
	ctx context.Context,
	tx pgx.Tx,
	personaID, _ string,
	request map[string]any,
) (map[string]any, error) {
	invitationID, _ := request["invitation_id"].(string)
	if invitationID == "" {
		return nil, fmt.Errorf("%w: workspace_invitation.accept requires invitation_id", agentstate.ErrBadRequest)
	}
	membership, err := s.AcceptTargetedInvitationInTx(
		ctx, tx, invitationID, participant.PersonalityAgent(personaID))
	if err != nil {
		return nil, coreInvitationFailure(err)
	}
	// The receipt describes the tenure as committed at this result, not a
	// promise of current membership: a same-target retry of a consumed
	// invite returns the recorded tenure, which may already be closed
	// (left_at set, e.g. the Human removed the secretary). The nullable
	// left_at matches the local-control membership wire so the model can
	// distinguish "member" from "recorded, closed tenure".
	var leftAt any
	if membership.LeftAt != nil {
		leftAt = membership.LeftAt.UTC().Format(time.RFC3339Nano)
	}
	return map[string]any{
		"workspace_member_id": membership.WorkspaceMemberID,
		"workspace_id":        membership.WorkspaceID,
		"participant": map[string]any{
			"kind": string(membership.Participant.Kind),
			"id":   membership.Participant.ID,
		},
		"display_name": membership.DisplayName,
		"owner":        membership.Owner,
		"role_ids":     membership.RoleIDs,
		"joined_at":    membership.JoinedAt.UTC().Format(time.RFC3339Nano),
		"left_at":      leftAt,
	}, nil
}

// coreInvitationFailure maps domain rejections to ErrBadRequest so the claim
// records a deterministic tool error and the turn continues; unknown failures
// pass through transient so the host can retry the claim.
func coreInvitationFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInviteUnavailable) ||
		errors.Is(err, ErrAlreadyMember) ||
		errors.Is(err, ErrInvalidWorkspaceInvitationListCursor) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, agentstate.ErrBadRequest) {
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	return err
}

// The local-control cursor MAC is keyed by the caller's bearer secret; the
// core claim has no per-request secret, so the core cursor is keyed by the
// domain plus the fixed persona identity. It is an opaque integrity tag for
// the model's own pagination position only — authority never derives from
// it: the page query always filters to the claiming persona.
const coreInvitationListCursorDomain = "sumi-workspace-invitation-list-cursor-core-v1\x00"

func encodeCoreInvitationListCursor(position workspaceInvitationListCursorPosition, personaID string) (string, error) {
	if !isCanonicalUUIDv7(position.InvitationID) {
		return "", ErrInvalidWorkspaceInvitationListCursor
	}
	invitationID, err := uuid.Parse(position.InvitationID)
	if err != nil {
		return "", ErrInvalidWorkspaceInvitationListCursor
	}
	payload := make([]byte, workspaceInvitationListCursorPayloadBytes)
	payload[0] = workspaceInvitationListCursorVersion
	copy(payload[9:], invitationID[:])
	wire := append(payload, coreInvitationListCursorMAC(personaID, payload)...)
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

func decodeCoreInvitationListCursor(raw string, personaID string) (*workspaceInvitationListCursorPosition, error) {
	if len(raw) != workspaceInvitationListCursorEncodedBytes {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	wire, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(wire) != workspaceInvitationListCursorBytes {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	payload := wire[:workspaceInvitationListCursorPayloadBytes]
	if !hmac.Equal(coreInvitationListCursorMAC(personaID, payload), wire[workspaceInvitationListCursorPayloadBytes:]) {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	if payload[0] != workspaceInvitationListCursorVersion {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	for _, reserved := range payload[1:9] {
		if reserved != 0 {
			return nil, ErrInvalidWorkspaceInvitationListCursor
		}
	}
	invitationID, err := uuid.FromBytes(payload[9:])
	if err != nil || invitationID.Version() != 7 || invitationID.Variant() != uuid.RFC4122 {
		return nil, ErrInvalidWorkspaceInvitationListCursor
	}
	return &workspaceInvitationListCursorPosition{InvitationID: invitationID.String()}, nil
}

func coreInvitationListCursorMAC(personaID string, payload []byte) []byte {
	mac := hmac.New(sha256.New, []byte(coreInvitationListCursorDomain))
	mac.Write([]byte(personaID))
	mac.Write([]byte{0})
	mac.Write(payload)
	return mac.Sum(nil)
}
