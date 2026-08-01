// Package messaging implements the shared messaging surface store (ADR 0011,
// docs/messaging-contracts-draft.md): workspaces, places (channel / dm /
// group_dm), per-place monotonic seq, messages with admission-time mention
// resolution, and monotonic read markers.
//
// Humans and PersonalityAgents are the same "participant" throughout — the
// permission model, the read cursor, and the message author all use the one
// ParticipantRef shape (AX: the agent tool path and the human UI path go
// through the identical store, so neither side can hold a capability the
// other lacks).
package messaging

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// ParticipantKind discriminates the participant sum type. It will grow "app"
// (non-personality tools/automations) later; consumers must treat unknown
// kinds fail-closed (契約ドラフト: Message author拡張).
type ParticipantKind string

const (
	KindHuman            ParticipantKind = "human"
	KindPersonalityAgent ParticipantKind = "personality_agent"
)

// ParticipantRef identifies one participant: a Human or a PersonalityAgent.
// IDs are canonical lowercase hyphenated UUIDv7 (the 戸籍 grammar, shared with
// agentevents so the same string is valid on every side of the boundary).
type ParticipantRef struct {
	Kind ParticipantKind
	ID   string
}

// Human returns a ParticipantRef for a 戸籍 HumanId.
func Human(humanID string) ParticipantRef {
	return ParticipantRef{Kind: KindHuman, ID: humanID}
}

// PersonalityAgent returns a ParticipantRef for a PersonalityAgentId.
func PersonalityAgent(agentID string) ParticipantRef {
	return ParticipantRef{Kind: KindPersonalityAgent, ID: agentID}
}

// Validate rejects unknown kinds and non-canonical IDs. Both human and agent
// IDs share the UUIDv7 grammar, so the kind is required to tell them apart —
// never treat the bare ID as globally unique across kinds.
func (p ParticipantRef) Validate() error {
	switch p.Kind {
	case KindHuman:
		id, err := uuid.Parse(p.ID)
		if err != nil || id.String() != p.ID || id.Version() != 7 || id.Variant() != uuid.RFC4122 {
			return fmt.Errorf("human_id must be a canonical lowercase UUIDv7")
		}
		return nil
	case KindPersonalityAgent:
		return agentevents.ValidatePersonalityAgentID(p.ID)
	default:
		return fmt.Errorf("unknown participant kind %q", p.Kind)
	}
}

// Key returns the stable map key "human:<id>" / "personality_agent:<id>",
// matching ActorRef.key() on the agent side and participantKey in the web
// model.
func (p ParticipantRef) Key() string {
	return string(p.Kind) + ":" + p.ID
}
