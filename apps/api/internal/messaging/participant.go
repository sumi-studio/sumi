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

import "github.com/sumi-studio/sumi/apps/api/internal/participant"

// ParticipantKind discriminates the participant sum type. It will grow "app"
// (non-personality tools/automations) later; consumers must treat unknown
// kinds fail-closed (契約ドラフト: Message author拡張).
type ParticipantKind = participant.Kind

const (
	KindHuman            = participant.KindHuman
	KindPersonalityAgent = participant.KindPersonalityAgent
)

// ParticipantRef identifies one participant: a Human or a PersonalityAgent.
// IDs are canonical lowercase hyphenated UUIDv7 (the 戸籍 grammar, shared with
// agentevents so the same string is valid on every side of the boundary).
type ParticipantRef = participant.Ref

// Human returns a ParticipantRef for a 戸籍 HumanId.
func Human(humanID string) ParticipantRef {
	return participant.Human(humanID)
}

// PersonalityAgent returns a ParticipantRef for a PersonalityAgentId.
func PersonalityAgent(agentID string) ParticipantRef {
	return participant.PersonalityAgent(agentID)
}
