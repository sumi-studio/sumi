package agentevents

import (
	"fmt"
	"regexp"

	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
)

// InboundProvenanceV1 is the frozen, surface-neutral admission snapshot shared
// by messaging and the agent runtime. It deliberately contains metadata only:
// the message body stays behind the authorized place-open boundary.
type InboundProvenanceV1 struct {
	Version            uint8              `json:"version"`
	TenantID           string             `json:"tenant_id"`
	PersonalityAgentID string             `json:"personality_agent_id"`
	Actor              InboundActorRef    `json:"actor"`
	Source             InboundSource      `json:"source"`
	Authority          AdmissionAuthority `json:"authority"`
}

type InboundActorRef struct {
	Kind               string `json:"kind"`
	HumanID            string `json:"human_id,omitempty"`
	PersonalityAgentID string `json:"personality_agent_id,omitempty"`
}

type InboundSource struct {
	Surface     string                     `json:"surface"`
	WorkspaceID *string                    `json:"workspace_id,omitempty"`
	Place       *InboundPlaceRef           `json:"place,omitempty"`
	Delivery    *InboundDeliveryProvenance `json:"delivery,omitempty"`
}

type InboundPlaceRef struct {
	Kind      string `json:"kind"`
	ChannelID string `json:"channel_id,omitempty"`
	DMID      string `json:"dm_id,omitempty"`
}

type InboundDeliveryProvenance struct {
	MessageID     string            `json:"message_id"`
	Seq           int64             `json:"seq"`
	Addressees    []InboundActorRef `json:"addressees"`
	TriggerReason string            `json:"trigger_reason"`
	Urgency       string            `json:"urgency"`
	CorrelationID *string           `json:"correlation_id"`
	CausationID   *string           `json:"causation_id"`
}

type AdmissionAuthority struct {
	Basis      string  `json:"basis"`
	DecisionID *string `json:"decision_id"`
}

type AttentionCandidate struct {
	Kind         string              `json:"kind"`
	CandidateID  string              `json:"candidate_id"`
	CandidateSeq uint64              `json:"candidate_seq"`
	Provenance   InboundProvenanceV1 `json:"provenance"`
	UnreadRange  UnreadRange         `json:"unread_range"`
	ArrivalTime  string              `json:"arrival_time"`
	Attachments  map[string]any      `json:"attachments"`
}

type UnreadRange struct {
	PlaceSeqFrom uint64 `json:"place_seq_from"`
	PlaceSeqTo   uint64 `json:"place_seq_to"`
}

var inboundProvenanceID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$`)

// Validate keeps this independently delivered payload closed. The full agent
// command envelope continues to own its older direct-chat shape until that
// transport is cut over; the local attention boundary nevertheless uses this
// frozen v1 value now.
func (p InboundProvenanceV1) Validate() error {
	if p.Version != 1 || !inboundProvenanceID.MatchString(p.TenantID) {
		return fmt.Errorf("invalid inbound provenance header")
	}
	if err := ValidatePersonalityAgentID(p.PersonalityAgentID); err != nil {
		return err
	}
	if err := p.Actor.Validate(); err != nil {
		return err
	}
	if p.Source.Surface != "messaging" || p.Source.Place == nil || p.Source.Delivery == nil {
		return fmt.Errorf("inbound provenance must carry a messaging delivery")
	}
	if p.Source.WorkspaceID == nil || !inboundProvenanceID.MatchString(*p.Source.WorkspaceID) {
		return fmt.Errorf("inbound provenance workspace_id is invalid")
	}
	if err := p.Source.Place.Validate(); err != nil {
		return err
	}
	if err := p.Source.Delivery.Validate(); err != nil {
		return err
	}
	if p.Authority.Basis != "place_membership" || p.Authority.DecisionID != nil {
		return fmt.Errorf("inbound provenance authority is invalid")
	}
	return nil
}

func (a InboundActorRef) Validate() error {
	switch a.Kind {
	case "human":
		if a.PersonalityAgentID != "" || !canonicalid.IsUUIDv7(a.HumanID) {
			return fmt.Errorf("invalid human actor")
		}
	case "personality_agent":
		if a.HumanID != "" {
			return fmt.Errorf("invalid personality-agent actor")
		}
		if err := ValidatePersonalityAgentID(a.PersonalityAgentID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown inbound actor kind %q", a.Kind)
	}
	return nil
}

func (p InboundPlaceRef) Validate() error {
	switch p.Kind {
	case "channel":
		if p.DMID != "" || !inboundProvenanceID.MatchString(p.ChannelID) {
			return fmt.Errorf("invalid channel place")
		}
	case "dm", "group_dm":
		if p.ChannelID != "" || !inboundProvenanceID.MatchString(p.DMID) {
			return fmt.Errorf("invalid direct-message place")
		}
	default:
		return fmt.Errorf("unknown inbound place kind %q", p.Kind)
	}
	return nil
}

func (d InboundDeliveryProvenance) Validate() error {
	if !inboundProvenanceID.MatchString(d.MessageID) || d.Seq < 1 || d.Seq > int64(maxJSONSafeInteger) || d.Addressees == nil || len(d.Addressees) > 64 {
		return fmt.Errorf("invalid inbound delivery")
	}
	for _, addressee := range d.Addressees {
		if err := addressee.Validate(); err != nil {
			return err
		}
	}
	if d.TriggerReason != "mention" && d.TriggerReason != "direct_message" && d.TriggerReason != "place_activity" {
		return fmt.Errorf("unknown trigger reason %q", d.TriggerReason)
	}
	if d.Urgency != "urgent" && d.Urgency != "normal" && d.Urgency != "fyi" {
		return fmt.Errorf("unknown urgency %q", d.Urgency)
	}
	return nil
}
