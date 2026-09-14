package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// AgentAttentionGateway is the internal Messaging-to-runtime adapter for the
// legacy agentevents provenance contract. That contract cannot express
// edited/deleted message changes: such rows fail clear — terminally
// suppressed as unsupported_route — rather than degrade into an unmarked
// external_event (a deletion would otherwise arrive as an empty message).
// The supported intake route is CoreAttentionDelivery, selected when
// SUMI_CORE_STATE_TOKEN is configured. Merely constructing the adapter does
// not activate delivery; the lifecycle owner runs the outbox only after the
// runtime and browser audience contract are deployed.
type AgentAttentionGateway struct {
	Gateway  *agentevents.DurableGateway
	Spawner  agentevents.DirectChatSpawner
	TenantID string
}

// errUnsupportedAttentionEvent marks an attention event this delivery route
// cannot represent truthfully. The drain treats it as terminal so the row
// keeps its frozen payload with an inspectable reason instead of retrying
// forever or claiming a delivery that silently lost its semantics.
var errUnsupportedAttentionEvent = errors.New("attention event is not representable on this delivery route")

var _ AgentAttentionDelivery = (*AgentAttentionGateway)(nil)

func (a *AgentAttentionGateway) Prepare(ctx context.Context, paID string) (func(), error) {
	if a.Gateway == nil {
		return nil, errors.New("attention gateway is unavailable")
	}
	return a.Gateway.PrepareAttention(ctx, a.Spawner, paID)
}

func (a *AgentAttentionGateway) input(event AgentAttentionEvent) (agentevents.IncomingProvenance, json.RawMessage, error) {
	if event.Change != "" {
		return agentevents.IncomingProvenance{}, nil, errUnsupportedAttentionEvent
	}
	source := agentevents.ProvenanceSource{
		Surface: "messaging", EventID: event.EventID, Kind: event.Kind,
		WorkspaceID: event.WorkspaceID, InstallationID: event.InstallationID,
		AuthorityEpoch: uint64(event.AuthorityEpoch),
		Place:          &agentevents.ProvenancePlace{ID: event.Place.ID, Kind: event.Place.Kind, Name: event.Place.Name},
		MessageID:      event.MessageID, MessageRevision: uint64(event.MessageRevision), MessageSeq: uint64(event.MessageSeq),
		ReplyToMessageID: event.ReplyToMessageID,
		OccurredAt:       event.OccurredAt.UTC().Format(time.RFC3339Nano), MarkerID: event.MarkerID,
	}
	if event.DueAt != nil {
		source.DueAt = event.DueAt.UTC().Format(time.RFC3339Nano)
	}
	if event.PollVote != nil {
		options := make([]agentevents.ProvenancePollOption, 0, len(event.PollVote.SelectedOptions))
		for _, option := range event.PollVote.SelectedOptions {
			options = append(options, agentevents.ProvenancePollOption{OptionID: option.OptionID, Text: option.Text})
		}
		source.PollVote = &agentevents.ProvenancePollVote{
			PollRevision: uint64(event.PollVote.PollRevision), Question: event.PollVote.Question, SelectedOptions: options,
		}
	}
	provenance := agentevents.IncomingProvenance{
		Version: 2, TenantID: a.TenantID, PersonalityAgentID: event.PersonalityAgentID,
		Actor:  agentevents.ProvenanceActor{Kind: event.Actor.Kind, PrincipalID: event.Actor.ID, DisplayName: event.Actor.DisplayName},
		Source: source,
	}
	if err := provenance.Validate(); err != nil {
		return provenance, nil, err
	}
	command, err := json.Marshal(agentevents.ExternalEventCommand{Type: "external_event", Content: event.Content})
	return provenance, command, err
}

func (a *AgentAttentionGateway) Lookup(ctx context.Context, key string, event AgentAttentionEvent) (AgentAttentionReceipt, bool, error) {
	if a.Gateway == nil {
		return AgentAttentionReceipt{}, false, errors.New("attention gateway is unavailable")
	}
	provenance, command, err := a.input(event)
	if err != nil {
		return AgentAttentionReceipt{}, false, err
	}
	envelope, found, err := a.Gateway.LookupAdmission(ctx, provenance, key, command)
	return AgentAttentionReceipt{CommandID: envelope.CommandID, Seq: envelope.Seq}, found, err
}

func (a *AgentAttentionGateway) Admit(ctx context.Context, key string, event AgentAttentionEvent) (AgentAttentionReceipt, error) {
	if a.Gateway == nil {
		return AgentAttentionReceipt{}, errors.New("attention gateway is unavailable")
	}
	provenance, command, err := a.input(event)
	if err != nil {
		return AgentAttentionReceipt{}, err
	}
	envelope, err := a.Gateway.Append(ctx, provenance, key, command)
	return AgentAttentionReceipt{CommandID: envelope.CommandID, Seq: envelope.Seq}, err
}
