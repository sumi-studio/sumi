package processoperations

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

type Receipt struct {
	CommandID string
	Seq       uint64
}
type Delivery interface {
	Prepare(context.Context, string) (func(), error)
	Lookup(context.Context, string, runtimeprovision.ProcessOperation) (Receipt, bool, error)
	Admit(context.Context, string, runtimeprovision.ProcessOperation) (Receipt, error)
}

type GatewayDelivery struct {
	Gateway  *agentevents.DurableGateway
	Spawner  agentevents.DirectChatSpawner
	TenantID string
}

func (d *GatewayDelivery) Prepare(ctx context.Context, pa string) (func(), error) {
	return d.Gateway.PrepareAttention(ctx, d.Spawner, pa)
}

func completionInput(tenant string, op runtimeprovision.ProcessOperation) (agentevents.IncomingProvenance, json.RawMessage, error) {
	var p agentevents.IncomingProvenance
	if op.FinishedAt == nil || op.FinishedAt.IsZero() || op.StdoutBytes < 0 || op.StderrBytes < 0 {
		return p, nil, errors.New("operation has no complete terminal receipt")
	}
	var exit *int64
	if op.ExitCode != nil {
		n := int64(*op.ExitCode)
		exit = &n
	}
	p = agentevents.IncomingProvenance{
		Version: 2, TenantID: tenant, PersonalityAgentID: op.PersonalityAgentID,
		Actor: agentevents.ProvenanceActor{Kind: "personality_agent", PrincipalID: op.PersonalityAgentID},
		Source: agentevents.ProvenanceSource{
			Surface: "workspace_operation", Kind: "process_completed", EventID: op.EventID,
			OperationID: op.OperationID, OriginatingToolCallID: op.OriginatingToolCallID,
			OccurredAt: op.FinishedAt.UTC().Format(time.RFC3339Nano),
			Result: &agentevents.ProvenanceOperationResult{State: string(op.State), ExitCode: exit,
				StdoutBytes: uint64(op.StdoutBytes), StderrBytes: uint64(op.StderrBytes),
				OutputTruncated: op.StdoutTruncated || op.StderrTruncated},
		},
	}
	if err := p.Validate(); err != nil {
		return p, nil, err
	}
	return p, json.RawMessage(`{"type":"external_event","content":""}`), nil
}

func (d *GatewayDelivery) Lookup(ctx context.Context, key string, op runtimeprovision.ProcessOperation) (Receipt, bool, error) {
	p, command, err := completionInput(d.TenantID, op)
	if err != nil {
		return Receipt{}, false, err
	}
	v, found, err := d.Gateway.LookupAdmission(ctx, p, key, command)
	return Receipt{v.CommandID, v.Seq}, found, err
}

func (d *GatewayDelivery) Admit(ctx context.Context, key string, op runtimeprovision.ProcessOperation) (Receipt, error) {
	p, command, err := completionInput(d.TenantID, op)
	if err != nil {
		return Receipt{}, err
	}
	v, err := d.Gateway.Append(ctx, p, key, command)
	return Receipt{v.CommandID, v.Seq}, err
}

// DeliverPending reconciles the durable gateway receipt before waking a PA.
// A lost acknowledgment retries the same event; it never starts another job.
func (s *Server) DeliverPending(ctx context.Context) error {
	if s.Delivery == nil {
		return nil
	}
	pending, err := s.Backend.PendingProcessCompletions(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, op := range pending {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.deliverOne(ctx, op); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (s *Server) deliverOne(ctx context.Context, op runtimeprovision.ProcessOperation) error {
	key := "process-completed/" + op.EventID
	receipt, found, err := s.Delivery.Lookup(ctx, key, op)
	if err != nil {
		return err
	}
	if !found {
		release, err := s.Delivery.Prepare(ctx, op.PersonalityAgentID)
		if err != nil {
			return err
		}
		defer release()
		receipt, err = s.Delivery.Admit(ctx, key, op)
		if err != nil {
			return err
		}
	}
	if receipt.CommandID == "" || receipt.Seq == 0 {
		return errors.New("operation completion has no admission receipt")
	}
	return s.Backend.AcknowledgeProcessCompletion(ctx, runtimeprovision.ProcessCompletionReceipt{
		PersonalityAgentID: op.PersonalityAgentID, OperationID: op.OperationID, EventID: op.EventID,
		CommandID: receipt.CommandID, CommandSeq: receipt.Seq,
	})
}
