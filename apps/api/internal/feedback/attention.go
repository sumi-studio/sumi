package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

// AttentionEvent is an immutable copy of what was submitted. Reconciliation
// must use these bytes even if display names or thread state change later.
type AttentionEvent struct {
	EventID, ThreadID, PersonalityAgentID, Kind, Title, Body string
	Revision                                                 int64
	Actor                                                    Author
	OccurredAt                                               time.Time
}
type AttentionDelivery interface {
	Prepare(context.Context, string) (func(), error)
	Lookup(context.Context, string, AttentionEvent) (bool, error)
	Admit(context.Context, string, AttentionEvent) error
}
type AttentionGateway struct {
	Gateway  *agentevents.DurableGateway
	Spawner  agentevents.DirectChatSpawner
	TenantID string
}

func (a *AttentionGateway) Prepare(ctx context.Context, id string) (func(), error) {
	if a.Gateway == nil {
		return nil, errors.New("feedback attention gateway unavailable")
	}
	return a.Gateway.PrepareAttention(ctx, a.Spawner, id)
}
func (a *AttentionGateway) input(e AttentionEvent) (agentevents.IncomingProvenance, json.RawMessage, error) {
	actorID := e.Actor.Participant.HumanID
	if e.Actor.Participant.Kind == participant.KindPersonalityAgent {
		actorID = e.Actor.Participant.PersonalityAgentID
	}
	p := agentevents.IncomingProvenance{Version: 2, TenantID: a.TenantID, PersonalityAgentID: e.PersonalityAgentID,
		Actor:  agentevents.ProvenanceActor{Kind: string(e.Actor.Participant.Kind), PrincipalID: actorID, DisplayName: e.Actor.DisplayName},
		Source: agentevents.ProvenanceSource{Surface: "feedback", Kind: e.Kind, EventID: e.EventID, ThreadID: e.ThreadID, Title: e.Title, Revision: uint64(e.Revision), OccurredAt: e.OccurredAt.UTC().Format(time.RFC3339Nano)}}
	if err := p.Validate(); err != nil {
		return p, nil, err
	}
	command, err := json.Marshal(agentevents.ExternalEventCommand{Type: "external_event", Content: e.Body})
	return p, command, err
}
func (a *AttentionGateway) Lookup(ctx context.Context, key string, e AttentionEvent) (bool, error) {
	if a.Gateway == nil {
		return false, errors.New("feedback attention gateway unavailable")
	}
	p, c, err := a.input(e)
	if err != nil {
		return false, err
	}
	_, found, err := a.Gateway.LookupAdmission(ctx, p, key, c)
	return found, err
}
func (a *AttentionGateway) Admit(ctx context.Context, key string, e AttentionEvent) error {
	if a.Gateway == nil {
		return errors.New("feedback attention gateway unavailable")
	}
	p, c, err := a.input(e)
	if err != nil {
		return err
	}
	_, err = a.Gateway.Append(ctx, p, key, c)
	return err
}

func (s *Store) enqueue(ctx context.Context, tx pgx.Tx, actor participant.Ref, threadID, eventID string, revision int64) error {
	var e AttentionEvent
	e.EventID, e.ThreadID, e.Revision = eventID, threadID, revision
	err := tx.QueryRow(ctx, `SELECT title,body,author,created_at FROM feedback_threads WHERE thread_id=$1`, threadID).Scan(&e.Title, &e.Body, &e.Actor, &e.OccurredAt)
	if err != nil {
		return err
	}
	e.Kind = "feedback_created"
	creator := e.Actor.Participant
	if revision > 1 {
		var kind, status string
		err = tx.QueryRow(ctx, `SELECT kind,author,COALESCE(body,''),COALESCE(status,''),created_at FROM feedback_events WHERE event_id=$1 AND thread_id=$2`, eventID, threadID).Scan(&kind, &e.Actor, &e.Body, &status, &e.OccurredAt)
		if err != nil {
			return err
		}
		e.Kind = "feedback_reply"
		if kind == "status" {
			e.Kind = "feedback_status"
			e.Body = "相談を再開しました。"
			if status == "resolved" {
				e.Body = "相談を解決済みにしました。"
			}
		}
	}
	targets := map[string]bool{}
	for _, r := range s.recipients {
		if r.Kind == participant.KindPersonalityAgent && r != actor {
			targets[r.ID] = true
		}
	}
	if creator.Kind == participant.KindPersonalityAgent && participant.PersonalityAgent(creator.PersonalityAgentID) != actor {
		targets[creator.PersonalityAgentID] = true
	}
	for target := range targets {
		e.PersonalityAgentID = target
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO feedback_attention_outbox(event_id,recipient_paid,thread_id,payload) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, eventID, target, threadID, payload); err != nil {
			return err
		}
	}
	return nil
}

// DeliverAttention retries durable admissions, not the author's app mutation.
// One unavailable PA cannot prevent another recipient's delivery.
func (s *Store) DeliverAttention(ctx context.Context, d AttentionDelivery, limit int) error {
	// Runtime preparation can legitimately take 30 seconds during a cold start.
	return s.deliverAttention(ctx, d, limit, 35*time.Second)
}

func (s *Store) deliverAttention(ctx context.Context, d AttentionDelivery, limit int, attemptTimeout time.Duration) error {
	if d == nil || limit < 1 || limit > 100 {
		return errors.New("invalid feedback attention delivery")
	}
	rows, err := s.pool.Query(ctx, `SELECT payload FROM feedback_attention_outbox WHERE finished_at IS NULL AND next_attempt_at<=now() ORDER BY next_attempt_at,event_id LIMIT $1`, limit)
	if err != nil {
		return err
	}
	var pending []AttentionEvent
	for rows.Next() {
		var e AttentionEvent
		if err = rows.Scan(&e); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failures []error
	for _, e := range pending {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		ready, err := s.attemptAttention(attemptCtx, d, e, false)
		if err == nil && ready {
			var release func()
			release, err = d.Prepare(attemptCtx, e.PersonalityAgentID)
			if err == nil && release == nil {
				err = errors.New("feedback runtime admission hold missing")
			}
			if err == nil {
				_, err = s.attemptAttention(attemptCtx, d, e, true)
				release()
			}
		}
		cancel()
		if err != nil {
			failures = append(failures, err)
			_, retryErr := s.pool.Exec(ctx, `UPDATE feedback_attention_outbox SET next_attempt_at=now()+interval '15 seconds' WHERE event_id=$1 AND recipient_paid=$2 AND finished_at IS NULL`, e.EventID, e.PersonalityAgentID)
			if retryErr != nil {
				failures = append(failures, retryErr)
			}
		}
	}
	return errors.Join(failures...)
}
func (s *Store) attemptAttention(ctx context.Context, d AttentionDelivery, e AttentionEvent, admit bool) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	actor := participant.PersonalityAgent(e.PersonalityAgentID)
	var enabled bool
	err = tx.QueryRow(ctx, `SELECT enabled FROM app_installations WHERE owner_kind='personality_agent' AND owner_id=$1 AND app_id='feedback' FOR SHARE`, actor.ID).Scan(&enabled)
	eligible := err == nil && enabled
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if eligible {
		_, err = s.thread(ctx, tx, actor, e.ThreadID, true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return false, err
		}
		eligible = err == nil
	}
	var finished bool
	err = tx.QueryRow(ctx, `SELECT finished_at IS NOT NULL FROM feedback_attention_outbox WHERE event_id=$1 AND recipient_paid=$2 FOR UPDATE`, e.EventID, e.PersonalityAgentID).Scan(&finished)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if finished {
		return false, nil
	}
	key := fmt.Sprintf("feedback:%s:%s", e.PersonalityAgentID, e.EventID)
	found, err := d.Lookup(ctx, key, e)
	if err != nil {
		return false, err
	}
	if !found && eligible && !admit {
		return true, tx.Commit(ctx)
	}
	if !found && eligible {
		if err = d.Admit(ctx, key, e); err != nil {
			return false, err
		}
		found = true
	}
	outcome := "suppressed"
	if found {
		outcome = "admitted"
	}
	_, err = tx.Exec(ctx, `UPDATE feedback_attention_outbox SET finished_at=now(),outcome=$3 WHERE event_id=$1 AND recipient_paid=$2`, e.EventID, e.PersonalityAgentID, outcome)
	if err != nil {
		return false, err
	}
	return false, tx.Commit(ctx)
}
