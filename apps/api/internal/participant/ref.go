// Package participant defines the application-wide Human | PersonalityAgent
// reference used by shared domain services. A bare UUID is never sufficient:
// HumanId and PersonalityAgentId deliberately share the same grammar.
package participant

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

type Kind string

const (
	KindHuman            Kind = "human"
	KindPersonalityAgent Kind = "personality_agent"
)

type Ref struct {
	Kind Kind
	ID   string
}

func Human(id string) Ref { return Ref{Kind: KindHuman, ID: id} }

func PersonalityAgent(id string) Ref {
	return Ref{Kind: KindPersonalityAgent, ID: id}
}

func (r Ref) Validate() error {
	switch r.Kind {
	case KindHuman:
		id, err := uuid.Parse(r.ID)
		if err != nil || id.String() != r.ID || id.Version() != 7 || id.Variant() != uuid.RFC4122 {
			return errors.New("human_id must be a canonical lowercase UUIDv7")
		}
		return nil
	case KindPersonalityAgent:
		return agentevents.ValidatePersonalityAgentID(r.ID)
	default:
		return fmt.Errorf("unknown participant kind %q", r.Kind)
	}
}

func (r Ref) Key() string { return string(r.Kind) + ":" + r.ID }

// QueryRower is the common subset implemented by pgx pools and transactions.
// It keeps polymorphic identity validation in one package without exposing a
// global participant-directory operation.
type QueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Exists validates and checks one already-addressed canonical participant.
// It is intentionally not a search/list API.
func Exists(ctx context.Context, q QueryRower, ref Ref) (bool, error) {
	if err := ref.Validate(); err != nil {
		return false, err
	}
	var exists bool
	var err error
	switch ref.Kind {
	case KindHuman:
		err = q.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM humans WHERE human_id = $1)", ref.ID,
		).Scan(&exists)
	case KindPersonalityAgent:
		err = q.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM agents WHERE personality_agent_id = $1)", ref.ID,
		).Scan(&exists)
	default:
		return false, fmt.Errorf("unknown participant kind %q", ref.Kind)
	}
	if err != nil {
		return false, fmt.Errorf("check participant existence: %w", err)
	}
	return exists, nil
}

// LockOwnIdentity pins the participant row for an owner-scoped mutation. The
// caller must first establish that the authenticated actor equals the owner;
// this function does not provide directory-style existence disclosure.
func LockOwnIdentity(ctx context.Context, q QueryRower, ref Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	var id string
	var err error
	switch ref.Kind {
	case KindHuman:
		err = q.QueryRow(ctx,
			"SELECT human_id FROM humans WHERE human_id = $1 FOR UPDATE", ref.ID,
		).Scan(&id)
	case KindPersonalityAgent:
		err = q.QueryRow(ctx,
			"SELECT personality_agent_id FROM agents WHERE personality_agent_id = $1 FOR UPDATE", ref.ID,
		).Scan(&id)
	default:
		return fmt.Errorf("unknown participant kind %q", ref.Kind)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("participant not found")
	}
	if err != nil {
		return fmt.Errorf("lock participant identity: %w", err)
	}
	return nil
}
