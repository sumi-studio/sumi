package messaging

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// MaxTaglineChars matches the schema CHECK on participant_profiles.tagline. A
// tagline is one line about what this participant does, not a biography.
const MaxTaglineChars = 100

// Profile sentinels. 表示名は戸籍が正本なので、その検証失敗はここへ写して返す
// （呼び出し側は koseki を知らなくてよい）。
var (
	ErrInvalidDisplayName = errors.New("display name is not usable")
	ErrInvalidTagline     = errors.New("tagline exceeds the allowed length")
)

// Profile returns the actor's own canonical profile: the 戸籍 display name plus
// whatever they have said about themselves.
func (s *ScopedStore) Profile(ctx context.Context) (MemberProfile, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MemberProfile{}, fmt.Errorf("begin profile read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return MemberProfile{}, err
	}
	profile, err := memberProfileFor(ctx, tx, s.Scope.Actor)
	if err != nil {
		return MemberProfile{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MemberProfile{}, fmt.Errorf("commit profile read: %w", err)
	}
	return profile, nil
}

// SetProfile replaces the actor's own profile. There is no route for setting
// anyone else's: the participant is the authenticated scope actor, never a
// request field — the same rule as Status (自己申告). Humans and
// PersonalityAgents go through this one path, so neither side can hold a
// capability the other lacks.
//
// displayName is written back to the 戸籍, which stays the canonical registry
// of names; only the tagline lands in participant_profiles. A nil field is
// preserved, so a caller who changes one field cannot silently clear the other.
func (s *ScopedStore) SetProfile(ctx context.Context, displayName, tagline *string) (MemberProfile, error) {
	if tagline != nil && utf8.RuneCountInString(*tagline) > MaxTaglineChars {
		return MemberProfile{}, ErrInvalidTagline
	}
	if displayName != nil {
		normalized, err := koseki.NormalizeDisplayName(*displayName)
		if err != nil {
			return MemberProfile{}, ErrInvalidDisplayName
		}
		displayName = &normalized
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MemberProfile{}, fmt.Errorf("begin set profile: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return MemberProfile{}, err
	}

	// Resolving the canonical row first takes the participant-level row lock, so
	// two concurrent profile writes for one participant run in a serial order
	// while unrelated participants stay independent.
	actor := s.Scope.Actor
	switch actor.Kind {
	case KindHuman:
		if _, err := koseki.ResolveHumanDisplayNameTx(ctx, tx, actor.ID, displayName); err != nil {
			return MemberProfile{}, mapDisplayNameError(err)
		}
	case KindPersonalityAgent:
		if _, err := koseki.ResolveAgentDisplayNameTx(ctx, tx, actor.ID, displayName); err != nil {
			return MemberProfile{}, mapDisplayNameError(err)
		}
	default:
		return MemberProfile{}, fmt.Errorf("unknown participant kind %q", actor.Kind)
	}

	if tagline != nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO participant_profiles (member_kind, member_id, tagline)
			VALUES ($1, $2, $3)
			ON CONFLICT (member_kind, member_id)
			DO UPDATE SET tagline = EXCLUDED.tagline, updated_at = now()`,
			actor.Kind, actor.ID, *tagline); err != nil {
			return MemberProfile{}, fmt.Errorf("upsert participant profile: %w", err)
		}
	}

	profile, err := memberProfileFor(ctx, tx, actor)
	if err != nil {
		return MemberProfile{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MemberProfile{}, fmt.Errorf("commit set profile: %w", err)
	}
	return profile, nil
}

// mapDisplayNameError translates a 戸籍 failure into the messaging vocabulary.
func mapDisplayNameError(err error) error {
	switch {
	case errors.Is(err, koseki.ErrInvalidDisplayName):
		return ErrInvalidDisplayName
	case errors.Is(err, koseki.ErrHumanNotFound), errors.Is(err, koseki.ErrAgentNotFound):
		return ErrParticipantNotFound
	default:
		return fmt.Errorf("resolve display name: %w", err)
	}
}

// memberProfileFor projects one participant's presentation profile from the
// 戸籍 name and their own tagline.
func memberProfileFor(ctx context.Context, q querier, subject ParticipantRef) (MemberProfile, error) {
	if err := subject.Validate(); err != nil {
		return MemberProfile{}, err
	}
	profile := MemberProfile{Participant: subject}
	err := q.QueryRow(ctx, `
		SELECT COALESCE(h.display_name, a.display_name, ''), COALESCE(pp.tagline, '')
		FROM (SELECT $1::text AS member_kind, $2::uuidv7 AS member_id) target
		LEFT JOIN humans h ON target.member_kind = 'human' AND h.human_id = target.member_id
		LEFT JOIN agents a ON target.member_kind = 'personality_agent'
		                  AND a.personality_agent_id = target.member_id
		LEFT JOIN participant_profiles pp ON pp.member_kind = target.member_kind
		                                AND pp.member_id = target.member_id`,
		string(subject.Kind), subject.ID).Scan(&profile.DisplayName, &profile.Tagline)
	if err != nil {
		return MemberProfile{}, fmt.Errorf("load member profile: %w", err)
	}
	if profile.DisplayName == "" {
		return MemberProfile{}, ErrParticipantNotFound
	}
	return profile, nil
}
