package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// MaxTaglineChars matches the schema CHECK on participant_profiles.tagline. A
// tagline is one line about what this participant does, not a biography.
const MaxTaglineChars = 100

// Profile sentinels. 表示名は戸籍が正本なので、その検証失敗はここへ写して返す
// （呼び出し側は koseki を知らなくてよい）。
var (
	ErrInvalidDisplayName = errors.New("display name is not usable")
	ErrInvalidTagline     = errors.New("tagline is not a single usable line")
)

// profilePublisher receives a committed immutable profile value and every
// currently enabled Messaging address where its participant is visible.
// Publication is intentionally best-effort. Revision makes a later snapshot
// or event win when delivery order differs from commit order.
type profilePublisher func(context.Context, []Scope, MemberProfile)

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

// profileAuthorization holds the caller-specific authority check inside the
// same transaction as the profile write. The browser's account settings route
// has already bound its Human through AuthorizeSession, while a scoped
// Messaging request must also hold its exact Workspace installation fence.
type profileAuthorization func(context.Context, pgx.Tx) error

// setProfile replaces one participant's profile through the only durable
// profile write boundary. displayName is written back to the 戸籍, which stays
// the canonical registry of names; only the tagline lands in
// participant_profiles. A nil field is preserved, so a caller who changes one
// field cannot silently clear the other.
//
// The optional authorization is deliberately part of this transaction. It
// lets every transport share the same 戸籍 resolution, revision allocation,
// post-commit ordering, and all-Workspace publication while retaining its own
// authenticated actor boundary.
func (s *Store) setProfile(ctx context.Context, actor ParticipantRef, displayName, tagline *string, authorize profileAuthorization, publish profilePublisher) (MemberProfile, error) {
	if tagline != nil {
		normalized, err := normalizeTagline(*tagline)
		if err != nil {
			return MemberProfile{}, err
		}
		tagline = &normalized
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
	if authorize != nil {
		if err := authorize(ctx, tx); err != nil {
			return MemberProfile{}, err
		}
	}

	// Resolving the canonical row first takes the participant-level row lock, so
	// two concurrent profile writes for one participant run in a serial order
	// while unrelated participants stay independent.
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

	// Always touch the profile row, including a display-name-only update. The
	// database trigger allocates the next revision; application code never does.
	// A missing tagline stays missing on conflict while a newly-created row gets
	// the empty default.
	var requestedTagline any
	if tagline != nil {
		requestedTagline = *tagline
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO participant_profiles (member_kind, member_id, tagline)
		VALUES ($1, $2, COALESCE($3::text, ''))
		ON CONFLICT (member_kind, member_id)
		DO UPDATE SET tagline = COALESCE($3::text, participant_profiles.tagline),
		              updated_at = now()`,
		actor.Kind, actor.ID, requestedTagline); err != nil {
		return MemberProfile{}, fmt.Errorf("upsert participant profile: %w", err)
	}

	profile, err := memberProfileFor(ctx, tx, actor)
	if err != nil {
		return MemberProfile{}, err
	}
	scopes, err := s.profileAudienceScopesInTx(ctx, tx, actor)
	if err != nil {
		return MemberProfile{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MemberProfile{}, fmt.Errorf("commit set profile: %w", err)
	}
	// A receiver can bootstrap immediately after this frame, so publication
	// must follow a successful commit. Concurrent committed publications can
	// pass each other; revision is the ordering rule at every receiver.
	if publish != nil {
		publish(ctx, scopes, profile)
	}
	return profile, nil
}

// SetProfile replaces the authenticated scope actor's profile. There is no
// route for setting anyone else's: the participant is the authenticated scope
// actor, never a request field — the same rule as Status (自己申告). Humans and
// PersonalityAgents go through Store.setProfile, so neither side can hold a
// capability the other lacks.
func (s *ScopedStore) SetProfile(ctx context.Context, displayName, tagline *string, publish profilePublisher) (MemberProfile, error) {
	return s.Store.setProfile(ctx, s.Scope.Actor, displayName, tagline, func(ctx context.Context, tx pgx.Tx) error {
		_, err := s.authorizeMutationInTx(ctx, tx)
		return err
	}, publish)
}

// normalizeTagline applies the shared display-text rule at the one profile
// write boundary. A tagline may be empty and has its own length limit, but its
// permitted characters and single-line rule are the same as a display name.
func normalizeTagline(raw string) (string, error) {
	if err := koseki.ValidateDisplayText(raw); err != nil {
		return "", ErrInvalidTagline
	}
	value := strings.TrimSpace(raw)
	if utf8.RuneCountInString(value) > MaxTaglineChars {
		return "", ErrInvalidTagline
	}
	return value, nil
}

// profileAudienceScopesInTx finds every Workspace-local Messaging address at
// which this participant is currently visible. The profile itself is global;
// the Hub remains responsible for taking each address's audience snapshot and
// excluding connections whose exact installation is no longer current.
func (s *Store) profileAudienceScopesInTx(ctx context.Context, tx querier, actor ParticipantRef) ([]Scope, error) {
	rows, err := tx.Query(ctx, `
		SELECT wm.workspace_id, ai.installation_id, ai.authority_epoch
		FROM workspace_members wm
		JOIN app_installations ai
		  ON ai.owner_kind = 'workspace' AND ai.owner_id = wm.workspace_id
		 AND ai.app_id = $3 AND ai.enabled
		WHERE wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		ORDER BY wm.workspace_id, ai.installation_id`, actor.Kind, actor.ID, MessagingAppID)
	if err != nil {
		return nil, fmt.Errorf("list profile delivery scopes: %w", err)
	}
	defer rows.Close()
	scopes := []Scope{}
	for rows.Next() {
		var scope Scope
		if err := rows.Scan(&scope.WorkspaceID, &scope.InstallationID, &scope.AuthorityEpoch); err != nil {
			return nil, fmt.Errorf("scan profile delivery scope: %w", err)
		}
		scope.Actor = actor
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate profile delivery scopes: %w", err)
	}
	return scopes, nil
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
		SELECT COALESCE(h.display_name, a.display_name, ''), COALESCE(pp.tagline, ''),
		       COALESCE(pp.revision, 0)
		FROM (SELECT $1::text AS member_kind, $2::uuidv7 AS member_id) target
		LEFT JOIN humans h ON target.member_kind = 'human' AND h.human_id = target.member_id
		LEFT JOIN agents a ON target.member_kind = 'personality_agent'
		                  AND a.personality_agent_id = target.member_id
		LEFT JOIN participant_profiles pp ON pp.member_kind = target.member_kind
		                                AND pp.member_id = target.member_id`,
		string(subject.Kind), subject.ID).Scan(&profile.DisplayName, &profile.Tagline, &profile.Revision)
	if err != nil {
		return MemberProfile{}, fmt.Errorf("load member profile: %w", err)
	}
	if profile.DisplayName == "" {
		return MemberProfile{}, ErrParticipantNotFound
	}
	return profile, nil
}
