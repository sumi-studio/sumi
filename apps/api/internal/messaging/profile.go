package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// MaxTaglineChars matches the schema CHECK on participant_profiles.tagline.
// A tagline is one line about what this participant does, not a biography.
const MaxTaglineChars = 100

// Profile sentinels. 表示名は戸籍が正本なので、その検証失敗はここへ写して
// 返す（呼び出し側は koseki を知らなくてよい）。
var (
	ErrInvalidDisplayName = errors.New("display name is not usable")
	ErrInvalidTagline     = errors.New("tagline exceeds the allowed length")
	// ErrInvalidProfileImage covers「自分がアップロードした、まだどのメッセージ
	// にも属していない画像」でないものを指定した場合。存在を明かさないため、
	// 他人の添付も未知のidも同じ答えになる。
	ErrInvalidProfileImage = errors.New("profile image must be an image you uploaded and have not sent")
)

// SetProfile replaces the actor's own profile. There is no route for setting
// anyone else's: the participant is the authenticated caller, never a request
// field — the same rule as Status (自己申告). Humans and PersonalityAgents go
// through this one path, so neither side can hold a capability the other lacks.
//
// displayName is written back to the 戸籍 (humans / agents), which stays the
// canonical registry of names; tagline と画像だけがこの表に載る。
func (s *Store) SetProfile(ctx context.Context, actor ParticipantRef, displayName, tagline, avatarID, bannerID string) (MemberProfile, error) {
	change, err := s.setProfile(ctx, actor, displayName, tagline, avatarID, bannerID)
	return change.Profile, err
}

// profileChange carries the participant whose profile was replaced and any
// other presentation profiles whose projection changed as a consequence. A
// Human name qualifies each of that Human's durable PersonalityAgents, so the
// HTTP surface must publish those dependent profiles too.
type profileChange struct {
	Profile    MemberProfile
	Dependents []MemberProfile
}

// profilePatch is an internal mutation shape. Nil preserves a field; a
// non-nil empty image id clears that image. REST PUT builds a patch with every
// field present, while the local-control and auth surfaces name only the fields
// their callers actually changed.
type profilePatch struct {
	DisplayName        *string
	Tagline            *string
	AvatarAttachmentID *string
	BannerAttachmentID *string
}

func (s *Store) setProfile(ctx context.Context, actor ParticipantRef, displayName, tagline, avatarID, bannerID string) (profileChange, error) {
	return s.patchProfile(ctx, actor, profilePatch{
		DisplayName:        &displayName,
		Tagline:            &tagline,
		AvatarAttachmentID: &avatarID,
		BannerAttachmentID: &bannerID,
	})
}

func (s *Store) patchProfile(ctx context.Context, actor ParticipantRef, patch profilePatch) (profileChange, error) {
	if err := actor.Validate(); err != nil {
		return profileChange{}, err
	}
	if patch.Tagline != nil && utf8.RuneCountInString(*patch.Tagline) > MaxTaglineChars {
		return profileChange{}, ErrInvalidTagline
	}
	if patch.DisplayName != nil {
		normalizedName, err := koseki.NormalizeDisplayName(*patch.DisplayName)
		if err != nil {
			return profileChange{}, ErrInvalidDisplayName
		}
		patch.DisplayName = &normalizedName
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return profileChange{}, fmt.Errorf("begin set profile: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Resolving the canonical row first acquires the participant-level row lock.
	// Concurrent full and partial mutations for one participant therefore run in
	// a serial order, while unrelated participants remain independent.
	registry := koseki.New(s.pool)
	nameChanged := false
	switch actor.Kind {
	case KindHuman:
		if _, nameChanged, err = registry.ResolveHumanDisplayNameTx(ctx, tx, actor.ID, patch.DisplayName); err != nil {
			if errors.Is(err, koseki.ErrInvalidDisplayName) {
				return profileChange{}, ErrInvalidDisplayName
			}
			if errors.Is(err, koseki.ErrHumanNotFound) {
				return profileChange{}, ErrParticipantNotFound
			}
			return profileChange{}, fmt.Errorf("update display name: %w", err)
		}
	case KindPersonalityAgent:
		if _, nameChanged, err = registry.ResolveAgentDisplayNameTx(ctx, tx, actor.ID, patch.DisplayName); err != nil {
			if errors.Is(err, koseki.ErrInvalidDisplayName) {
				return profileChange{}, ErrInvalidDisplayName
			}
			if errors.Is(err, koseki.ErrAgentNotFound) {
				return profileChange{}, ErrParticipantNotFound
			}
			return profileChange{}, fmt.Errorf("update display name: %w", err)
		}
	default:
		return profileChange{}, fmt.Errorf("unknown participant kind %q", actor.Kind)
	}

	// The canonical row above is the participant lock. Read the values a partial
	// patch preserves only after acquiring it, so a concurrent patch cannot merge
	// an old field back over a newer commit.
	current, err := s.memberProfileFor(ctx, tx, actor)
	if err != nil {
		return profileChange{}, err
	}

	// Lock every newly chosen attachment row until the profile row commits.
	// Preserved images are already bound to this profile; only explicitly
	// supplied ids need to compete with message binding.
	imageIDs := make([]string, 0, 2)
	for _, imageID := range []*string{patch.AvatarAttachmentID, patch.BannerAttachmentID} {
		if imageID != nil && *imageID != "" {
			imageIDs = append(imageIDs, *imageID)
		}
	}
	sort.Strings(imageIDs)
	images := make(map[string]*string, len(imageIDs))
	for _, imageID := range imageIDs {
		if _, seen := images[imageID]; seen {
			continue
		}
		image, err := s.profileImage(ctx, tx, actor, imageID)
		if err != nil {
			return profileChange{}, err
		}
		images[imageID] = image
	}
	avatar := nullableProfileImage(current.AvatarAttachmentID)
	if patch.AvatarAttachmentID != nil {
		avatar = images[*patch.AvatarAttachmentID]
	}
	banner := nullableProfileImage(current.BannerAttachmentID)
	if patch.BannerAttachmentID != nil {
		banner = images[*patch.BannerAttachmentID]
	}
	tagline := current.Tagline
	if patch.Tagline != nil {
		tagline = *patch.Tagline
	}
	if patch.Tagline != nil || patch.AvatarAttachmentID != nil || patch.BannerAttachmentID != nil {
		if _, err := tx.Exec(ctx,
			`INSERT INTO participant_profiles
		   (member_kind, member_id, tagline, avatar_attachment_id, banner_attachment_id)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (member_kind, member_id) DO UPDATE
		   SET tagline = EXCLUDED.tagline,
		       avatar_attachment_id = EXCLUDED.avatar_attachment_id,
		       banner_attachment_id = EXCLUDED.banner_attachment_id,
		       updated_at = now()`,
			actor.Kind, actor.ID, tagline, avatar, banner); err != nil {
			return profileChange{}, fmt.Errorf("upsert participant profile: %w", err)
		}
	}

	change := profileChange{}
	change.Profile, err = s.memberProfileFor(ctx, tx, actor)
	if err != nil {
		return profileChange{}, err
	}
	if actor.Kind == KindHuman && nameChanged {
		change.Dependents, err = s.dependentAgentProfiles(ctx, tx, actor.ID)
		if err != nil {
			return profileChange{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return profileChange{}, fmt.Errorf("commit set profile: %w", err)
	}
	return change, nil
}

func nullableProfileImage(attachmentID string) *string {
	if attachmentID == "" {
		return nil
	}
	return &attachmentID
}

// profileImage validates one profile image reference. An empty id clears the
// image. The bytes must be an inline-renderable image the actor uploaded and
// has not attached to any message, so a profile picture can never smuggle an
// arbitrary document into an <img>, and can never point at someone else's file.
func (s *Store) profileImage(ctx context.Context, q querier, actor ParticipantRef, attachmentID string) (*string, error) {
	if attachmentID == "" {
		return nil, nil
	}
	if !validAttachmentID(attachmentID) {
		return nil, ErrInvalidProfileImage
	}
	var mimeType string
	err := q.QueryRow(ctx,
		`SELECT mime FROM message_attachments
		 WHERE attachment_id = $1 AND message_id IS NULL
		   AND uploader_kind = $2 AND uploader_id = $3
		 FOR UPDATE`,
		attachmentID, actor.Kind, actor.ID).Scan(&mimeType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidProfileImage
	}
	if err != nil {
		return nil, fmt.Errorf("load profile image: %w", err)
	}
	if !inlineImageMIMEs[mimeType] {
		return nil, ErrInvalidProfileImage
	}
	return &attachmentID, nil
}

// MemberProfileFor projects one participant's presentation profile: the 戸籍
// display name (with the Secretary qualifier the wire uses) plus whatever the
// participant said about themselves.
func (s *Store) MemberProfileFor(ctx context.Context, participant ParticipantRef) (MemberProfile, error) {
	return s.memberProfileFor(ctx, s.pool, participant)
}

func (s *Store) memberProfileFor(ctx context.Context, q querier, participant ParticipantRef) (MemberProfile, error) {
	if err := participant.Validate(); err != nil {
		return MemberProfile{}, err
	}
	profile := MemberProfile{Participant: participant}
	var avatar, banner *string
	err := q.QueryRow(ctx,
		`SELECT COALESCE(h.display_name, a.display_name, '') AS display_name,
		        CASE WHEN target.member_kind = 'personality_agent'
		             THEN COALESCE(owner.display_name, '') ELSE '' END,
		        COALESCE(pp.tagline, ''),
		        pp.avatar_attachment_id, pp.banner_attachment_id
		 FROM (SELECT $1::text AS member_kind, $2::uuidv7 AS member_id) target
		 LEFT JOIN humans h ON target.member_kind = 'human' AND h.human_id = target.member_id
		 LEFT JOIN agents a ON target.member_kind = 'personality_agent'
		                   AND a.personality_agent_id = target.member_id
		 LEFT JOIN humans owner ON owner.human_id = a.human_id
		 LEFT JOIN participant_profiles pp ON pp.member_kind = target.member_kind
		                                  AND pp.member_id = target.member_id`,
		string(participant.Kind), participant.ID).
		Scan(&profile.DisplayName, &profile.SecretaryForDisplayName,
			&profile.Tagline, &avatar, &banner)
	if err != nil {
		return MemberProfile{}, fmt.Errorf("load member profile: %w", err)
	}
	if profile.DisplayName == "" {
		return MemberProfile{}, ErrParticipantNotFound
	}
	if avatar != nil {
		profile.AvatarAttachmentID = *avatar
	}
	if banner != nil {
		profile.BannerAttachmentID = *banner
	}
	return profile, nil
}

// dependentAgentProfiles returns the presentations whose projected names use
// the given Human's canonical name as a qualifier. It runs in the same
// transaction as the rename so every emitted profile is one committed view.
func (s *Store) dependentAgentProfiles(ctx context.Context, q querier, humanID string) ([]MemberProfile, error) {
	rows, err := q.Query(ctx,
		`SELECT 'personality_agent', a.personality_agent_id, '' AS role,
		        a.display_name, owner.display_name, COALESCE(pp.tagline, ''),
		        pp.avatar_attachment_id, pp.banner_attachment_id
		 FROM agents a
		 JOIN humans owner ON owner.human_id = a.human_id
		 LEFT JOIN participant_profiles pp
		        ON pp.member_kind = 'personality_agent'
		       AND pp.member_id = a.personality_agent_id
		 WHERE a.human_id = $1
		 ORDER BY a.created_at, a.personality_agent_id`, humanID)
	if err != nil {
		return nil, fmt.Errorf("query profiles affected by Human rename: %w", err)
	}
	profiles, err := scanMemberProfiles(rows)
	if err != nil {
		return nil, fmt.Errorf("load profiles affected by Human rename: %w", err)
	}
	return profiles, nil
}

// attachmentIsProfileImage reports whether an attachment is somebody's avatar
// or header. Such an attachment is readable by anyone who can see that
// participant — a face on a member list is meant to be seen, unlike an unbound
// upload which is still private to its uploader.
func (s *Store) attachmentIsProfileImage(ctx context.Context, attachmentID string, viewer ParticipantRef) (bool, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT member_kind, member_id FROM participant_profiles
		 WHERE avatar_attachment_id = $1 OR banner_attachment_id = $1`, attachmentID)
	if err != nil {
		return false, fmt.Errorf("query profile image owners: %w", err)
	}
	defer rows.Close()
	var owners []ParticipantRef
	for rows.Next() {
		var owner ParticipantRef
		var kind string
		if err := rows.Scan(&kind, &owner.ID); err != nil {
			return false, fmt.Errorf("scan profile image owner: %w", err)
		}
		owner.Kind = ParticipantKind(kind)
		owners = append(owners, owner)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate profile image owners: %w", err)
	}
	for _, owner := range owners {
		visible, err := s.ParticipantVisible(ctx, viewer, owner)
		if err != nil {
			return false, err
		}
		if visible {
			return true, nil
		}
	}
	return false, nil
}
