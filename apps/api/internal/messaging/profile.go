package messaging

import (
	"context"
	"errors"
	"fmt"
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
	if err := actor.Validate(); err != nil {
		return MemberProfile{}, err
	}
	if err := s.participantExists(ctx, actor); err != nil {
		return MemberProfile{}, err
	}
	if utf8.RuneCountInString(tagline) > MaxTaglineChars {
		return MemberProfile{}, ErrInvalidTagline
	}
	registry := koseki.New(s.pool)
	if _, err := koseki.NormalizeDisplayName(displayName); err != nil {
		return MemberProfile{}, ErrInvalidDisplayName
	}
	avatar, err := s.profileImage(ctx, actor, avatarID)
	if err != nil {
		return MemberProfile{}, err
	}
	banner, err := s.profileImage(ctx, actor, bannerID)
	if err != nil {
		return MemberProfile{}, err
	}
	switch actor.Kind {
	case KindHuman:
		if _, err := registry.UpdateHumanDisplayName(ctx, actor.ID, displayName); err != nil {
			if errors.Is(err, koseki.ErrInvalidDisplayName) {
				return MemberProfile{}, ErrInvalidDisplayName
			}
			return MemberProfile{}, fmt.Errorf("update display name: %w", err)
		}
	case KindPersonalityAgent:
		if _, err := registry.UpdateAgentDisplayName(ctx, actor.ID, displayName); err != nil {
			if errors.Is(err, koseki.ErrInvalidDisplayName) {
				return MemberProfile{}, ErrInvalidDisplayName
			}
			return MemberProfile{}, fmt.Errorf("update display name: %w", err)
		}
	default:
		return MemberProfile{}, fmt.Errorf("unknown participant kind %q", actor.Kind)
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO participant_profiles
		   (member_kind, member_id, tagline, avatar_attachment_id, banner_attachment_id)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (member_kind, member_id) DO UPDATE
		   SET tagline = EXCLUDED.tagline,
		       avatar_attachment_id = EXCLUDED.avatar_attachment_id,
		       banner_attachment_id = EXCLUDED.banner_attachment_id,
		       updated_at = now()`,
		actor.Kind, actor.ID, tagline, avatar, banner); err != nil {
		return MemberProfile{}, fmt.Errorf("upsert participant profile: %w", err)
	}
	return s.MemberProfileFor(ctx, actor)
}

// profileImage validates one profile image reference. An empty id clears the
// image. The bytes must be an inline-renderable image the actor uploaded and
// has not attached to any message, so a profile picture can never smuggle an
// arbitrary document into an <img>, and can never point at someone else's file.
func (s *Store) profileImage(ctx context.Context, actor ParticipantRef, attachmentID string) (*string, error) {
	if attachmentID == "" {
		return nil, nil
	}
	if !validAttachmentID(attachmentID) {
		return nil, ErrInvalidProfileImage
	}
	var mimeType string
	err := s.pool.QueryRow(ctx,
		`SELECT mime FROM message_attachments
		 WHERE attachment_id = $1 AND message_id IS NULL
		   AND uploader_kind = $2 AND uploader_id = $3`,
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
	if err := participant.Validate(); err != nil {
		return MemberProfile{}, err
	}
	profile := MemberProfile{Participant: participant}
	var avatar, banner *string
	err := s.pool.QueryRow(ctx,
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
