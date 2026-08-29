package koseki

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

const MaxHumanTaglineRunes = 100

var (
	ErrInvalidTagline         = errors.New("invalid Human tagline")
	ErrEmptyHumanProfilePatch = errors.New("empty Human profile patch")
)

// HumanProfile is the Human-owned projection shown in global settings.
// The canonical display name remains on humans; the Participant-global
// tagline is joined from participant_profiles rather than copied per Workspace.
type HumanProfile struct {
	HumanID     string
	DisplayName string
	Tagline     string
}

type humanProfileQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// HumanProfile returns the current durable profile for one canonical Human.
func (s *Store) HumanProfile(ctx context.Context, humanID string) (HumanProfile, error) {
	return humanProfileFor(ctx, s.pool, humanID)
}

// UpdateHumanProfile applies a partial self-service profile change atomically.
// A nil field is preserved. The Human row is locked first so concurrent name
// and tagline writes for the same person cannot combine stale halves.
func (s *Store) UpdateHumanProfile(
	ctx context.Context,
	humanID string,
	displayName *string,
	tagline *string,
) (HumanProfile, error) {
	if displayName == nil && tagline == nil {
		return HumanProfile{}, ErrEmptyHumanProfilePatch
	}
	var normalizedName *string
	if displayName != nil {
		value, err := normalizeHumanDisplayName(*displayName)
		if err != nil {
			return HumanProfile{}, err
		}
		normalizedName = &value
	}
	var normalizedTagline *string
	if tagline != nil {
		value, err := normalizeHumanTagline(*tagline)
		if err != nil {
			return HumanProfile{}, err
		}
		normalizedTagline = &value
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return HumanProfile{}, fmt.Errorf("begin Human profile update: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var currentName string
	if err := tx.QueryRow(ctx,
		"SELECT display_name FROM humans WHERE human_id=$1 FOR UPDATE", humanID,
	).Scan(&currentName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return HumanProfile{}, ErrHumanNotFound
		}
		return HumanProfile{}, fmt.Errorf("lock Human profile: %w", err)
	}
	if normalizedName != nil {
		if err := tx.QueryRow(ctx, `UPDATE humans
			SET display_name=$2, display_name_customized=true, display_name_initialized=true
			WHERE human_id=$1 RETURNING display_name`, humanID, *normalizedName,
		).Scan(&currentName); err != nil {
			return HumanProfile{}, fmt.Errorf("update Human display name: %w", err)
		}
	}
	if normalizedTagline != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO participant_profiles
			(member_kind, member_id, tagline)
			VALUES ('human', $1, $2)
			ON CONFLICT (member_kind, member_id)
			DO UPDATE SET tagline=EXCLUDED.tagline`,
			humanID, *normalizedTagline); err != nil {
			return HumanProfile{}, fmt.Errorf("update Human tagline: %w", err)
		}
	}
	profile, err := humanProfileFor(ctx, tx, humanID)
	if err != nil {
		return HumanProfile{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return HumanProfile{}, fmt.Errorf("commit Human profile update: %w", err)
	}
	return profile, nil
}

func humanProfileFor(ctx context.Context, q humanProfileQuerier, humanID string) (HumanProfile, error) {
	profile := HumanProfile{HumanID: humanID}
	err := q.QueryRow(ctx, `SELECT h.display_name, COALESCE(pp.tagline, '')
		FROM humans h
		LEFT JOIN participant_profiles pp
		  ON pp.member_kind='human' AND pp.member_id=h.human_id
		WHERE h.human_id=$1`, humanID,
	).Scan(&profile.DisplayName, &profile.Tagline)
	if errors.Is(err, pgx.ErrNoRows) {
		return HumanProfile{}, ErrHumanNotFound
	}
	if err != nil {
		return HumanProfile{}, fmt.Errorf("read Human profile: %w", err)
	}
	return profile, nil
}

func normalizeHumanTagline(raw string) (string, error) {
	visible := false
	for _, r := range raw {
		if unicode.IsControl(r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return "", ErrInvalidTagline
		}
		if unicode.In(r, unicode.Cf) && r != '\u200c' && r != '\u200d' {
			return "", ErrInvalidTagline
		}
		if !unicode.IsSpace(r) && !unicode.In(r, unicode.Cf) && !unicode.Is(unicode.M, r) {
			visible = true
		}
	}
	value := strings.TrimSpace(raw)
	if value != "" && !visible {
		return "", ErrInvalidTagline
	}
	if utf8.RuneCountInString(value) > MaxHumanTaglineRunes {
		return "", ErrInvalidTagline
	}
	return value, nil
}
