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

const MaxHumanDisplayNameRunes = 80

var ErrInvalidDisplayName = errors.New("invalid Human display name")

// normalizeHumanDisplayName makes profile text safe to persist while
// preserving ordinary Unicode names and the joiners used by emoji/scripts.
func normalizeHumanDisplayName(raw string) (string, error) {
	var cleaned strings.Builder
	cleaned.Grow(len(raw))
	visible := false
	for _, r := range raw {
		if unicode.IsControl(r) {
			if unicode.IsSpace(r) {
				cleaned.WriteByte(' ')
				continue
			}
			return "", ErrInvalidDisplayName
		}
		if unicode.In(r, unicode.Cf) && r != '\u200c' && r != '\u200d' {
			return "", ErrInvalidDisplayName
		}
		if !unicode.IsSpace(r) && !unicode.In(r, unicode.Cf) && !unicode.Is(unicode.M, r) {
			visible = true
		}
		cleaned.WriteRune(r)
	}
	name := strings.Join(strings.Fields(cleaned.String()), " ")
	if name == "" || !visible {
		return "", ErrInvalidDisplayName
	}
	if utf8.RuneCountInString(name) <= MaxHumanDisplayNameRunes {
		return name, nil
	}
	return "", ErrInvalidDisplayName
}

// NormalizeDisplayName is the registry's rule for a名乗り, exported so other
// boundaries (messaging の個人設定) apply the identical normalization instead of
// inventing a second one. Humans and PersonalityAgents share it: a participant
// is a participant.
func NormalizeDisplayName(raw string) (string, error) {
	return normalizeHumanDisplayName(raw)
}

// ErrAgentNotFound is returned when a PersonalityAgentId names no live agent.
var ErrAgentNotFound = errors.New("personality agent not found")

// UpdateAgentDisplayName renames a PersonalityAgent. The agents table belongs
// to the 戸籍, so the write lives here even though the caller is the messaging
// surface — and the agent renaming itself goes through the same door a Human
// does (AX: UIだけにある操作を作らない).
func (s *Store) UpdateAgentDisplayName(ctx context.Context, agentID, raw string) (string, error) {
	name, err := normalizeHumanDisplayName(raw)
	if err != nil {
		return "", err
	}
	var stored string
	if err := s.pool.QueryRow(ctx,
		`UPDATE agents SET display_name=$2 WHERE personality_agent_id=$1
		 RETURNING display_name`, agentID, name).Scan(&stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrAgentNotFound
		}
		return "", fmt.Errorf("update PersonalityAgent display name: %w", err)
	}
	return stored, nil
}

// initialHumanDisplayName treats a malformed verified provider label as absent
// so profile metadata can never prevent authentication.
func initialHumanDisplayName(raw string) string {
	name, err := normalizeHumanDisplayName(raw)
	if err != nil {
		return ""
	}
	return name
}

// HumanDisplayName returns the canonical name owned by the Human registry.
func (s *Store) HumanDisplayName(ctx context.Context, humanID string) (string, error) {
	var name string
	if err := s.pool.QueryRow(ctx,
		"SELECT display_name FROM humans WHERE human_id=$1", humanID,
	).Scan(&name); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrHumanNotFound
		}
		return "", fmt.Errorf("read Human display name: %w", err)
	}
	return name, nil
}

// UpdateHumanDisplayName is the explicit self-service override. Setting the
// customized bit in the same statement prevents every later Firebase login
// from silently replacing the Human's chosen name.
func (s *Store) UpdateHumanDisplayName(ctx context.Context, humanID, raw string) (string, error) {
	name, err := normalizeHumanDisplayName(raw)
	if err != nil {
		return "", err
	}
	var stored string
	if err := s.pool.QueryRow(ctx, `UPDATE humans
		SET display_name=$2, display_name_customized=true, display_name_initialized=true
		WHERE human_id=$1 RETURNING display_name`, humanID, name).Scan(&stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrHumanNotFound
		}
		return "", fmt.Errorf("update Human display name: %w", err)
	}
	return stored, nil
}

// SeedHumanDisplayName upgrades only the historical creation sentinel and only
// while no explicit Sumi settings choice exists. Provider profile metadata is
// therefore an initial label, never identity and never a durable override.
func (s *Store) SeedHumanDisplayName(ctx context.Context, humanID, raw string) error {
	name := initialHumanDisplayName(raw)
	if name == "" {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE humans SET display_name=$2, display_name_initialized=true
		WHERE human_id=$1 AND NOT display_name_customized
		  AND NOT display_name_initialized AND display_name='Sumi'`, humanID, name)
	if err != nil {
		return fmt.Errorf("seed Human display name: %w", err)
	}
	return nil
}

func seedHumanDisplayNameTx(ctx context.Context, tx pgx.Tx, humanID, raw string) error {
	name := initialHumanDisplayName(raw)
	if name == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE humans SET display_name=$2, display_name_initialized=true
		WHERE human_id=$1 AND NOT display_name_customized
		  AND NOT display_name_initialized AND display_name='Sumi'`, humanID, name)
	return err
}
