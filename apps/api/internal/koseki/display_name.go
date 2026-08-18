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

// ValidateDisplayText rejects characters that can change how a displayed
// string is laid out or interpreted. ZWNJ and ZWJ are the only format
// controls retained: scripts and emoji sequences use them as joiners.
//
// This is shared by every sender-controlled display string. Keep the
// single-line rule here too, so a field cannot become a looser display path.
func ValidateDisplayText(raw string) error {
	for _, r := range raw {
		if unicode.IsControl(r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return ErrInvalidDisplayName
		}
		if unicode.In(r, unicode.Cf) && r != '\u200c' && r != '\u200d' {
			return ErrInvalidDisplayName
		}
	}
	return nil
}

// normalizeHumanDisplayName makes profile text safe to persist while
// preserving ordinary Unicode names and the joiners used by emoji/scripts.
func normalizeHumanDisplayName(raw string) (string, error) {
	if err := ValidateDisplayText(raw); err != nil {
		return "", err
	}
	var cleaned strings.Builder
	cleaned.Grow(len(raw))
	visible := false
	for _, r := range raw {
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

// initialHumanDisplayName treats a malformed verified provider label as absent
// so profile metadata can never prevent authentication.
func initialHumanDisplayName(raw string) string {
	name, err := normalizeHumanDisplayName(raw)
	if err != nil {
		return ""
	}
	return name
}

// ErrAgentNotFound reports a PersonalityAgent the registry does not know.
var ErrAgentNotFound = errors.New("PersonalityAgent not found")

// NormalizeDisplayName exposes the registry's one name rule to callers that
// write a profile from another package. The rule stays here because the 戸籍
// remains the canonical registry of names, whichever surface offers the field.
func NormalizeDisplayName(raw string) (string, error) {
	return normalizeHumanDisplayName(raw)
}

// nameRowQuerier is the subset of pgx satisfied by both a pool and a caller's
// transaction. A profile write needs the latter: the canonical name and the
// rest of the profile must land in one transaction or in neither.
type nameRowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ResolveHumanDisplayNameTx locks one Human's canonical row inside the caller's
// transaction and returns the name it ends up with. A nil name reads without
// changing anything; the lock is taken either way, so two concurrent profile
// writes for one participant serialize instead of interleaving.
func ResolveHumanDisplayNameTx(ctx context.Context, q nameRowQuerier, humanID string, name *string) (string, error) {
	var current string
	err := q.QueryRow(ctx,
		"SELECT display_name FROM humans WHERE human_id=$1 FOR UPDATE", humanID,
	).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrHumanNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock Human display name: %w", err)
	}
	if name == nil {
		return current, nil
	}
	normalized, err := normalizeHumanDisplayName(*name)
	if err != nil {
		return "", err
	}
	if normalized == current {
		return current, nil
	}
	if err := q.QueryRow(ctx, `UPDATE humans
		SET display_name=$2, display_name_customized=true, display_name_initialized=true
		WHERE human_id=$1 RETURNING display_name`, humanID, normalized,
	).Scan(&current); err != nil {
		return "", fmt.Errorf("update Human display name: %w", err)
	}
	return current, nil
}

// ResolveAgentDisplayNameTx is the PersonalityAgent half of the same rule. A
// PersonalityAgent names itself through the validation a Human passes, so
// neither side can carry a name the other could not.
func ResolveAgentDisplayNameTx(ctx context.Context, q nameRowQuerier, agentID string, name *string) (string, error) {
	var current string
	err := q.QueryRow(ctx,
		"SELECT display_name FROM agents WHERE personality_agent_id=$1 FOR UPDATE", agentID,
	).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrAgentNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock PersonalityAgent display name: %w", err)
	}
	if name == nil {
		return current, nil
	}
	normalized, err := normalizeHumanDisplayName(*name)
	if err != nil {
		return "", err
	}
	if normalized == current {
		return current, nil
	}
	if err := q.QueryRow(ctx, `UPDATE agents SET display_name=$2
		WHERE personality_agent_id=$1 RETURNING display_name`, agentID, normalized,
	).Scan(&current); err != nil {
		return "", fmt.Errorf("update PersonalityAgent display name: %w", err)
	}
	return current, nil
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
