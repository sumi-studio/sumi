package koseki

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestHumanProfileIsParticipantGlobalAndPersistsPartialUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")
	registration, err := store.AutoRegisterWithDisplayName(
		ctx, "firebase", "profile-owner", "Yohaku",
	)
	if err != nil {
		t.Fatal(err)
	}

	initial, err := store.HumanProfile(ctx, registration.HumanID)
	if err != nil {
		t.Fatal(err)
	}
	if initial.DisplayName != "Yohaku" || initial.Tagline != "" {
		t.Fatalf("initial profile = %+v", initial)
	}

	name, tagline := "  余白   ハク  ", "  開発  "
	updated, err := store.UpdateHumanProfile(
		ctx, registration.HumanID, &name, &tagline,
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated.DisplayName != "余白 ハク" || updated.Tagline != "開発" {
		t.Fatalf("updated profile = %+v", updated)
	}

	tagline = "設計"
	updated, err = store.UpdateHumanProfile(
		ctx, registration.HumanID, nil, &tagline,
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated.DisplayName != "余白 ハク" || updated.Tagline != "設計" {
		t.Fatalf("tagline-only profile = %+v", updated)
	}

	// A new store instance reads the same Participant-global row; no Workspace
	// context participates in the profile identity.
	reloaded, err := New(pool).HumanProfile(ctx, registration.HumanID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded != updated {
		t.Fatalf("reloaded profile = %+v, want %+v", reloaded, updated)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM participant_profiles
		WHERE member_kind='human' AND member_id=$1`, registration.HumanID,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("participant profile rows = %d", rows)
	}
}

func TestHumanProfileRejectsInvalidFieldsWithoutPartialWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := connectTestPool(t, ctx)
	store := NewWithWrappingKeyID(pool, "test-wrapping/v1")
	registration, err := store.AutoRegisterWithDisplayName(
		ctx, "firebase", "profile-validation", "Before",
	)
	if err != nil {
		t.Fatal(err)
	}

	validName := "After"
	for _, tagline := range []string{
		"line one\nline two",
		"safe\u202edanger",
		"\u200d",
		strings.Repeat("名", MaxHumanTaglineRunes+1),
	} {
		if _, err := store.UpdateHumanProfile(
			ctx, registration.HumanID, &validName, &tagline,
		); !errors.Is(err, ErrInvalidTagline) {
			t.Fatalf("tagline %q: got %v", tagline, err)
		}
	}
	profile, err := store.HumanProfile(ctx, registration.HumanID)
	if err != nil {
		t.Fatal(err)
	}
	if profile.DisplayName != "Before" || profile.Tagline != "" {
		t.Fatalf("refused patch changed profile: %+v", profile)
	}
	if _, err := store.UpdateHumanProfile(
		ctx, registration.HumanID, nil, nil,
	); !errors.Is(err, ErrEmptyHumanProfilePatch) {
		t.Fatalf("empty patch: %v", err)
	}

	allowed := "家族\u200d👩"
	if profile, err := store.UpdateHumanProfile(
		ctx, registration.HumanID, nil, &allowed,
	); err != nil || profile.Tagline != allowed {
		t.Fatalf("ZWJ tagline = %+v, %v", profile, err)
	}
}
