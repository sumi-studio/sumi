// e2e-auth-email-seed is a test-only fixture for the email-code browser
// journey in apps/web/e2e. It creates an existing Sumi account the way an
// earlier sign-in would have: a verified Firebase Auth emulator user bound to a
// newly registered Human. It is not wired into cmd/server and refuses to run
// without the Firebase Auth emulator.
//
//	SUMI_E2E_AUTH_SEED_DATABASE_URL     postgres://... (already migrated)
//	SUMI_E2E_AUTH_SEED_WRAPPING_KEY_ID  the server's SUMI_AGENT_WRAPPING_KEY_ID
//	SUMI_E2E_AUTH_SEED_PROJECT_ID       Firebase project of the emulator
//	SUMI_E2E_AUTH_SEED_FIREBASE_UID     UID to create
//	SUMI_E2E_AUTH_SEED_EMAIL            verified email of that UID
//	FIREBASE_AUTH_EMULATOR_HOST         required
//
// It prints the new Human ID.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	firebase "firebase.google.com/go/v4"
	firebaseauth "firebase.google.com/go/v4/auth"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	env := func(name string) string { return strings.TrimSpace(os.Getenv(name)) }
	databaseURL := env("SUMI_E2E_AUTH_SEED_DATABASE_URL")
	wrappingKeyID := env("SUMI_E2E_AUTH_SEED_WRAPPING_KEY_ID")
	projectID := env("SUMI_E2E_AUTH_SEED_PROJECT_ID")
	uid := env("SUMI_E2E_AUTH_SEED_FIREBASE_UID")
	email := env("SUMI_E2E_AUTH_SEED_EMAIL")
	if databaseURL == "" || wrappingKeyID == "" || projectID == "" || uid == "" || email == "" {
		return errors.New("SUMI_E2E_AUTH_SEED_{DATABASE_URL,WRAPPING_KEY_ID,PROJECT_ID,FIREBASE_UID,EMAIL} are required")
	}
	if env("FIREBASE_AUTH_EMULATOR_HOST") == "" {
		return errors.New("FIREBASE_AUTH_EMULATOR_HOST is required; this fixture never touches a real Firebase project")
	}
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID})
	if err != nil {
		return fmt.Errorf("firebase app: %w", err)
	}
	client, err := app.Auth(ctx)
	if err != nil {
		return fmt.Errorf("firebase auth: %w", err)
	}
	if _, err := client.CreateUser(ctx, (&firebaseauth.UserToCreate{}).UID(uid).Email(email).EmailVerified(true)); err != nil {
		return fmt.Errorf("create emulator user: %w", err)
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	registration, err := koseki.NewWithWrappingKeyID(pool, wrappingKeyID).AutoRegisterWithDisplayName(ctx, "firebase", uid, "Email E2E")
	if err != nil {
		return fmt.Errorf("register Human: %w", err)
	}
	fmt.Println(registration.HumanID)
	return nil
}
