package todo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/migrations"
)

func TestPostgresTodoContract(t *testing.T) {
	databaseURL := os.Getenv("SUMI_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUMI_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("todo_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE") }()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "CREATE TABLE users (user_id UUID PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Run(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO users (user_id) VALUES ($1), ($2)", ownerA, ownerB); err != nil {
		t.Fatal(err)
	}

	service := newTestService(t, NewPostgresRepository(pool))
	item, err := service.Create(ctx, ownerA, CreateInput{
		Title: "postgres", Due: &DueInput{Kind: DueKindDatetime, At: "2026-08-01T15:00:00+09:00", Timezone: "Asia/Tokyo"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Get(ctx, ownerB, item.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner get returned %v", err)
	}

	title := "current"
	updated, err := service.Update(ctx, ownerA, item.ID, UpdateInput{ExpectedVersion: 1, Title: &title}, false)
	if err != nil {
		t.Fatal(err)
	}
	stale := "stale"
	if _, err := service.Update(ctx, ownerA, item.ID, UpdateInput{ExpectedVersion: 1, Title: &stale}, false); err == nil {
		t.Fatal("stale update unexpectedly succeeded")
	}
	current, err := service.Get(ctx, ownerA, item.ID)
	if err != nil || current.Title != title || current.Version != updated.Version {
		t.Fatalf("stale update changed row: %+v, %v", current, err)
	}

	replacement := &DueInput{Kind: DueKindDate, Date: "2026-08-02", Timezone: "Asia/Tokyo"}
	updated, err = service.Update(ctx, ownerA, item.ID, UpdateInput{
		ExpectedVersion: updated.Version, DueSet: true, Due: replacement,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	var dueAtIsNull bool
	if err := pool.QueryRow(ctx, "SELECT due_at IS NULL FROM todos WHERE id = $1 AND owner_user_id = $2", item.ID, ownerA).Scan(&dueAtIsNull); err != nil {
		t.Fatal(err)
	}
	if !dueAtIsNull || updated.Due == nil || updated.Due.Kind != DueKindDate {
		t.Fatalf("datetime-to-date replacement retained due_at: %+v", updated.Due)
	}

	for _, timezone := range []string{"Asia/Tokyo", "UTC"} {
		location, _ := time.LoadLocation(timezone)
		yesterday := time.Now().In(location).AddDate(0, 0, -1).Format("2006-01-02")
		if _, err := service.Create(ctx, ownerA, CreateInput{
			Title: "overdue " + timezone, Due: &DueInput{Kind: DueKindDate, Date: yesterday, Timezone: timezone},
		}, false); err != nil {
			t.Fatal(err)
		}
	}
	overdue, err := service.List(ctx, ownerA, ListFilter{Overdue: true, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, candidate := range overdue.Items {
		if strings.HasPrefix(candidate.Title, "overdue ") {
			matched++
		}
	}
	if matched != 2 {
		t.Fatalf("got %d overdue timezone fixtures, want 2", matched)
	}
}
