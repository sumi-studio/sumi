package todo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestPostgresTodoContract(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO humans (human_id) VALUES ($1), ($2)", ownerA, ownerB,
	); err != nil {
		t.Fatal(err)
	}

	service := newTestService(t, NewPostgresRepository(pool))
	futureAppNow := time.Now().UTC().Add(24 * time.Hour)
	service.now = func() time.Time { return futureAppNow }
	item, err := service.Create(ctx, ownerA, CreateInput{
		Title: "postgres", Due: &DueInput{Kind: DueKindDatetime, At: "2026-08-01T15:00:00+09:00", Timezone: "Asia/Tokyo"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !item.CreatedAt.Equal(item.UpdatedAt) || !item.CreatedAt.Before(futureAppNow.Add(-time.Hour)) {
		t.Fatalf("PostgreSQL timestamps followed the application clock: %+v", item)
	}
	if item.Due == nil || item.Due.At == nil || item.Due.At.Format(time.RFC3339) != "2026-08-01T15:00:00+09:00" {
		t.Fatalf("datetime response lost its timezone offset: %+v", item.Due)
	}
	if _, err := service.Get(ctx, ownerB, item.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner get returned %v", err)
	}

	roundTripDue := &DueInput{
		Kind: DueKindDatetime, At: item.Due.At.Format(time.RFC3339), Timezone: item.Due.Timezone,
	}
	item, err = service.Update(ctx, ownerA, item.ID, UpdateInput{
		ExpectedVersion: item.Version, DueSet: true, Due: roundTripDue,
	}, false)
	if err != nil {
		t.Fatalf("round-trip datetime response through update: %v", err)
	}

	title := "current"
	updated, err := service.Update(ctx, ownerA, item.ID, UpdateInput{ExpectedVersion: item.Version, Title: &title}, false)
	if err != nil {
		t.Fatal(err)
	}
	stale := "stale"
	if _, err := service.Update(ctx, ownerA, item.ID, UpdateInput{ExpectedVersion: item.Version, Title: &stale}, false); err == nil {
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
