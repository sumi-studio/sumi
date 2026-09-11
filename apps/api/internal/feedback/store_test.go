package feedback

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

type world struct {
	s                        *Store
	pool                     *pgxpool.Pool
	human, dev, stranger, pa participant.Ref
}

func fixture(t *testing.T) world {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	k := koseki.New(pool)
	mint := func() string {
		id, err := k.MintHuman(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	w := world{pool: pool, human: participant.Human(mint()), dev: participant.Human(mint()), stranger: participant.Human(mint())}
	paid, err := k.MintSecretary(ctx, w.human.ID)
	if err != nil {
		t.Fatal(err)
	}
	w.pa = participant.PersonalityAgent(paid)
	for _, p := range []participant.Ref{w.human, w.dev, w.stranger, w.pa} {
		if _, err := apps.New(pool, nil).InstallAtOperation(ctx, apps.ParticipantOwner(p), p, AppID, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	w.s = New(pool, []participant.Ref{w.dev})
	return w
}
func TestFeedbackSharedConversationAndUnread(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	b, err := w.s.Bootstrap(ctx, w.pa)
	if err != nil || !b.Available || !b.Enabled {
		t.Fatalf("bootstrap: %+v %v", b, err)
	}
	thread, err := w.s.Create(ctx, w.pa, "Notification mismatch", "The channel says all, but only mentions arrive.", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	// An employer does not gain the PA's inbox, even though the PA belongs to them.
	for _, p := range []participant.Ref{w.human, w.stranger} {
		if _, err := w.s.Open(ctx, p, thread.ID, ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-participant open: %v", err)
		}
		list, err := w.s.List(ctx, p, "all", "")
		if err != nil || len(list.Threads) != 0 {
			t.Fatalf("leaked list: %+v %v", list, err)
		}
	}
	detail, err := w.s.Open(ctx, w.dev, thread.ID, "")
	if err != nil || !detail.Thread.Unread {
		t.Fatalf("developer unread %+v %v", detail, err)
	}
	if err = w.s.Read(ctx, w.dev, thread.ID, 1); err != nil {
		t.Fatal(err)
	}
	resolved, err := w.s.Status(ctx, w.dev, thread.ID, "resolved", 1)
	if err != nil || resolved.Revision != 2 {
		t.Fatalf("resolve %+v %v", resolved, err)
	}
	reply, err := w.s.Reply(ctx, w.dev, thread.ID, "Fixed; please try again.", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	detail, err = w.s.Open(ctx, w.pa, thread.ID, "")
	if err != nil || !detail.Thread.Unread || detail.Thread.Status != "resolved" || len(detail.Activities) != 1 || len(detail.Messages) != 1 {
		t.Fatalf("resolved followup %+v %v", detail, err)
	}
	if detail.Activities[0].Author.Participant.HumanID != w.dev.ID || detail.Messages[0].ID != reply.ID {
		t.Fatal("event identities")
	}
	if err = w.s.Read(ctx, w.pa, thread.ID, 2); err != nil {
		t.Fatal(err)
	}
	detail, _ = w.s.Open(ctx, w.pa, thread.ID, "")
	if !detail.Thread.Unread {
		t.Fatal("read old status swallowed later reply")
	}
	if err = w.s.Read(ctx, w.pa, thread.ID, reply.Revision); err != nil {
		t.Fatal(err)
	}
	if err = w.s.Read(ctx, w.pa, thread.ID, 1); err != nil {
		t.Fatal(err)
	}
	detail, _ = w.s.Open(ctx, w.pa, thread.ID, "")
	if detail.Thread.Unread {
		t.Fatal("read cursor regressed")
	}
	if err = w.s.Read(ctx, w.pa, thread.ID, 100); !errors.Is(err, ErrInvalid) {
		t.Fatalf("future read accepted: %v", err)
	}
	if _, err = w.s.Status(ctx, w.pa, thread.ID, "open", 1); !errors.Is(err, ErrRevision) {
		t.Fatalf("stale status accepted: %v", err)
	}
}
func TestFeedbackRetryAndLifecycle(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	nonce := uuid.NewString()
	var wg sync.WaitGroup
	ids := make(chan string, 6)
	errs := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			th, err := w.s.Create(ctx, w.human, "Question", "Can I suggest something?", nonce)
			ids <- th.ID
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	id := ""
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	for next := range ids {
		if id != "" && id != next {
			t.Fatal("duplicate retry")
		}
		id = next
	}
	if _, err := w.s.Create(ctx, w.human, "Different", "Can I suggest something?", nonce); !errors.Is(err, ErrRequest) {
		t.Fatalf("reused nonce %v", err)
	}
	rn := uuid.NewString()
	m, err := w.s.Reply(ctx, w.human, id, "More detail", rn)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := w.s.Reply(ctx, w.human, id, "More detail", rn)
	if err != nil || m.ID != retry.ID {
		t.Fatalf("reply retry %v", err)
	}
	if _, err = w.s.Reply(ctx, w.human, id, "Changed detail", rn); !errors.Is(err, ErrRequest) {
		t.Fatal(err)
	}
	if _, err = w.pool.Exec(ctx, `UPDATE app_installations SET enabled=false WHERE owner_kind='human' AND owner_id=$1 AND app_id='feedback'`, w.human.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = w.s.Open(ctx, w.human, id, ""); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled read %v", err)
	}
	if _, err = w.pool.Exec(ctx, `DELETE FROM app_installations WHERE owner_kind='human' AND owner_id=$1 AND app_id='feedback'`, w.human.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = w.s.List(ctx, w.human, "all", ""); !errors.Is(err, ErrInstallation) {
		t.Fatal(err)
	}
	if _, err = apps.New(w.pool, nil).InstallAtOperation(ctx, apps.ParticipantOwner(w.human), w.human, AppID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	detail, err := w.s.Open(ctx, w.human, id, "")
	if err != nil || len(detail.Messages) != 1 {
		t.Fatalf("uninstall lost conversation: %+v %v", detail, err)
	}
}
func TestFeedbackBoundedPaginationAndAvailability(t *testing.T) {
	w := fixture(t)
	ctx := context.Background()
	off := New(w.pool, nil)
	b, err := off.Bootstrap(ctx, w.human)
	if err != nil || b.Available {
		t.Fatal("unconfigured available")
	}
	if _, err = off.Create(ctx, w.human, "No destination", "Please deliver", uuid.NewString()); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	t0, err := w.s.Create(ctx, w.human, "A long conversation", "Initial", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 53; i++ {
		if _, err = w.s.Reply(ctx, w.human, t0.ID, fmt.Sprint(i), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	newest, err := w.s.Open(ctx, w.dev, t0.ID, "")
	if err != nil || len(newest.Messages) != 50 || newest.NextCursor == nil {
		t.Fatalf("newest %+v %v", newest, err)
	}
	older, err := w.s.Open(ctx, w.dev, t0.ID, *newest.NextCursor)
	if err != nil || len(older.Messages) != 3 || older.NextCursor != nil || older.Messages[2].Revision >= newest.Messages[0].Revision {
		t.Fatalf("older %+v %v", older, err)
	}
	for i := 0; i < 50; i++ {
		if _, err = w.s.Create(ctx, w.human, fmt.Sprint("Report ", i), "Text", uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	list, err := w.s.List(ctx, w.dev, "all", "")
	if err != nil || len(list.Threads) != 50 || list.NextCursor == nil {
		t.Fatalf("list %+v %v", list, err)
	}
	rest, err := w.s.List(ctx, w.dev, "all", *list.NextCursor)
	if err != nil || len(rest.Threads) != 1 || rest.NextCursor != nil {
		t.Fatalf("rest %+v %v", rest, err)
	}
}
