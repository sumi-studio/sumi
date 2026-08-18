package messaging

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func statusOf(t *testing.T, statuses []ParticipantStatus, who ParticipantRef) (ParticipantStatus, bool) {
	t.Helper()
	for _, status := range statuses {
		if status.Participant == who {
			return status, true
		}
	}
	return ParticipantStatus{}, false
}

func TestStatusIsReplacedInPlaceAndExpiresAtReadTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "取り込み中", nil); err != nil {
		t.Fatalf("set status: %v", err)
	}
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses: %v", err)
	}
	got, ok := statusOf(t, statuses, w.humanA)
	if !ok || got.Status != StatusBusy || got.Note != "取り込み中" || got.ExpiresAt != nil {
		t.Fatalf("status = %+v (found %v)", got, ok)
	}

	// Setting again replaces the one row rather than accumulating history.
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "", nil); err != nil {
		t.Fatalf("replace status: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after replace: %v", err)
	}
	got, _ = statusOf(t, statuses, w.humanA)
	if got.Status != StatusAway || got.Note != "" {
		t.Fatalf("replaced status = %+v", got)
	}
	if n := len(statuses); n != 1 {
		t.Fatalf("statuses = %d, want exactly one row per participant", n)
	}

	// An expired temporary status is resolved at read time, so no sweeper can
	// disagree with what readers see. With a lasting declaration underneath,
	// what is reported is that declaration — not「対応可能」and not nothing.
	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set expiring status: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatalf("list statuses after expiry: %v", err)
	}
	got, ok = statusOf(t, statuses, w.humanA)
	if !ok || got.Status != StatusAway || got.Note != "" || got.ExpiresAt != nil {
		t.Fatalf("lapsed status = %+v (found %v), want the lasting away", got, ok)
	}

	// With nothing behind it, the lapse ends the declaration outright.
	if _, err := w.store.SetStatus(ctx, w.humanB, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set expiring status without a base: %v", err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list own statuses: %v", err)
	}
	if got, ok := statusOf(t, statuses, w.humanB); !ok || got.Status != "" || got.Revision != 1 {
		t.Fatalf("a lapsed status with no base must report its empty revisioned projection, got %+v (found %v)", got, ok)
	}

	// Only the three declared values exist; anything else fails closed.
	if _, err := w.store.SetStatus(ctx, w.humanA, "invisible", "", nil); err == nil {
		t.Fatal("unknown status must be rejected")
	}
}

func TestStatusVisibilityIsBoundToExactWorkspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "", nil); err != nil {
		t.Fatalf("set status: %v", err)
	}
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); !ok {
		t.Fatalf("shared exact Workspace did not expose status: %+v", statuses)
	}
	strangerID, err := koseki.New(w.store.core.pool).MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stranger := Human(strangerID)
	if _, err := w.store.createWorkspace(ctx, "isolated", stranger); err != nil {
		t.Fatal(err)
	}
	statuses, err = w.store.StatusesVisibleTo(ctx, stranger)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := statusOf(t, statuses, w.humanA); ok {
		t.Fatalf("status leaked across exact Workspace: %+v", statuses)
	}
}

func TestStatusOverHTTPPublishesToParticipantScopedSubscribers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	conn := dialWS(t, ts, w.humanB.ID, nil)
	resp, body := call(t, ts, http.MethodPut, "/messaging/status", w.humanA.ID,
		map[string]any{"status": "busy", "note": "取り込み中"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set status: status %d body %v", resp.StatusCode, body)
	}
	if body["status"] != "busy" || body["note"] != "取り込み中" || body["expires_at"] != nil {
		t.Fatalf("status body = %v", body)
	}
	if body["revision"] != float64(1) {
		t.Fatalf("status revision = %v, want 1", body["revision"])
	}

	// status_updated carries no place: it is scoped to the participant, and the
	// subscriber receives it because they share a workspace.
	frame := readFrame(t, conn)
	event := frame["event"].(map[string]any)
	if event["type"] != EventStatusUpdated {
		t.Fatalf("event = %v", event)
	}
	if _, hasPlace := event["place_id"]; hasPlace {
		t.Fatalf("status event must not claim a place: %v", event)
	}
	status := event["status"].(map[string]any)
	if status["status"] != "busy" ||
		status["participant"].(map[string]any)["human_id"] != w.humanA.ID {
		t.Fatalf("status event payload = %v", status)
	}
	if status["revision"] != float64(1) {
		t.Fatalf("status event revision = %v, want 1", status["revision"])
	}

	// Bootstrap reports the current value, since the event never replays.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d", resp.StatusCode)
	}
	statuses := body["statuses"].([]any)
	if len(statuses) != 1 || statuses[0].(map[string]any)["status"] != "busy" {
		t.Fatalf("bootstrap statuses = %v", statuses)
	}
	if statuses[0].(map[string]any)["revision"] != float64(1) {
		t.Fatalf("bootstrap status revision = %v, want 1", statuses[0])
	}

	// Nobody sets anybody else's: the value comes from the session only, and a
	// value outside the vocabulary is rejected before the store.
	resp, _ = call(t, ts, http.MethodPut, "/messaging/status", w.humanA.ID,
		map[string]any{"status": "invisible"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown status: status %d, want 400", resp.StatusCode)
	}
	resp, _ = call(t, ts, http.MethodPut, "/messaging/status", w.humanA.ID,
		map[string]any{"status": "busy", "participant": map[string]any{"kind": "human", "human_id": w.humanB.ID}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("participant field: status %d, want 400", resp.StatusCode)
	}
}

func TestLocalStatusSetsTheAgentsOwnAttentionState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	status, body := callLocal(t, ctx, server.localStatus, LocalStatusPath, map[string]any{
		"status": "busy", "note": "別の対応中です", "expires_in_minutes": 45,
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent status: status %d body %v", status, body)
	}
	declared := body["status"].(map[string]any)
	participant := declared["participant"].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent status participant = %v", participant)
	}
	if declared["status"] != "busy" || declared["note"] != "別の対応中です" {
		t.Fatalf("agent status = %v", declared)
	}
	if declared["expires_at"] == nil {
		t.Fatalf("a relative expiry must resolve to an instant: %v", declared)
	}

	// Explicit membership, not the Human-Agent relation, makes the status
	// visible through the same store the UI reads.
	w.workspaceWithChannel(t, ctx)
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatalf("list statuses: %v", err)
	}
	if got, ok := statusOf(t, statuses, w.agent); !ok || got.Status != StatusBusy {
		t.Fatalf("human view of the agent status = %+v (found %v)", got, ok)
	}

	// Same vocabulary, same bounds as the human lane.
	status, _ = callLocal(t, ctx, server.localStatus, LocalStatusPath, map[string]any{
		"status": "invisible",
	}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("unknown agent status: %d, want 400", status)
	}
}

// A timed status is a promise about a return, not just about an end: it has to
// say what it returns to, decide that at declaration time, and hold that answer
// even if another timed status is stacked on top of it.
func TestTimedStatusLapsesBackToTheDeclarationUnderneath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "在宅です", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	soon := time.Now().Add(time.Hour)
	declared, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &soon)
	if err != nil {
		t.Fatalf("set timed status: %v", err)
	}
	// The declaration itself already names the return, so a screen can say
	// 「期限が来たら『離席中』に戻ります」before the hour is up.
	if declared.BaseStatus != StatusAway || declared.BaseNote != "在宅です" {
		t.Fatalf("timed status = %+v, want the lasting away recorded as its base", declared)
	}

	// Stacking a second timed status keeps the lasting declaration underneath.
	stacked, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "電話中", &soon)
	if err != nil {
		t.Fatalf("stack timed status: %v", err)
	}
	if stacked.BaseStatus != StatusAway || stacked.BaseNote != "在宅です" {
		t.Fatalf("stacked status = %+v, want the base to survive", stacked)
	}

	// A lasting declaration has nothing to return to.
	lasting, err := w.store.SetStatus(ctx, w.humanA, StatusAvailable, "", nil)
	if err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	if lasting.BaseStatus != "" || lasting.ExpiresAt != nil {
		t.Fatalf("lasting status = %+v, want no base", lasting)
	}
}

func TestStatusRevisionAdvancesForEveryDurableProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	lasting, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "在宅です", nil)
	if err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	if lasting.Revision != 1 {
		t.Fatalf("first status revision = %d, want 1", lasting.Revision)
	}
	past := time.Now().Add(-time.Minute)
	temporary, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past)
	if err != nil {
		t.Fatalf("set temporary status: %v", err)
	}
	if temporary.Revision != 2 {
		t.Fatalf("replacement status revision = %d, want 2", temporary.Revision)
	}

	expiries, err := collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(expiries) != 1 || expiries[0].Status.Revision != 3 {
		t.Fatalf("expiry revisions = %+v, want one revision 3", expiries)
	}
	if expiries[0].Status.Status != StatusAway {
		t.Fatalf("expiry status = %+v, want restored away", expiries[0].Status)
	}
}

// The same guarantee has to hold for a participant who has never declared
// anything. Ordering used to come from locking the existing row, so a
// participant with no row had nothing to serialize on: two first declarations
// each concluded「下には何も無い」, and the temporary one committed last would
// erase the lasting one instead of promising to return to it — an hour later
// the participant would be saying nothing at all.
//
// This is the same footing as the sweeper: the row is what orders the
// statements. Deriving the base inside the writing statement makes the second
// writer wait on the key and read what the first actually committed, whether
// or not the row existed when it started.
func TestATimedStatusTakesItsBaseFromTheDeclarationThatCommittedFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	// A lasting declaration in flight on another connection, written exactly
	// as SetStatus writes one. Nothing is committed yet, so there is no row
	// for the timed declaration below to find or lock.
	lasting, err := w.store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the lasting declaration: %v", err)
	}
	defer func() { _ = lasting.Rollback(context.Background()) }()
	if _, err := lasting.Exec(ctx, `
		INSERT INTO participant_statuses
			(member_kind, member_id, status, note, expires_at, base_status, base_note)
		VALUES ($1, $2, $3, $4, NULL, NULL, '')`,
		w.humanA.Kind, w.humanA.ID, StatusAway, "在宅です"); err != nil {
		t.Fatalf("write the lasting declaration: %v", err)
	}

	type declaration struct {
		status ParticipantStatus
		err    error
	}
	soon := time.Now().Add(time.Hour)
	done := make(chan declaration, 1)
	go func() {
		status, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &soon)
		done <- declaration{status: status, err: err}
	}()
	waitForWaitingBackend(t, ctx, w.store.pool)
	if err := lasting.Commit(ctx); err != nil {
		t.Fatalf("commit the lasting declaration: %v", err)
	}

	timed := <-done
	if timed.err != nil {
		t.Fatalf("set timed status: %v", timed.err)
	}
	if timed.status.BaseStatus != StatusAway || timed.status.BaseNote != "在宅です" {
		t.Fatalf("timed status = %+v, want the lasting declaration as its base", timed.status)
	}

	// And it really returns there rather than ending. The hour is moved into
	// the past directly; what is under test is the base, not the clock.
	if _, err := w.store.pool.Exec(ctx, `
		UPDATE participant_statuses SET expires_at = now() - interval '1 minute'
		WHERE member_kind = $1 AND member_id = $2`,
		w.humanA.Kind, w.humanA.ID); err != nil {
		t.Fatalf("move the expiry into the past: %v", err)
	}
	expiries, err := collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(expiries) != 1 || expiries[0].Status.Status != StatusAway ||
		expiries[0].Status.Note != "在宅です" {
		t.Fatalf("expiries = %+v, want a lapse back to the lasting declaration", expiries)
	}
}

// The sweep only makes the read-time answer durable and announces it. It must
// not invent a state nobody declared, and it must have somewhere to announce to.
func TestExpireStatusesRestoresTheBaseAndClearsWhatHasNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)

	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "在宅です", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status with a base: %v", err)
	}
	if _, err := w.store.SetStatus(ctx, w.humanB, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status without a base: %v", err)
	}

	expiries, err := collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(expiries) != 2 {
		t.Fatalf("expiries = %d, want both lapsed rows", len(expiries))
	}
	for _, expiry := range expiries {
		switch expiry.Status.Participant {
		case w.humanA:
			if expiry.Status.Status != StatusAway || expiry.Status.Note != "在宅です" {
				t.Fatalf("restored status = %+v", expiry.Status)
			}
		case w.humanB:
			// An empty status is how the sweep says「もう何も言っていない」.
			if expiry.Status.Status != "" {
				t.Fatalf("cleared status = %+v", expiry.Status)
			}
		default:
			t.Fatalf("unexpected expiry subject %+v", expiry.Status.Participant)
		}
		// The announcement has an address to go to, and it is the Messaging
		// installation of a Workspace they are actually in.
		if len(expiry.Scopes) != 1 || expiry.Scopes[0].WorkspaceID != workspace.WorkspaceID {
			t.Fatalf("expiry scopes = %+v", expiry.Scopes)
		}
	}

	// Sweeping is idempotent: the second pass finds nothing left to lapse.
	again, err := collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("second expire: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second sweep = %+v, want nothing", again)
	}

	// And the durable rows now agree with what readers were already told.
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := statusOf(t, statuses, w.humanA); !ok || got.Status != StatusAway || got.ExpiresAt != nil {
		t.Fatalf("status after sweep = %+v (found %v)", got, ok)
	}
	if got, ok := statusOf(t, statuses, w.humanB); !ok || got.Status != "" || got.Revision != 2 {
		t.Fatalf("cleared status = %+v (found %v), want empty revision 2", got, ok)
	}
}

// collectExpiries runs one sweep and records what it announced. The sweep hands
// each lapse over inside its own transaction, so this is the only way to see
// what it said — there is no separate read to inspect afterwards.
func collectExpiries(ctx context.Context, store *Store) ([]StatusExpiry, error) {
	var announced []StatusExpiry
	err := store.ExpireStatuses(ctx, func(_ context.Context, expiry StatusExpiry) {
		announced = append(announced, expiry)
	})
	return announced, err
}

// A lapse is only worth announcing while it is still the last word. If the
// participant declares something new before the sweep runs, there is nothing
// left to lapse — and saying so anyway would put a stale state on every open
// screen, after the newer declaration had already arrived.
func TestSweepSaysNothingAboutAParticipantWhoDeclaredSomethingNewFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "在宅です", nil); err != nil {
		t.Fatalf("set lasting status: %v", err)
	}
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status: %v", err)
	}
	// The row is already eligible to lapse. The participant speaks again first.
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAvailable, "戻りました", nil); err != nil {
		t.Fatalf("declare something new: %v", err)
	}

	announced, err := collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(announced) != 0 {
		t.Fatalf("sweep announced %+v, want silence", announced)
	}

	// And it changed nothing: the newest declaration is still what everyone reads.
	statuses, err := w.store.StatusesVisibleTo(ctx, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := statusOf(t, statuses, w.humanA)
	if !ok || got.Status != StatusAvailable || got.Note != "戻りました" || got.ExpiresAt != nil {
		t.Fatalf("status after the silent sweep = %+v (found %v)", got, ok)
	}
}

// A participant with nothing left to lapse must not be swept a second time: a
// clear is a one-time transition, not a state the sweep keeps re-announcing.
func TestSweepAnnouncesOnlyWhatItsOwnStatementsChanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	past := time.Now().Add(-time.Minute)
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusBusy, "会議中", &past); err != nil {
		t.Fatalf("set lapsed status: %v", err)
	}
	announced, err := collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("expire statuses: %v", err)
	}
	if len(announced) != 1 || announced[0].Status.Participant != w.humanA ||
		announced[0].Status.Status != "" {
		t.Fatalf("first sweep announced %+v", announced)
	}

	// Declaring again after the clear is a fresh statement, not a lapse, so the
	// next sweep still has nothing of its own to say.
	if _, err := w.store.SetStatus(ctx, w.humanA, StatusAway, "", nil); err != nil {
		t.Fatalf("declare again: %v", err)
	}
	announced, err = collectExpiries(ctx, w.store.core)
	if err != nil {
		t.Fatalf("second expire: %v", err)
	}
	if len(announced) != 0 {
		t.Fatalf("second sweep announced %+v, want silence", announced)
	}
}
