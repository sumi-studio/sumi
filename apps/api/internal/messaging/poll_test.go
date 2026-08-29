package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

type interleavingPollQuerier struct {
	querier
	once       sync.Once
	interleave func()
}

func (q *interleavingPollQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := q.querier.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &interleavingPollRows{
		Rows: rows,
		afterFirstScan: func() {
			q.once.Do(q.interleave)
		},
	}, nil
}

type interleavingPollRows struct {
	pgx.Rows
	once           sync.Once
	afterFirstScan func()
}

func (rows *interleavingPollRows) Scan(dest ...any) error {
	err := rows.Rows.Scan(dest...)
	if err == nil {
		rows.once.Do(rows.afterFirstScan)
	}
	return err
}

func appendTestPoll(
	t *testing.T,
	ctx context.Context,
	store *ScopedStore,
	placeID, nonce string,
	poll PollInput,
) Message {
	t.Helper()
	message, created, err := store.AppendMessage(ctx, AppendInput{
		PlaceID: placeID, ClientNonce: nonce, Poll: &poll,
	})
	if err != nil || !created {
		t.Fatalf("append poll %q: created=%t err=%v", nonce, created, err)
	}
	return message
}

func TestPollInputCanonicalizesUnicodeAndDigestShape(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	closesAt := now.Add(time.Hour).In(time.FixedZone("JST", 9*60*60)).Add(999 * time.Nanosecond)
	poll := PollInput{
		Question: "\u3000" + strings.Repeat("界", MaxPollQuestionChars) + "\u3000",
		ClosesAt: &closesAt,
		Options: []string{
			"\t" + strings.Repeat("選", MaxPollOptionChars) + "\n",
			"  別案  ",
		},
	}
	if err := poll.Validate(now); err != nil {
		t.Fatalf("valid Unicode boundary: %v", err)
	}
	if got := utf8RuneCount(poll.Question); got != MaxPollQuestionChars {
		t.Fatalf("canonical question code points = %d", got)
	}
	if poll.Options[1] != "別案" {
		t.Fatalf("canonical option = %q", poll.Options[1])
	}
	if poll.ClosesAt.Location() != time.UTC || poll.ClosesAt.Nanosecond()%1000 != 0 {
		t.Fatalf("canonical closes_at = %s", poll.ClosesAt)
	}

	for name, invalid := range map[string]PollInput{
		"question invalid UTF-8": {Question: string([]byte{0xff}), Options: []string{"A", "B"}},
		"option invalid UTF-8":   {Question: "question", Options: []string{string([]byte{0xff}), "B"}},
		"question NUL":           {Question: "bad\x00question", Options: []string{"A", "B"}},
		"option NUL":             {Question: "question", Options: []string{"bad\x00option", "B"}},
		"duplicate trimmed":      {Question: "question", Options: []string{" A ", "A"}},
		"too few":                {Question: "question", Options: []string{"A"}},
		"question too long":      {Question: strings.Repeat("界", MaxPollQuestionChars+1), Options: []string{"A", "B"}},
		"option too long":        {Question: "question", Options: []string{strings.Repeat("界", MaxPollOptionChars+1), "B"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invalid.Validate(now); !errors.Is(err, ErrInvalidPoll) {
				t.Fatalf("Validate() = %v, want ErrInvalidPoll", err)
			}
		})
	}

	equalDeadline := now
	closed := PollInput{Question: "equal", Options: []string{"A", "B"}, ClosesAt: &equalDeadline}
	if err := closed.Validate(now); !errors.Is(err, ErrInvalidPoll) {
		t.Fatalf("equal creation deadline = %v, want ErrInvalidPoll", err)
	}
	if !(Poll{ClosesAt: &equalDeadline}).Closed(now) {
		t.Fatal("poll must be closed at exact deadline equality")
	}

	plainNil := messageRequestDigest("hello", UrgencyNormal, "", nil, nil)
	plainEmpty := messageRequestDigest("hello", UrgencyNormal, "", []string{}, nil)
	if !bytes.Equal(plainNil, plainEmpty) {
		t.Fatal("nil and empty attachment lists did not share the canonical poll:null shape")
	}
	firstDerived := now.Add(30 * time.Minute)
	secondDerived := now.Add(31 * time.Minute)
	relativeA := &PollInput{
		Question: "relative", Options: []string{"A", "B"},
		ClosesAt: &firstDerived, RelativeClosesInMinutes: 30,
	}
	relativeB := &PollInput{
		Question: "relative", Options: []string{"A", "B"},
		ClosesAt: &secondDerived, RelativeClosesInMinutes: 30,
	}
	if err := relativeA.validateFields(); err != nil {
		t.Fatal(err)
	}
	if err := relativeB.validateFields(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(
		messageRequestDigest("", UrgencyNormal, "", nil, relativeA),
		messageRequestDigest("", UrgencyNormal, "", nil, relativeB),
	) {
		t.Fatal("relative poll digest retained an attempt-specific absolute time")
	}
	if bytes.Equal(plainNil, messageRequestDigest("hello", UrgencyNormal, "", nil, relativeA)) {
		t.Fatal("poll:null and a poll request shared a digest")
	}
}

func utf8RuneCount(value string) int {
	return len([]rune(value))
}

func TestPollLifecycleWholeChoiceReplacementAndNoVoteNotification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	closesAt := time.Now().Add(time.Hour)
	message := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-lifecycle", PollInput{
		Question: "  いつ？  ", Options: []string{" 今日 ", "明日", "来週"}, ClosesAt: &closesAt,
	})
	if message.Content != "" || message.Poll == nil || message.Poll.Question != "いつ？" ||
		message.Poll.Revision != 0 || len(message.Poll.Options) != 3 {
		t.Fatalf("created poll = %+v", message)
	}
	for _, option := range message.Poll.Options {
		if !validPollOptionID(option.OptionID) || option.Voters == nil {
			t.Fatalf("created option = %+v", option)
		}
	}

	var intentsBefore int
	if err := w.store.pool.QueryRow(ctx, `
		SELECT count(*) FROM message_notification_intents WHERE message_id = $1`,
		message.MessageID).Scan(&intentsBefore); err != nil {
		t.Fatal(err)
	}
	first := message.Poll.Options[0].OptionID
	second := message.Poll.Options[1].OptionID
	voted, err := b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{first})
	if err != nil || voted.Poll == nil || voted.Poll.Revision != 1 ||
		len(voted.Poll.Options[0].Voters) != 1 {
		t.Fatalf("first vote = %+v err=%v", voted.Poll, err)
	}
	if _, err := b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{first, second}); !errors.Is(err, ErrPollSingleChoice) {
		t.Fatalf("single-choice multi vote = %v", err)
	}
	voted, err = b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{second})
	if err != nil || voted.Poll.Revision != 2 || len(voted.Poll.Options[0].Voters) != 0 ||
		len(voted.Poll.Options[1].Voters) != 1 {
		t.Fatalf("replacement vote = %+v err=%v", voted.Poll, err)
	}
	voted, err = b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{})
	if err != nil || voted.Poll.Revision != 3 {
		t.Fatalf("withdraw = %+v err=%v", voted.Poll, err)
	}
	for _, option := range voted.Poll.Options {
		if len(option.Voters) != 0 {
			t.Fatalf("withdraw left voters: %+v", voted.Poll)
		}
	}
	multi := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-lifecycle-multi", PollInput{
		Question: "複数？", AllowMulti: true, Options: []string{"A", "B", "C"},
	})
	multiVoted, err := b.VotePoll(ctx, channel.PlaceID, multi.MessageID, []string{
		multi.Poll.Options[0].OptionID, multi.Poll.Options[2].OptionID,
	})
	if err != nil || multiVoted.Poll == nil || multiVoted.Poll.Revision != 1 ||
		len(multiVoted.Poll.Options[0].Voters) != 1 || len(multiVoted.Poll.Options[1].Voters) != 0 ||
		len(multiVoted.Poll.Options[2].Voters) != 1 {
		t.Fatalf("multi-choice vote = %+v err=%v", multiVoted.Poll, err)
	}
	var intentsAfter int
	if err := w.store.pool.QueryRow(ctx, `
		SELECT count(*) FROM message_notification_intents WHERE message_id = $1`,
		message.MessageID).Scan(&intentsAfter); err != nil {
		t.Fatal(err)
	}
	if intentsAfter != intentsBefore {
		t.Fatalf("vote changed notification intents: before=%d after=%d", intentsBefore, intentsAfter)
	}
}

func TestPollCreationKeepsThreadRevisionParticipationAndNotificationAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	thread, created, err := a.CreateThread(
		ctx, channel.PlaceID, "poll thread", "", "poll-thread-create",
	)
	if err != nil || !created {
		t.Fatalf("create thread: created=%t err=%v", created, err)
	}
	initialRevision := thread.Place.Revision
	message, fresh, err := b.AppendMessage(ctx, AppendInput{
		PlaceID: thread.Place.PlaceID, Content: "@Yohaku poll",
		ClientNonce: "poll-thread-message",
		Poll:        &PollInput{Question: "thread poll", Options: []string{"A", "B"}},
	})
	if err != nil || !fresh || message.Poll == nil {
		t.Fatalf("thread poll: fresh=%t message=%+v err=%v", fresh, message, err)
	}
	projected, err := a.ThreadFor(ctx, thread.Place.PlaceID)
	if err != nil {
		t.Fatal(err)
	}
	if projected.Place.Revision <= initialRevision || projected.MessageCount != 1 ||
		!containsParticipant(projected.Participants, w.humanA) ||
		!containsParticipant(projected.Participants, w.humanB) {
		t.Fatalf("thread projection after poll = %+v", projected)
	}
	var mentionIntent int
	if err := w.store.pool.QueryRow(ctx, `
		SELECT count(*) FROM message_notification_intents
		WHERE message_id = $1 AND recipient_kind = $2 AND recipient_id = $3
		  AND reason = 'mention'`, message.MessageID, w.humanA.Kind, w.humanA.ID,
	).Scan(&mentionIntent); err != nil {
		t.Fatal(err)
	}
	if mentionIntent != 1 {
		t.Fatalf("thread poll mention intents = %d, want one", mentionIntent)
	}
}

func containsParticipant(participants []ParticipantRef, target ParticipantRef) bool {
	for _, participant := range participants {
		if participant == target {
			return true
		}
	}
	return false
}

func TestPollReplayAfterCloseUsesCanonicalRequestAndChangedPollConflicts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	closesAt := time.Now().Add(2 * time.Second).UTC().Truncate(time.Microsecond)
	request := AppendInput{
		PlaceID: channel.PlaceID, ClientNonce: "poll-replay-after-close",
		Poll: &PollInput{Question: "締切後？", Options: []string{"はい", "いいえ"}, ClosesAt: &closesAt},
	}
	created, fresh, err := a.AppendMessage(ctx, request)
	if err != nil || !fresh {
		t.Fatalf("create: fresh=%t err=%v", fresh, err)
	}
	for time.Now().Before(closesAt) {
		if wait := time.Until(closesAt); wait > 0 {
			time.Sleep(wait)
		}
	}
	if time.Now().Before(closesAt) {
		t.Fatalf("test clock has not reached original closes_at %s", closesAt)
	}
	expiredNewNonce := request
	expiredNewNonce.ClientNonce = "poll-expired-new-nonce"
	if _, _, err := a.AppendMessage(ctx, expiredNewNonce); !errors.Is(err, ErrInvalidPoll) {
		t.Fatalf("expired request under a new nonce = %v, want ErrInvalidPoll", err)
	}
	replayed, fresh, err := a.AppendMessage(ctx, request)
	if err != nil || fresh || replayed.MessageID != created.MessageID || replayed.Poll == nil ||
		!replayed.Poll.Closed(time.Now()) {
		t.Fatalf("closed replay = %+v fresh=%t err=%v", replayed, fresh, err)
	}
	changed := request
	changed.Poll = &PollInput{Question: "変更", Options: []string{"はい", "いいえ"}, ClosesAt: &closesAt}
	if _, _, err := a.AppendMessage(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed closed replay = %v, want ErrIdempotencyConflict", err)
	}
}

func TestPollCloseEqualityAndConcurrentSameActorReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorldWithMaxConns(t, ctx, 12)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	equality := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-close-equality", PollInput{
		Question: "equal?", Options: []string{"A", "B"}, ClosesAt: &deadline,
	})
	if _, err := b.votePollWithClock(
		ctx, channel.PlaceID, equality.MessageID,
		[]string{equality.Poll.Options[0].OptionID}, func() time.Time { return deadline },
	); !errors.Is(err, ErrPollClosed) {
		t.Fatalf("vote at exact closes_at = %v, want ErrPollClosed", err)
	}

	concurrent := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-concurrent", PollInput{
		Question: "replace", Options: []string{"A", "B"},
	})
	start := make(chan struct{})
	results := make(chan Message, 2)
	errs := make(chan error, 2)
	for _, option := range concurrent.Poll.Options {
		optionID := option.OptionID
		go func() {
			<-start
			message, err := b.VotePoll(ctx, channel.PlaceID, concurrent.MessageID, []string{optionID})
			results <- message
			errs <- err
		}()
	}
	close(start)
	byRevision := make(map[int64]Message, 2)
	for range 2 {
		message := <-results
		if err := <-errs; err != nil {
			t.Fatalf("concurrent replacement: %v", err)
		}
		byRevision[message.Poll.Revision] = message
	}
	if len(byRevision) != 2 || byRevision[1].Poll == nil || byRevision[2].Poll == nil {
		t.Fatalf("concurrent revisions = %+v", byRevision)
	}
	history, err := b.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var final Message
	for _, message := range history {
		if message.MessageID == concurrent.MessageID {
			final = message
			break
		}
	}
	if final.Poll == nil || final.Poll.Revision != 2 {
		t.Fatalf("final concurrent poll = %+v", final.Poll)
	}
	if got, want := selectedOption(final.Poll, w.humanB), selectedOption(byRevision[2].Poll, w.humanB); got == "" || got != want {
		t.Fatalf("final selected option = %q, revision-2 snapshot = %q", got, want)
	}
}

func TestPollProjectionUsesOneStatementSnapshotAcrossConcurrentVote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	message := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-projection-snapshot", PollInput{
		Question: "snapshot?", Options: []string{"A", "B"},
	})

	var (
		voted   Message
		voteErr error
	)
	q := &interleavingPollQuerier{
		querier: w.store.pool,
		interleave: func() {
			voted, voteErr = b.VotePoll(ctx, channel.PlaceID, message.MessageID,
				[]string{message.Poll.Options[0].OptionID})
		},
	}
	projection := []Message{{MessageID: message.MessageID, PlaceID: channel.PlaceID}}
	if err := attachPollsWith(ctx, q, projection); err != nil {
		t.Fatal(err)
	}
	if voteErr != nil {
		t.Fatalf("interleaved vote: %v", voteErr)
	}
	if voted.Poll == nil || voted.Poll.Revision != 1 ||
		len(voted.Poll.Options[0].Voters) != 1 {
		t.Fatalf("committed interleaved vote = %+v", voted.Poll)
	}
	poll := projection[0].Poll
	if poll == nil {
		t.Fatal("projection omitted poll")
	}
	voters := 0
	for _, option := range poll.Options {
		voters += len(option.Voters)
	}
	// The vote commits immediately after the first result row is scanned. A
	// single statement must keep reading its revision-0 snapshot; the former
	// three-SELECT loader instead returned revision 0 plus the revision-1 voter.
	if poll.Revision != 0 || voters != 0 {
		t.Fatalf("mixed poll projection: revision=%d voters=%d poll=%+v", poll.Revision, voters, poll)
	}
}

func selectedOption(poll *Poll, actor ParticipantRef) string {
	if poll == nil {
		return ""
	}
	for _, option := range poll.Options {
		for _, voter := range option.Voters {
			if voter == actor {
				return option.OptionID
			}
		}
	}
	return ""
}

func TestPollVotingRejectsWrongScopeForeignHiddenAndDeletedTargets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	first := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-scope-first", PollInput{
		Question: "first", Options: []string{"A", "B"},
	})
	second := appendTestPoll(t, ctx, a, channel.PlaceID, "poll-scope-second", PollInput{
		Question: "second", Options: []string{"C", "D"},
	})
	if _, err := b.VotePoll(ctx, channel.PlaceID, first.MessageID,
		[]string{second.Poll.Options[0].OptionID}); !errors.Is(err, ErrPollOptionNotFound) {
		t.Fatalf("foreign option = %v", err)
	}
	firstOption := first.Poll.Options[0].OptionID
	if _, err := b.VotePoll(ctx, channel.PlaceID, first.MessageID,
		[]string{firstOption, firstOption}); !errors.Is(err, ErrInvalidPoll) {
		t.Fatalf("duplicate option = %v", err)
	}
	if _, err := b.VotePoll(ctx, channel.PlaceID, first.MessageID,
		[]string{"not-a-uuid"}); !errors.Is(err, ErrPollOptionNotFound) {
		t.Fatalf("invalid option id = %v", err)
	}
	otherPlace, err := a.CreateChannel(ctx, "other-polls", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.VotePoll(ctx, otherPlace.PlaceID, first.MessageID,
		[]string{firstOption}); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("wrong place = %v", err)
	}

	otherWorkspace, _ := w.workspaceWithChannel(t, ctx)
	wrongScope := w.store.mustScope(t, ctx, otherWorkspace.WorkspaceID, w.humanB)
	if _, err := wrongScope.VotePoll(ctx, channel.PlaceID, first.MessageID,
		[]string{firstOption}); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("wrong Workspace = %v", err)
	}

	group, err := a.CreateGroupDM(ctx, []ParticipantRef{w.humanB, w.agent})
	if err != nil {
		t.Fatal(err)
	}
	hiddenPoll := appendTestPoll(t, ctx, a, group.PlaceID, "poll-hidden", PollInput{
		Question: "hidden", Options: []string{"A", "B"},
	})
	if _, err := w.store.pool.Exec(ctx, `
		UPDATE place_members SET visible_from_seq = $1
		WHERE workspace_id = $2 AND place_id = $3
		  AND member_kind = $4 AND member_id = $5 AND left_at IS NULL`,
		hiddenPoll.Seq+1, workspace.WorkspaceID, group.PlaceID, w.humanB.Kind, w.humanB.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := b.VotePoll(ctx, group.PlaceID, hiddenPoll.MessageID,
		[]string{hiddenPoll.Poll.Options[0].OptionID}); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("hidden message = %v", err)
	}

	if _, err := a.DeleteMessage(ctx, channel.PlaceID, first.MessageID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.VotePoll(ctx, channel.PlaceID, first.MessageID,
		[]string{firstOption}); !errors.Is(err, ErrMessageDeleted) {
		t.Fatalf("deleted message = %v", err)
	}
	nonPoll := w.send(t, ctx, channel.PlaceID, w.humanA, "plain")
	if _, err := a.VotePoll(ctx, channel.PlaceID, nonPoll.MessageID, []string{}); !errors.Is(err, ErrPollNotFound) {
		t.Fatalf("non-poll message = %v", err)
	}
}

func TestEditReactionPreservePollAndTombstoneCascadesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)
	a := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)
	b := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB)
	message, created, err := a.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "before", ClientNonce: "poll-preservation",
		Poll: &PollInput{Question: "keep?", Options: []string{"A", "B"}},
	})
	if err != nil || !created {
		t.Fatalf("create = %t err=%v", created, err)
	}
	edited, err := a.EditMessage(ctx, channel.PlaceID, message.MessageID, "after", 1)
	if err != nil || edited.Poll == nil || edited.Poll.Question != "keep?" {
		t.Fatalf("edit lost poll: %+v err=%v", edited, err)
	}
	reacted, _, err := b.ToggleReactionIdempotent(
		ctx, channel.PlaceID, message.MessageID, "👍", "poll-reaction-preservation",
	)
	if err != nil || reacted.Poll == nil || reacted.Content != "after" || len(reacted.Reactions) != 1 {
		t.Fatalf("reaction lost poll/message state: %+v err=%v", reacted, err)
	}
	votedOptionID := reacted.Poll.Options[0].OptionID
	voted, err := b.VotePoll(ctx, channel.PlaceID, message.MessageID, []string{votedOptionID})
	if err != nil || voted.Poll == nil || voted.Poll.Revision != 1 ||
		len(voted.Poll.Options[0].Voters) != 1 {
		t.Fatalf("vote before tombstone = %+v err=%v", voted.Poll, err)
	}
	var voteCountBefore int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_poll_votes WHERE option_id = $1", votedOptionID,
	).Scan(&voteCountBefore); err != nil {
		t.Fatal(err)
	}
	if voteCountBefore != 1 {
		t.Fatalf("vote rows before tombstone = %d, want one", voteCountBefore)
	}
	tombstone, err := a.DeleteMessage(ctx, channel.PlaceID, message.MessageID)
	if err != nil || !tombstone.Deleted || tombstone.Poll != nil {
		t.Fatalf("tombstone = %+v err=%v", tombstone, err)
	}
	var pollCount, optionCount, voteCount int
	if err := w.store.pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM message_polls WHERE message_id = $1),
		  (SELECT count(*) FROM message_poll_options WHERE message_id = $1),
		  (SELECT count(*) FROM message_poll_votes WHERE option_id = $2)`,
		message.MessageID, votedOptionID).Scan(&pollCount, &optionCount, &voteCount); err != nil {
		t.Fatal(err)
	}
	if pollCount != 0 || optionCount != 0 || voteCount != 0 {
		t.Fatalf("tombstone projection counts poll=%d options=%d votes=%d", pollCount, optionCount, voteCount)
	}
	history, err := a.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, reloaded := range history {
		if reloaded.MessageID == message.MessageID && reloaded.Poll != nil {
			t.Fatalf("reloaded tombstone retained poll: %+v", reloaded)
		}
	}
}

func TestPollHTTPRejectsAttachmentCombinationAndPublishesPartialUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, server := newWSWorld(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)
	path := "/messaging/places/" + channel.PlaceID + "/messages"

	response, body := call(t, server, http.MethodPost, path, w.humanA.ID, map[string]any{
		"content": "", "client_nonce": "poll-attachment-rejected",
		"attachments": []string{newUUIDv7()},
		"poll":        map[string]any{"question": "invalid", "options": []string{"A", "B"}},
	})
	if response.StatusCode != http.StatusBadRequest || body["error"] != "invalid_poll" {
		t.Fatalf("poll+attachment = %d %v", response.StatusCode, body)
	}
	var lastSeq int64
	if err := w.store.pool.QueryRow(ctx, "SELECT last_seq FROM places WHERE place_id=$1", channel.PlaceID).Scan(&lastSeq); err != nil {
		t.Fatal(err)
	}
	if lastSeq != 0 {
		t.Fatalf("rejected poll+attachment allocated seq %d", lastSeq)
	}
	scoped := w.store.mustScopeForPlace(t, ctx, channel.PlaceID, w.humanA)
	if _, _, err := scoped.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, ClientNonce: "store-poll-attachment-rejected",
		AttachmentIDs: []string{newUUIDv7()},
		Poll:          &PollInput{Question: "invalid", Options: []string{"A", "B"}},
	}); !errors.Is(err, ErrInvalidPoll) {
		t.Fatalf("Store poll+attachment = %v", err)
	}

	closesAt := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	response, body = call(t, server, http.MethodPost, path, w.humanA.ID, map[string]any{
		"content": "before", "client_nonce": "http-poll",
		"poll": map[string]any{
			"question": "どちら？", "options": []string{"A", "B"},
			"allow_multi": false, "closes_at": closesAt,
		},
	})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create poll = %d %v", response.StatusCode, body)
	}
	messageID := body["message_id"].(string)
	messageHistory, err := scoped.History(ctx, channel.PlaceID, HistoryOptions{})
	if err != nil || len(messageHistory) != 1 || messageHistory[0].Poll == nil {
		t.Fatalf("history = %+v err=%v", messageHistory, err)
	}
	message := messageHistory[0]

	editPath := path + "/" + messageID
	response, body = call(t, server, http.MethodPatch, editPath, w.humanA.ID, map[string]any{
		"content": "kept content", "revision": 1,
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("edit = %d %v", response.StatusCode, body)
	}
	reactionPath := editPath + "/reactions"
	response, body = call(t, server, http.MethodPost, reactionPath, w.humanA.ID, map[string]any{
		"emoji": "👍", "client_nonce": "poll-event-reaction",
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("reaction = %d %v", response.StatusCode, body)
	}

	conn := dialWS(t, server, w.humanB.ID, nil)
	votePath := editPath + "/poll/vote"
	response, body = call(t, server, http.MethodPost, votePath, w.humanB.ID, map[string]any{
		"option_ids": []string{message.Poll.Options[0].OptionID},
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("vote = %d %v", response.StatusCode, body)
	}
	confirmed := body["message"].(map[string]any)
	if confirmed["content"] != "kept content" || len(confirmed["reactions"].([]any)) != 1 {
		t.Fatalf("vote response lost independent message fields: %v", confirmed)
	}

	frame := readFrame(t, conn)
	event := frame["event"].(map[string]any)
	if event["type"] != EventPollUpdated || event["place_id"] != channel.PlaceID {
		t.Fatalf("poll event = %v", event)
	}
	if _, fullMessage := event["message"]; fullMessage {
		t.Fatalf("poll event carried full Message: %v", event)
	}
	update := event["poll"].(map[string]any)
	if len(update) != 2 || update["message_id"] != messageID {
		t.Fatalf("poll update envelope = %v", update)
	}
	for _, forbidden := range []string{"content", "mentions", "reactions", "attachments", "edited_at"} {
		if _, exists := update[forbidden]; exists {
			t.Fatalf("poll update can overwrite %s: %v", forbidden, update)
		}
	}
	poll := update["poll"].(map[string]any)
	if poll["revision"] != float64(1) {
		t.Fatalf("poll update revision = %v", poll)
	}

	response, body = call(t, server, http.MethodPost, votePath, w.humanB.ID, map[string]any{})
	if response.StatusCode != http.StatusBadRequest || body["error"] != "invalid_poll" {
		t.Fatalf("missing option_ids = %d %v", response.StatusCode, body)
	}
}

func TestLocalPollRelativeDeadlineReplayAndVoteUseExactScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, channel := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}
	request := map[string]any{
		"place_id": channel.PlaceID, "question": "いつ？", "options": []string{"今日", "明日"},
		"client_nonce": "relative-deadline-replay", "closes_in_minutes": 30,
	}
	status, first := callLocal(t, ctx, server.localCreatePoll, LocalCreatePollPath, request, authorization)
	if status != http.StatusCreated || first["created"] != true {
		t.Fatalf("first local poll = %d %v", status, first)
	}
	firstMessage := first["message"].(map[string]any)
	messageID := firstMessage["message_id"].(string)
	if _, err := w.store.pool.Exec(ctx, `
		UPDATE message_polls SET closes_at = clock_timestamp() - interval '1 second'
		WHERE message_id = $1`, messageID); err != nil {
		t.Fatal(err)
	}
	status, replay := callLocal(t, ctx, server.localCreatePoll, LocalCreatePollPath, request, authorization)
	if status != http.StatusOK || replay["created"] != false ||
		replay["message"].(map[string]any)["message_id"] != messageID {
		t.Fatalf("closed local replay = %d %v", status, replay)
	}
	changed := map[string]any{
		"place_id": channel.PlaceID, "question": "いつ？", "options": []string{"今日", "明日"},
		"client_nonce": "relative-deadline-replay", "closes_in_minutes": 31,
	}
	status, conflict := callLocal(t, ctx, server.localCreatePoll, LocalCreatePollPath, changed, authorization)
	if status != http.StatusConflict || conflict["error"] != "idempotency_conflict" {
		t.Fatalf("changed relative replay = %d %v", status, conflict)
	}

	// Re-open the poll only to exercise local voting without a wall-clock race.
	if _, err := w.store.pool.Exec(ctx, `
		UPDATE message_polls SET closes_at = clock_timestamp() + interval '1 hour'
		WHERE message_id = $1`, messageID); err != nil {
		t.Fatal(err)
	}
	options := replay["message"].(map[string]any)["poll"].(map[string]any)["options"].([]any)
	optionID := options[0].(map[string]any)["option_id"].(string)
	status, voted := callLocal(t, ctx, server.localVotePoll, LocalVotePollPath, map[string]any{
		"place_id": channel.PlaceID, "message_id": messageID, "option_ids": []string{optionID},
	}, authorization)
	if status != http.StatusOK || voted["message"].(map[string]any)["poll"].(map[string]any)["revision"] != float64(1) {
		t.Fatalf("local vote = %d %v", status, voted)
	}
}

func TestPollUpdateEventJSONNeverContainsFullMessage(t *testing.T) {
	poll := &Poll{
		Question: "partial", Revision: 4,
		Options: []PollOption{{OptionID: "01900000-0000-7000-8000-000000000099", Text: "A"}},
	}
	message := Message{
		MessageID: "01900000-0000-7000-8000-000000000098",
		Content:   "must not travel", Reactions: []ReactionSummary{{Emoji: "👍"}}, Poll: poll,
	}
	update := pollUpdateToWire(message)
	raw, err := json.Marshal(Event{
		Type: EventPollUpdated, PlaceID: "01900000-0000-7000-8000-000000000097", Poll: &update,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("must not travel")) || bytes.Contains(raw, []byte(`"message"`)) ||
		bytes.Contains(raw, []byte(`"reactions"`)) {
		t.Fatalf("partial event leaked full Message fields: %s", raw)
	}
}
