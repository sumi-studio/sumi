package messaging

import (
	"context"
	"testing"
	"time"
)

func TestPollAttentionRecipientAndDeliveryGuards(t *testing.T) {
	for _, scenario := range []string{"self", "human-author", "mute", "left-before-vote", "deleted", "poll-removed", "left-after-vote", "disabled", "deadline", "thread", "thread-left"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w := newWorld(t, ctx)
			ws, ch := w.workspaceWithChannel(t, ctx)
			pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
			voter := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
			if scenario == "thread" || scenario == "thread-left" {
				thread, _, err := pa.CreateThread(ctx, ch.PlaceID, "Poll thread", "", "poll-thread")
				if err != nil {
					t.Fatal(err)
				}
				ch = thread.Place
			}
			author := pa
			if scenario == "human-author" {
				author = voter
				// Isolate vote attention from the original channel message.
				if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMentions, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			q := appendTestPoll(t, ctx, author, ch.PlaceID, "guard", PollInput{Question: "Choose", Options: []string{"A", "B"}})
			if scenario == "thread-left" {
				if _, err := w.store.pool.Exec(ctx, `UPDATE place_members SET left_at=now() WHERE place_id=$1 AND member_id=$2`, ch.PlaceID, w.agent.ID); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "self" {
				voter = pa
			}
			if scenario == "mute" {
				if _, err := pa.SetNotificationSetting(ctx, NotifyLevelMute, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "left-before-vote" {
				if err := w.workspaces.Leave(ctx, ws.WorkspaceID, w.agent); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := voter.VotePoll(ctx, ch.PlaceID, q.MessageID, []string{q.Poll.Options[0].OptionID}); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "deleted":
				if _, err := pa.DeleteMessage(ctx, ch.PlaceID, q.MessageID); err != nil {
					t.Fatal(err)
				}
			case "poll-removed":
				if _, err := w.store.pool.Exec(ctx, `DELETE FROM message_polls WHERE message_id=$1`, q.MessageID); err != nil {
					t.Fatal(err)
				}
			case "left-after-vote":
				if err := w.workspaces.Leave(ctx, ws.WorkspaceID, w.agent); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := w.apps.SetEnabledByID(ctx, pa.Scope.InstallationID, w.humanA, false); err != nil {
					t.Fatal(err)
				}
			case "deadline":
				if _, err := w.store.pool.Exec(ctx, `UPDATE message_polls SET closes_at=now()-interval '1 second' WHERE message_id=$1`, q.MessageID); err != nil {
					t.Fatal(err)
				}
			}
			d := newAttentionDelivery()
			stats, err := w.store.core.DeliverAgentAttention(ctx, d, 10)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "deadline" || scenario == "thread" {
				if stats.Admitted != 1 {
					t.Fatalf("valid answer lost after deadline: %+v", stats)
				}
			} else if len(d.events) != 0 || d.prepares != 0 {
				t.Fatalf("unexpected delivery: %+v", stats)
			}
		})
	}
}

func TestPollAttentionOutboxFailureRollsBackVote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	ws, ch := w.workspaceWithChannel(t, ctx)
	pa := w.store.mustScope(t, ctx, ws.WorkspaceID, w.agent)
	voter := w.store.mustScope(t, ctx, ws.WorkspaceID, w.humanB)
	q := appendTestPoll(t, ctx, pa, ch.PlaceID, "atomic", PollInput{Question: "Choose", Options: []string{"A", "B"}})
	if _, err := w.store.pool.Exec(ctx, `CREATE FUNCTION reject_poll_attention() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$;
 CREATE TRIGGER reject_poll_attention BEFORE INSERT ON agent_attention_deliveries FOR EACH ROW EXECUTE FUNCTION reject_poll_attention()`); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.VotePoll(ctx, ch.PlaceID, q.MessageID, []string{q.Poll.Options[0].OptionID}); err == nil {
		t.Fatal("expected outbox failure")
	}
	var revision, votes int
	if err := w.store.pool.QueryRow(ctx, `SELECT revision, (SELECT count(*) FROM message_poll_votes v JOIN message_poll_options o USING(option_id) WHERE o.message_id=p.message_id) FROM message_polls p WHERE message_id=$1`, q.MessageID).Scan(&revision, &votes); err != nil {
		t.Fatal(err)
	}
	if revision != 0 || votes != 0 {
		t.Fatalf("partial vote commit: revision=%d votes=%d", revision, votes)
	}
}
