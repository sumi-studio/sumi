package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

func ptr(value string) *string { return &value }

func profileOf(t *testing.T, profiles []MemberProfile, who ParticipantRef) MemberProfile {
	t.Helper()
	for _, profile := range profiles {
		if profile.Participant == who {
			return profile
		}
	}
	t.Fatalf("participant %s not present in %+v", who.Key(), profiles)
	return MemberProfile{}
}

func readPublishedProfile(t *testing.T, sub *subscriber) memberWire {
	t.Helper()
	select {
	case raw := <-sub.send:
		var frame struct {
			Event Event `json:"event"`
		}
		if err := json.Unmarshal(raw.payload, &frame); err != nil {
			t.Fatalf("decode profile event: %v", err)
		}
		if frame.Event.Type != EventProfileUpdated || frame.Event.Profile == nil {
			t.Fatalf("profile event = %+v", frame.Event)
		}
		return *frame.Event.Profile
	case <-time.After(5 * time.Second):
		t.Fatal("profile event was not published")
		return memberWire{}
	}
}

// The tagline belongs to the Participant, so it rides with every member list
// the participant appears in rather than being re-declared per place.
func TestTaglineIsCarriedByEveryMemberListOfTheParticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, channel := w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetProfile(ctx, w.humanA, nil, ptr("開発")); err != nil {
		t.Fatalf("set profile: %v", err)
	}

	members, err := w.store.WorkspaceMemberProfiles(ctx, workspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("workspace members: %v", err)
	}
	if got := profileOf(t, members, w.humanA).Tagline; got != "開発" {
		t.Fatalf("workspace member tagline = %q", got)
	}
	place, err := w.store.ActiveMembers(ctx, channel.PlaceID, w.humanB)
	if err != nil {
		t.Fatalf("place members: %v", err)
	}
	if got := profileOf(t, place, w.humanA).Tagline; got != "開発" {
		t.Fatalf("place member tagline = %q", got)
	}
	if got := profileOf(t, place, w.humanB).Tagline; got != "" {
		t.Fatalf("participant who declared nothing has tagline %q", got)
	}

	// Replacing the tagline replaces the one row rather than accumulating.
	if _, err := w.store.SetProfile(ctx, w.humanA, nil, ptr("秘書")); err != nil {
		t.Fatalf("replace tagline: %v", err)
	}
	var rows int
	if err := w.store.pool.QueryRow(ctx,
		"SELECT count(*) FROM participant_profiles WHERE member_kind=$1 AND member_id=$2",
		w.humanA.Kind, w.humanA.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("participant_profiles rows = %d, want exactly one per participant", rows)
	}
	own, err := w.store.Profile(ctx, w.humanA)
	if err != nil {
		t.Fatalf("read own profile: %v", err)
	}
	if own.Tagline != "秘書" || own.DisplayName != "Yohaku" {
		t.Fatalf("own profile = %+v", own)
	}
}

// A caller who names one field must not silently clear the other: the REST
// surface sends every field, the settings screen may send one.
func TestSetProfilePreservesTheFieldsTheCallerDidNotName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	if _, err := w.store.SetProfile(ctx, w.humanA, ptr("余白"), ptr("開発")); err != nil {
		t.Fatalf("set profile: %v", err)
	}
	profile, err := w.store.SetProfile(ctx, w.humanA, nil, ptr("秘書"))
	if err != nil {
		t.Fatalf("patch tagline: %v", err)
	}
	if profile.DisplayName != "余白" || profile.Tagline != "秘書" {
		t.Fatalf("after tagline-only patch = %+v", profile)
	}
	profile, err = w.store.SetProfile(ctx, w.humanA, ptr("Yohaku"), nil)
	if err != nil {
		t.Fatalf("patch display name: %v", err)
	}
	if profile.DisplayName != "Yohaku" || profile.Tagline != "秘書" {
		t.Fatalf("after name-only patch = %+v", profile)
	}
}

// The 戸籍 stays the canonical registry of names: the profile route writes
// through to it rather than keeping a second copy that can disagree.
func TestSetProfileWritesTheDisplayNameBackToTheKoseki(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	// Surrounding whitespace is collapsed by the registry's one rule.
	if _, err := w.store.SetProfile(ctx, w.humanA, ptr("  余白   ハク  "), nil); err != nil {
		t.Fatalf("set display name: %v", err)
	}
	registry := koseki.New(w.store.pool)
	name, err := registry.HumanDisplayName(ctx, w.humanA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if name != "余白 ハク" {
		t.Fatalf("戸籍 display name = %q", name)
	}
	var customized bool
	if err := w.store.pool.QueryRow(ctx,
		"SELECT display_name_customized FROM humans WHERE human_id=$1", w.humanA.ID).Scan(&customized); err != nil {
		t.Fatal(err)
	}
	if !customized {
		// Without the bit, the next provider login would silently replace the
		// name the Human just chose.
		t.Fatal("a self-chosen name must set display_name_customized")
	}
}

func TestSetProfileRefusesUnusableNamesAndOverlongTaglines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	// Blank, whitespace-only, a stray control character, and an invisible
	// formatting character are all names nobody can be called by.
	for _, name := range []string{"", "   ", "a\u0007b", "\u200e"} {
		if _, err := w.store.SetProfile(ctx, w.humanA, ptr(name), nil); !errors.Is(err, ErrInvalidDisplayName) {
			t.Fatalf("display name %q: got %v, want ErrInvalidDisplayName", name, err)
		}
	}
	for _, tagline := range []string{strings.Repeat("あ", MaxTaglineChars+1), "a\nb", "a\n"} {
		if _, err := w.store.SetProfile(ctx, w.humanA, nil, ptr(tagline)); !errors.Is(err, ErrInvalidTagline) {
			t.Fatalf("invalid tagline %q: got %v, want ErrInvalidTagline", tagline, err)
		}
	}
	// A refused write leaves the canonical name untouched.
	profile, err := w.store.Profile(ctx, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if profile.DisplayName != "Yohaku" || profile.Tagline != "" {
		t.Fatalf("profile after refusals = %+v", profile)
	}
	// Exactly at the bound is allowed.
	if _, err := w.store.SetProfile(ctx, w.humanA, nil,
		ptr(strings.Repeat("あ", MaxTaglineChars))); err != nil {
		t.Fatalf("tagline of exactly %d runes: %v", MaxTaglineChars, err)
	}
	profile, err = w.store.SetProfile(ctx, w.humanA, nil, ptr("  開発  "))
	if err != nil || profile.Tagline != "開発" {
		t.Fatalf("trimmed tagline = %+v, err %v", profile, err)
	}
}

// AX: a PersonalityAgent names itself through the same store path a Human uses,
// so neither side holds a capability the other lacks.
func TestPersonalityAgentNamesItselfThroughTheSamePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)

	profile, err := w.store.SetProfile(ctx, w.agent, ptr("クロ"), ptr("調べもの"))
	if err != nil {
		t.Fatalf("agent set profile: %v", err)
	}
	if profile.DisplayName != "クロ" || profile.Tagline != "調べもの" {
		t.Fatalf("agent profile = %+v", profile)
	}
	var stored string
	if err := w.store.pool.QueryRow(ctx,
		"SELECT display_name FROM agents WHERE personality_agent_id=$1", w.agent.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "クロ" {
		t.Fatalf("戸籍 agent display name = %q", stored)
	}
	members, err := w.store.WorkspaceMemberProfiles(ctx, workspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if got := profileOf(t, members, w.agent); got.DisplayName != "クロ" || got.Tagline != "調べもの" {
		t.Fatalf("agent seen by a Human = %+v", got)
	}
	// The same refusals apply: the agent lane is not a looser lane.
	if _, err := w.store.SetProfile(ctx, w.agent, ptr("a\u0007b"), nil); !errors.Is(err, ErrInvalidDisplayName) {
		t.Fatalf("agent control character: got %v, want ErrInvalidDisplayName", err)
	}
}

// The profile is the actor's own. There is no request field naming a subject,
// so the HTTP surface cannot be pointed at anyone else.
func TestProfileOverHTTPIsSelfDeclaredAndPublishedToWhoCanSeeIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newWSWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)

	conn := dialWS(t, ts, w.humanB.ID, nil)
	resp, body := call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID,
		map[string]any{"display_name": "余白", "tagline": "開発"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set profile: status %d body %v", resp.StatusCode, body)
	}
	if body["display_name"] != "余白" || body["tagline"] != "開発" {
		t.Fatalf("profile body = %v", body)
	}

	// profile_updated carries no place: it is scoped to the participant, and the
	// subscriber receives it because they can see that participant.
	frame := readFrame(t, conn)
	event := frame["event"].(map[string]any)
	if event["type"] != EventProfileUpdated {
		t.Fatalf("event = %v", event)
	}
	if _, hasPlace := event["place_id"]; hasPlace {
		t.Fatalf("participant-scoped event carried a place: %v", event)
	}
	profile := event["profile"].(map[string]any)
	if profile["display_name"] != "余白" || profile["tagline"] != "開発" {
		t.Fatalf("event profile = %v", profile)
	}

	// Bootstrap is where a fresh client learns it; the tagline rides on the
	// member list rather than needing a second round trip.
	resp, body = call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d body %v", resp.StatusCode, body)
	}
	found := false
	for _, entry := range body["members"].([]any) {
		member := entry.(map[string]any)
		participant := member["participant"].(map[string]any)
		if participant["human_id"] == w.humanA.ID {
			found = true
			if member["display_name"] != "余白" || member["tagline"] != "開発" {
				t.Fatalf("bootstrap member = %v", member)
			}
		}
	}
	if !found {
		t.Fatalf("bootstrap did not list the renamed participant: %v", body["members"])
	}

	// An overlong tagline is refused before anything is written.
	resp, body = call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID,
		map[string]any{"tagline": strings.Repeat("あ", MaxTaglineChars+1)})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_tagline" {
		t.Fatalf("overlong tagline: status %d body %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID,
		map[string]any{"tagline": "a\nb"})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_tagline" {
		t.Fatalf("multiline tagline: status %d body %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID,
		map[string]any{"display_name": "  "})
	if resp.StatusCode != http.StatusBadRequest || body["error"] != "invalid_display_name" {
		t.Fatalf("blank display name: status %d body %v", resp.StatusCode, body)
	}
	resp, body = call(t, ts, http.MethodGet, "/messaging/profile", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK || body["display_name"] != "余白" || body["tagline"] != "開発" {
		t.Fatalf("profile after refusals: status %d body %v", resp.StatusCode, body)
	}
}

// The PersonalityAgent's 名乗り travels its own transport, but it meets the
// Human's contract: the same store method, the same wire, the same refusals,
// and the same participant-scoped event to everyone who can see it. Without a
// route on this lane the parity SetProfile claims would be a comment only.
func TestLocalProfileGivesTheAgentTheSameSelfDeclarationAsAHuman(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	server.Hub = NewHub(w.store.core)
	watcher := server.Hub.subscribe(w.store.mustScopeForActor(t, ctx, w.humanB))
	t.Cleanup(func() { server.Hub.unsubscribe(watcher) })
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	// Naming no field is a read, the same shape the notification-setting lane
	// already gives the agent.
	status, body := callLocal(t, ctx, server.localProfile, LocalProfilePath,
		map[string]any{}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent profile read: status %d body %v", status, body)
	}
	if got := body["profile"].(map[string]any); got["display_name"] != "Kuro" || got["tagline"] != "" {
		t.Fatalf("agent profile read = %v", got)
	}

	status, body = callLocal(t, ctx, server.localProfile, LocalProfilePath, map[string]any{
		"display_name": "クロ", "tagline": "調べもの",
	}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent names itself: status %d body %v", status, body)
	}
	declared := body["profile"].(map[string]any)
	participant := declared["participant"].(map[string]any)
	if participant["kind"] != "personality_agent" || participant["personality_agent_id"] != w.agent.ID {
		t.Fatalf("agent profile participant = %v", participant)
	}
	if declared["display_name"] != "クロ" || declared["tagline"] != "調べもの" {
		t.Fatalf("agent profile = %v", declared)
	}

	// The same participant-scoped event the Human lane publishes, carrying no
	// place, reaches a Human who can see the agent.
	select {
	case raw := <-watcher.send:
		var frame struct {
			Event Event `json:"event"`
		}
		if err := json.Unmarshal(raw.payload, &frame); err != nil {
			t.Fatalf("decode agent profile event: %v", err)
		}
		if frame.Event.Type != EventProfileUpdated || frame.Event.PlaceID != "" {
			t.Fatalf("agent profile event = %+v", frame.Event)
		}
		if frame.Event.Profile == nil || frame.Event.Profile.DisplayName != "クロ" ||
			frame.Event.Profile.Tagline != "調べもの" {
			t.Fatalf("agent profile event payload = %+v", frame.Event.Profile)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the agent's 名乗り never reached the Human who can see it")
	}

	// Naming one field preserves the other, exactly as the REST PUT does.
	status, body = callLocal(t, ctx, server.localProfile, LocalProfilePath,
		map[string]any{"tagline": "散歩中"}, authorization)
	if status != http.StatusOK {
		t.Fatalf("agent tagline only: status %d body %v", status, body)
	}
	if got := body["profile"].(map[string]any); got["display_name"] != "クロ" || got["tagline"] != "散歩中" {
		t.Fatalf("agent tagline only = %v", got)
	}

	// One validation, so this lane is not the looser one.
	status, body = callLocal(t, ctx, server.localProfile, LocalProfilePath,
		map[string]any{"tagline": strings.Repeat("あ", MaxTaglineChars+1)}, authorization)
	if status != http.StatusBadRequest || body["error"] != "invalid_tagline" {
		t.Fatalf("agent overlong tagline: status %d body %v", status, body)
	}
	status, body = callLocal(t, ctx, server.localProfile, LocalProfilePath,
		map[string]any{"tagline": "a\nb"}, authorization)
	if status != http.StatusBadRequest || body["error"] != "invalid_tagline" {
		t.Fatalf("agent multiline tagline: status %d body %v", status, body)
	}
	status, body = callLocal(t, ctx, server.localProfile, LocalProfilePath,
		map[string]any{"display_name": "  "}, authorization)
	if status != http.StatusBadRequest || body["error"] != "invalid_display_name" {
		t.Fatalf("agent blank display name: status %d body %v", status, body)
	}

	// Self-declaration only: there is no field for naming anybody else, so the
	// attempt is refused by the wire itself rather than by a permission check.
	status, body = callLocal(t, ctx, server.localProfile, LocalProfilePath, map[string]any{
		"participant": map[string]any{"kind": "human", "human_id": w.humanB.ID},
		"tagline":     "他人の名乗り",
	}, authorization)
	if status != http.StatusBadRequest {
		t.Fatalf("naming another participant: status %d body %v", status, body)
	}

	// Nothing the refusals touched was written, and a Human reads the agent's
	// 名乗り from the ordinary member list rather than from a second route.
	members, err := w.store.WorkspaceMemberProfiles(ctx, workspace.WorkspaceID, w.humanB)
	if err != nil {
		t.Fatal(err)
	}
	if got := profileOf(t, members, w.agent); got.DisplayName != "クロ" || got.Tagline != "散歩中" {
		t.Fatalf("agent seen by a Human = %+v", got)
	}
	if got := profileOf(t, members, w.humanB); got.Tagline != "" {
		t.Fatalf("the agent's request altered another participant: %+v", got)
	}
}

func TestProfileUpdateFansOutToEveryWorkspaceAudience(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspaceA, _ := w.workspaceWithChannel(t, ctx)
	workspaceB, err := w.store.CreateWorkspace(ctx, "sumi-ops", w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.store.AddWorkspaceMember(ctx, workspaceB.WorkspaceID, w.humanB, RoleMember); err != nil {
		t.Fatal(err)
	}

	server := NewServer(w.store.core, nil)
	server.Hub = NewHub(w.store.core)
	watchA := server.Hub.subscribe(w.store.mustScope(t, ctx, workspaceA.WorkspaceID, w.humanB))
	watchB := server.Hub.subscribe(w.store.mustScope(t, ctx, workspaceB.WorkspaceID, w.humanB))
	t.Cleanup(func() {
		server.Hub.unsubscribe(watchA)
		server.Hub.unsubscribe(watchB)
	})

	actor := w.store.mustScope(t, ctx, workspaceA.WorkspaceID, w.humanA)
	if _, err := server.setProfile(ctx, actor, nil, ptr("両方に届く")); err != nil {
		t.Fatalf("set profile: %v", err)
	}
	for label, watcher := range map[string]*subscriber{"workspace A": watchA, "workspace B": watchB} {
		profile := readPublishedProfile(t, watcher)
		if profile.Participant.HumanID != w.humanA.ID || profile.Tagline != "両方に届く" {
			t.Fatalf("%s profile = %+v", label, profile)
		}
	}
}

func TestConcurrentProfileUpdatesPublishInDatabaseOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	workspace, _ := w.workspaceWithChannel(t, ctx)
	server := NewServer(w.store.core, nil)
	server.Hub = NewHub(w.store.core)
	watcher := server.Hub.subscribe(w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanB))
	t.Cleanup(func() { server.Hub.unsubscribe(watcher) })
	actor := w.store.mustScope(t, ctx, workspace.WorkspaceID, w.humanA)

	firstEntered := make(chan struct{})
	allowFirst := make(chan struct{})
	published := make(chan string, 2)
	publisher := func(ctx context.Context, scopes []Scope, profile MemberProfile) {
		if profile.Tagline == "古い" {
			close(firstEntered)
			<-allowFirst
		}
		published <- profile.Tagline
		server.publishProfile(ctx, scopes, profile)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := actor.SetProfile(ctx, nil, ptr("古い"), publisher)
		firstDone <- err
	}()
	<-firstEntered

	secondDone := make(chan error, 1)
	go func() {
		_, err := actor.SetProfile(ctx, nil, ptr("新しい"), publisher)
		secondDone <- err
	}()
	select {
	case tagline := <-published:
		t.Fatalf("later write published %q before the earlier write released its row lock", tagline)
	case <-time.After(150 * time.Millisecond):
	}
	close(allowFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first update: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second update: %v", err)
	}

	if got := <-published; got != "古い" {
		t.Fatalf("first published profile = %q", got)
	}
	if got := <-published; got != "新しい" {
		t.Fatalf("second published profile = %q", got)
	}
	if got := readPublishedProfile(t, watcher).Tagline; got != "古い" {
		t.Fatalf("first live profile = %q", got)
	}
	if got := readPublishedProfile(t, watcher).Tagline; got != "新しい" {
		t.Fatalf("second live profile = %q", got)
	}
	profile, err := actor.Profile(ctx)
	if err != nil || profile.Tagline != "新しい" {
		t.Fatalf("durable profile = %+v, err %v", profile, err)
	}
}
