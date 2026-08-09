package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

type profileResult struct {
	profile MemberProfile
	err     error
}

// holdProfileTestGate owns one transaction-scoped advisory lock until the
// returned function is called. Tests use a database trigger that waits on the
// same key to stop a profile write at an exact statement boundary.
func holdProfileTestGate(t *testing.T, ctx context.Context, w world, key int32) func() {
	t.Helper()
	conn, err := w.store.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire advisory gate connection: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		t.Fatalf("begin advisory gate transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		t.Fatalf("hold advisory gate: %v", err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = tx.Rollback(context.Background())
		}
		conn.Release()
	})
	return func() {
		t.Helper()
		if released {
			return
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("release advisory gate: %v", err)
		}
		released = true
	}
}

func waitForProfileTestGate(t *testing.T, ctx context.Context, w world, key int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		err := w.store.pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted
			  AND classid = 0 AND objid = $1
		)`, key).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect advisory gate: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("profile write did not reach the advisory gate")
}

func TestProfileIsSelfDeclaredAndReachesEveryMemberList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit human A: %v", err)
	}
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanB); err != nil {
		t.Fatalf("admit human B: %v", err)
	}

	profile, err := w.store.SetProfile(ctx, w.humanA, "余白", "創業・デザイン", "", "")
	if err != nil {
		t.Fatalf("set profile: %v", err)
	}
	if profile.DisplayName != "余白" || profile.Tagline != "創業・デザイン" {
		t.Fatalf("profile = %#v", profile)
	}

	// The agent renames itself through the identical path a Human uses.
	if _, err := w.store.SetProfile(ctx, w.agent, "墨", "秘書", "", ""); err != nil {
		t.Fatalf("agent sets own profile: %v", err)
	}

	members, err := w.store.WorkspaceMemberProfiles(ctx, DefaultWorkspaceID, w.humanB)
	if err != nil {
		t.Fatalf("workspace members: %v", err)
	}
	seen := map[string]MemberProfile{}
	for _, member := range members {
		seen[member.Participant.Key()] = member
	}
	if got := seen[w.humanA.Key()]; got.DisplayName != "余白" || got.Tagline != "創業・デザイン" {
		t.Fatalf("human A on B's member list = %#v", got)
	}
	// The Secretary qualifier is presentation only: the registry keeps 墨.
	if got := seen[w.agent.Key()]; got.DisplayName != "墨" ||
		got.ProjectedDisplayName() != "墨（余白）" || got.Tagline != "秘書" {
		t.Fatalf("agent on B's member list = %#v", got)
	}
}

func TestProfileRejectsUnusableNamesAndTaglines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	if _, err := w.store.SetProfile(ctx, w.humanA, "   ", "", "", ""); !errors.Is(err, ErrInvalidDisplayName) {
		t.Fatalf("blank display name error = %v, want ErrInvalidDisplayName", err)
	}
	if _, err := w.store.SetProfile(ctx, w.humanA, "余白", strings.Repeat("あ", MaxTaglineChars+1), "", ""); !errors.Is(err, ErrInvalidTagline) {
		t.Fatalf("long tagline error = %v, want ErrInvalidTagline", err)
	}
	// A rejected change leaves the previous name standing.
	profile, err := w.store.MemberProfileFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("read profile: %v", err)
	}
	if profile.DisplayName != "Yohaku" {
		t.Fatalf("display name after rejected updates = %q", profile.DisplayName)
	}
}

func TestSetProfileRollsBackDisplayNameWhenTheProfileWriteFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)

	if _, err := w.store.pool.Exec(ctx, `
		CREATE FUNCTION reject_test_profile_write() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.tagline = 'reject-after-name' THEN
				RAISE EXCEPTION 'injected participant_profiles failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_test_profile_write
		BEFORE INSERT OR UPDATE ON participant_profiles
		FOR EACH ROW EXECUTE FUNCTION reject_test_profile_write()`); err != nil {
		t.Fatalf("install profile failure trigger: %v", err)
	}

	if _, err := w.store.SetProfile(ctx, w.humanA, "Changed", "reject-after-name", "", ""); err == nil {
		t.Fatal("SetProfile succeeded despite the injected profile write failure")
	}
	profile, err := w.store.MemberProfileFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("read profile after rollback: %v", err)
	}
	if profile.DisplayName != "Yohaku" || profile.Tagline != "" {
		t.Fatalf("profile after failed replacement = %#v, want the original profile", profile)
	}
}

func TestSetProfileSerializesConcurrentWholeProfileReplacements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	const gateKey int32 = 22901

	if _, err := w.store.pool.Exec(ctx, `
		CREATE FUNCTION gate_first_test_profile() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.tagline = 'tag-a' THEN
				PERFORM pg_advisory_xact_lock(22901);
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER gate_first_test_profile
		BEFORE INSERT OR UPDATE ON participant_profiles
		FOR EACH ROW EXECUTE FUNCTION gate_first_test_profile()`); err != nil {
		t.Fatalf("install profile gate trigger: %v", err)
	}
	release := holdProfileTestGate(t, ctx, w, gateKey)

	first := make(chan profileResult, 1)
	go func() {
		profile, err := w.store.SetProfile(ctx, w.humanA, "Name A", "tag-a", "", "")
		first <- profileResult{profile: profile, err: err}
	}()
	waitForProfileTestGate(t, ctx, w, gateKey)

	second := make(chan profileResult, 1)
	go func() {
		profile, err := w.store.SetProfile(ctx, w.humanA, "Name B", "tag-b", "", "")
		second <- profileResult{profile: profile, err: err}
	}()

	// Wait until B either completes (the old split-autocommit behavior) or is
	// blocked behind A's participant row lock (the serialized transaction).
	var secondBeforeRelease *profileResult
	deadline := time.Now().Add(5 * time.Second)
	for secondBeforeRelease == nil && time.Now().Before(deadline) {
		select {
		case result := <-second:
			secondBeforeRelease = &result
		default:
			var waitingOnRow bool
			err := w.store.pool.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_locks l
				JOIN pg_stat_activity a ON a.pid = l.pid
				WHERE a.datname = current_database() AND NOT l.granted
				  AND l.locktype IN ('transactionid', 'tuple')
			)`).Scan(&waitingOnRow)
			if err != nil {
				t.Fatalf("inspect participant row waiter: %v", err)
			}
			if waitingOnRow {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if secondBeforeRelease == nil {
			var waitingOnRow bool
			_ = w.store.pool.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
				WHERE a.datname = current_database() AND NOT l.granted
				  AND l.locktype IN ('transactionid', 'tuple')
			)`).Scan(&waitingOnRow)
			if waitingOnRow {
				break
			}
		}
	}
	release()

	firstResult := <-first
	secondResult := profileResult{}
	if secondBeforeRelease != nil {
		secondResult = *secondBeforeRelease
	} else {
		secondResult = <-second
	}
	if firstResult.err != nil || secondResult.err != nil {
		t.Fatalf("concurrent replacements: first error %v, second error %v", firstResult.err, secondResult.err)
	}
	if firstResult.profile.DisplayName != "Name A" || firstResult.profile.Tagline != "tag-a" {
		t.Fatalf("first response mixed concurrent replacements: %#v", firstResult.profile)
	}
	if secondResult.profile.DisplayName != "Name B" || secondResult.profile.Tagline != "tag-b" {
		t.Fatalf("second response mixed concurrent replacements: %#v", secondResult.profile)
	}
	stored, err := w.store.MemberProfileFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("read final profile: %v", err)
	}
	if stored.DisplayName != "Name B" || stored.Tagline != "tag-b" {
		t.Fatalf("final profile = %#v, want the later whole replacement", stored)
	}
}

func TestProfileImageAndMessageBindingChooseOneWinner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit Human: %v", err)
	}
	attachmentID := NewAttachmentID()
	if _, err := w.store.CreateAttachment(ctx, attachmentID, w.humanA, "face.png", "image/png", 64); err != nil {
		t.Fatalf("create test attachment: %v", err)
	}
	const gateKey int32 = 22902
	if _, err := w.store.pool.Exec(ctx, `
		CREATE FUNCTION gate_test_profile_name() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.display_name = 'Racing name' THEN
				PERFORM pg_advisory_xact_lock(22902);
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER gate_test_profile_name
		BEFORE UPDATE ON humans
		FOR EACH ROW EXECUTE FUNCTION gate_test_profile_name()`); err != nil {
		t.Fatalf("install name gate trigger: %v", err)
	}
	release := holdProfileTestGate(t, ctx, w, gateKey)

	profileDone := make(chan profileResult, 1)
	go func() {
		profile, err := w.store.SetProfile(ctx, w.humanA, "Racing name", "", attachmentID, "")
		profileDone <- profileResult{profile: profile, err: err}
	}()
	waitForProfileTestGate(t, ctx, w, gateKey)

	if _, created, err := w.store.AppendMessage(ctx, AppendInput{
		PlaceID: DefaultGeneralChannelID, Author: w.humanA, Content: "the same image",
		ClientNonce: "profile-image-race", AttachmentIDs: []string{attachmentID},
	}); err != nil || !created {
		t.Fatalf("bind the competing message attachment: created=%v error=%v", created, err)
	}
	release()
	result := <-profileDone
	if !errors.Is(result.err, ErrInvalidProfileImage) {
		t.Fatalf("losing profile replacement error = %v, want ErrInvalidProfileImage", result.err)
	}
	profile, err := w.store.MemberProfileFor(ctx, w.humanA)
	if err != nil {
		t.Fatalf("read profile after attachment race: %v", err)
	}
	if profile.DisplayName != "Yohaku" || profile.AvatarAttachmentID != "" {
		t.Fatalf("losing profile replacement partially committed: %#v", profile)
	}
}

func TestCommittedProfileImageFencesAWaitingMessageBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	if err := w.store.EnsureDefaultWorkspaceMembership(ctx, w.humanA); err != nil {
		t.Fatalf("admit Human: %v", err)
	}
	attachmentID := NewAttachmentID()
	if _, err := w.store.CreateAttachment(ctx, attachmentID, w.humanA, "face.png", "image/png", 64); err != nil {
		t.Fatalf("create test attachment: %v", err)
	}
	const gateKey int32 = 22903
	if _, err := w.store.pool.Exec(ctx, `
		CREATE FUNCTION gate_test_profile_row() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.tagline = 'profile-wins' THEN
				PERFORM pg_advisory_xact_lock(22903);
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER gate_test_profile_row
		BEFORE INSERT OR UPDATE ON participant_profiles
		FOR EACH ROW EXECUTE FUNCTION gate_test_profile_row()`); err != nil {
		t.Fatalf("install profile row gate trigger: %v", err)
	}
	release := holdProfileTestGate(t, ctx, w, gateKey)

	profileDone := make(chan profileResult, 1)
	go func() {
		profile, err := w.store.SetProfile(ctx, w.humanA, "Profile winner", "profile-wins", attachmentID, "")
		profileDone <- profileResult{profile: profile, err: err}
	}()
	waitForProfileTestGate(t, ctx, w, gateKey)

	type messageResult struct {
		created bool
		err     error
	}
	messageDone := make(chan messageResult, 1)
	go func() {
		_, created, err := w.store.AppendMessage(ctx, AppendInput{
			PlaceID: DefaultGeneralChannelID, Author: w.humanA, Content: "the same image",
			ClientNonce: "profile-image-loses", AttachmentIDs: []string{attachmentID},
		})
		messageDone <- messageResult{created: created, err: err}
	}()

	select {
	case result := <-messageDone:
		t.Fatalf("message binding completed before the profile transaction: created=%v error=%v", result.created, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	profileResult := <-profileDone
	if profileResult.err != nil || profileResult.profile.AvatarAttachmentID != attachmentID {
		t.Fatalf("winning profile replacement = %#v error %v", profileResult.profile, profileResult.err)
	}
	receivedMessage := <-messageDone
	if !errors.Is(receivedMessage.err, ErrAttachmentNotFound) || receivedMessage.created {
		t.Fatalf("waiting message binding: created=%v error=%v, want ErrAttachmentNotFound", receivedMessage.created, receivedMessage.err)
	}
}

func TestHumanRenamePublishesItsDependentAgentProjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	for _, participant := range []ParticipantRef{w.humanA, w.humanB} {
		if err := w.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
			t.Fatalf("admit %s: %v", participant.Key(), err)
		}
	}
	hub := NewHub(w.store)
	subscriber := hub.subscribe(w.humanB)
	defer hub.unsubscribe(subscriber)
	server := NewServer(w.store, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	server.Hub = hub

	request := httptest.NewRequest(http.MethodPut, "/messaging/profile", strings.NewReader(`{
		"display_name":"New owner","tagline":"design",
		"avatar_attachment_id":"","banner_attachment_id":""
	}`)).WithContext(ctx)
	request.Header.Set("Origin", testOrigin)
	request.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
	response := httptest.NewRecorder()
	server.serveSetProfile(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("rename Human: status %d body %s", response.Code, response.Body.String())
	}

	seen := map[string]string{}
	for len(seen) < 2 {
		select {
		case raw := <-subscriber.send:
			var frame struct {
				Type  string `json:"type"`
				Event Event  `json:"event"`
			}
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("decode profile event: %v", err)
			}
			if frame.Type != "event" || frame.Event.Type != EventProfileUpdated || frame.Event.Member == nil {
				t.Fatalf("unexpected profile frame: %s", raw)
			}
			participant, err := frame.Event.Member.Participant.ref()
			if err != nil {
				t.Fatalf("profile event participant: %v", err)
			}
			seen[participant.Key()] = frame.Event.Member.DisplayName
		case <-time.After(time.Second):
			t.Fatalf("profile events = %#v, want Human and dependent PersonalityAgent", seen)
		}
	}
	if seen[w.humanA.Key()] != "New owner" || seen[w.agent.Key()] != "Kuro（New owner）" {
		t.Fatalf("profile events = %#v", seen)
	}
}

func TestProfileImageMustBeYourOwnUnsentImageAndBecomesVisibleToOthers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newAttachmentServer(t, ctx)
	for _, participant := range []ParticipantRef{w.humanA, w.humanB} {
		if err := w.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
			t.Fatalf("admit %s: %v", participant.Key(), err)
		}
	}

	resp, body := upload(t, ts, w.humanA.ID, "face.png", "image/png", pngBytes)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload avatar: status %d", resp.StatusCode)
	}
	avatarID, _ := body["attachment_id"].(string)
	resp, body = upload(t, ts, w.humanA.ID, "notes.txt", "text/plain", []byte("plain text, not a face"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload document: status %d", resp.StatusCode)
	}
	documentID, _ := body["attachment_id"].(string)
	resp, body = upload(t, ts, w.humanB.ID, "other.png", "image/png", pngBytes)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload as B: status %d", resp.StatusCode)
	}
	othersID, _ := body["attachment_id"].(string)

	for name, id := range map[string]string{
		"a document":            documentID,
		"someone else's upload": othersID,
		"a made-up id":          "not-a-uuid",
	} {
		if _, err := w.store.SetProfile(ctx, w.humanA, "Yohaku", "", id, ""); !errors.Is(err, ErrInvalidProfileImage) {
			t.Fatalf("avatar from %s: error = %v, want ErrInvalidProfileImage", name, err)
		}
	}

	if _, err := w.store.SetProfile(ctx, w.humanA, "Yohaku", "", avatarID, ""); err != nil {
		t.Fatalf("set avatar: %v", err)
	}
	// A face on the member list is meant to be seen by everyone who can see
	// the person, even though the upload was never sent as a message.
	if _, err := w.store.AttachmentForViewer(ctx, avatarID, w.humanB); err != nil {
		t.Fatalf("B reads A's avatar: %v", err)
	}
	// An ordinary unbound upload stays private to its uploader.
	if _, err := w.store.AttachmentForViewer(ctx, documentID, w.humanB); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("B reads A's unsent document: error = %v, want ErrAttachmentNotFound", err)
	}

	// The avatar cannot also become a message attachment: deleting that
	// message would otherwise blank the face on every member list.
	_, _, err := w.store.AppendMessage(ctx, AppendInput{
		PlaceID: DefaultGeneralChannelID, Author: w.humanA, Content: "顔です",
		ClientNonce: "avatar-as-attachment", AttachmentIDs: []string{avatarID},
	})
	if !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("sending the avatar as an attachment: error = %v, want ErrAttachmentNotFound", err)
	}
}

func TestProfileRouteIsOwnerOnlyAndAnswersBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, ts := newTestServer(t, ctx)

	resp, _ := call(t, ts, http.MethodGet, "/messaging/bootstrap", w.humanA.ID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap: status %d", resp.StatusCode)
	}

	resp, body := call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID, map[string]any{
		"display_name": "余白", "tagline": "創業・デザイン",
		"avatar_attachment_id": "", "banner_attachment_id": "",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set profile: status %d body %v", resp.StatusCode, body)
	}
	if body["display_name"] != "余白" || body["tagline"] != "創業・デザイン" {
		t.Fatalf("profile response = %v", body)
	}

	// There is no field naming whose profile it is; the session decides.
	resp, _ = call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID, map[string]any{
		"display_name": "他人", "participant": map[string]string{"kind": "human", "human_id": w.humanB.ID},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("profile with a participant field: status %d, want 400", resp.StatusCode)
	}

	resp, _ = call(t, ts, http.MethodPut, "/messaging/profile", w.humanA.ID, map[string]any{
		"display_name": "  ",
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("blank display name: status %d, want 400", resp.StatusCode)
	}

	resp, body = call(t, ts, http.MethodGet, "/messaging/profile", w.humanB.ID, nil)
	if resp.StatusCode != http.StatusOK || body["display_name"] != "Haru" {
		t.Fatalf("B reads own profile: status %d body %v", resp.StatusCode, body)
	}
}

func TestLocalProfileReadsThenChangesOnlyNamedFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	server := NewServer(w.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: w.agent.ID}

	localProfile := func(payload string) map[string]string {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, LocalProfilePath, strings.NewReader(payload)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.localProfile(response, request, authorization)
		if response.Code != http.StatusOK {
			t.Fatalf("local profile %s: status %d body %s", payload, response.Code, response.Body.String())
		}
		var decoded struct {
			Profile memberWire `json:"profile"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode local profile: %v", err)
		}
		return map[string]string{
			"display_name": decoded.Profile.DisplayName,
			"tagline":      decoded.Profile.Tagline,
		}
	}

	// An empty request is a read, not a silent overwrite.
	read := localProfile(`{}`)
	if read["display_name"] != "Kuro（Yohaku）" || read["tagline"] != "" {
		t.Fatalf("initial read = %v", read)
	}

	// Naming only the tagline keeps the name; naming only the name keeps the
	// tagline. The projected qualifier is never written back to the registry.
	if got := localProfile(`{"tagline":"開発"}`); got["display_name"] != "Kuro（Yohaku）" || got["tagline"] != "開発" {
		t.Fatalf("tagline-only update = %v", got)
	}
	if got := localProfile(`{"display_name":"墨"}`); got["display_name"] != "墨（Yohaku）" || got["tagline"] != "開発" {
		t.Fatalf("name-only update = %v", got)
	}
	stored, err := w.store.MemberProfileFor(ctx, w.agent)
	if err != nil {
		t.Fatalf("read stored profile: %v", err)
	}
	if stored.DisplayName != "墨" {
		t.Fatalf("registry display name = %q, want the canonical name without the qualifier", stored.DisplayName)
	}
}
