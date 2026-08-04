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
