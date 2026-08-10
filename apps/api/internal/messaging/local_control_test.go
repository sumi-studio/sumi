package messaging

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestLocalOpenAdmitsAgentWithoutOverviewFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	server := NewServer(world.store, nil)

	request := httptest.NewRequest(
		http.MethodPost,
		LocalOpenPath,
		strings.NewReader(`{"place_id":"`+DefaultGeneralChannelID+`","limit":20}`),
	).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.localOpen(response, request, agentevents.LocalRuntimeAuthorization{
		PersonalityAgentID: world.agent.ID,
	})

	if response.Code != http.StatusOK {
		t.Fatalf("direct first-use open status = %d, body = %s", response.Code, response.Body.String())
	}
	var opened struct {
		Members []memberWire `json:"members"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &opened); err != nil {
		t.Fatalf("decode local open: %v", err)
	}
	if !hasProjectedMember(opened.Members, world.agent, "Kuro（Yohaku）") {
		t.Fatalf("local open members = %#v", opened.Members)
	}
	workspaces, err := world.store.WorkspacesFor(ctx, world.agent)
	if err != nil {
		t.Fatalf("list agent workspaces: %v", err)
	}
	if len(workspaces) != 1 || workspaces[0].WorkspaceID != DefaultWorkspaceID {
		t.Fatalf("agent workspaces = %#v, want only default Workspace", workspaces)
	}
	overview, err := server.buildOverview(ctx, world.agent)
	if err != nil {
		t.Fatalf("build overview: %v", err)
	}
	if !hasProjectedMember(overview.Members, world.agent, "Kuro（Yohaku）") {
		t.Fatalf("overview members = %#v", overview.Members)
	}
}

func hasProjectedMember(members []memberWire, participant ParticipantRef, name string) bool {
	for _, member := range members {
		ref, err := member.Participant.ref()
		if err == nil && ref == participant && member.DisplayName == name {
			return true
		}
	}
	return false
}

// The agent opens conversations through the same store calls the human
// sidebar uses: one participant reuses the single dm (idempotent, like
// EnsureDM behind POST /messaging/dms), several mint a group dm.
func TestLocalStartDMOpensTheSamePlacesAsTheHumanUI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	server := NewServer(world.store, nil)
	for _, participant := range []ParticipantRef{world.humanA, world.humanB} {
		if err := world.store.EnsureDefaultWorkspaceMembership(ctx, participant); err != nil {
			t.Fatalf("admit %s: %v", participant.Key(), err)
		}
	}

	startDM := func(body string) (int, struct {
		DM      dmWire `json:"dm"`
		Created bool   `json:"created"`
	},
	) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, LocalStartDMPath, strings.NewReader(body)).
			WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.localStartDM(response, request, agentevents.LocalRuntimeAuthorization{
			PersonalityAgentID: world.agent.ID,
		})
		var decoded struct {
			DM      dmWire `json:"dm"`
			Created bool   `json:"created"`
		}
		if response.Code < 300 {
			if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
				t.Fatalf("decode start-dm: %v (%s)", err, response.Body.String())
			}
		}
		return response.Code, decoded
	}

	code, first := startDM(`{"participants":[{"kind":"human","human_id":"` + world.humanA.ID + `"}]}`)
	if code != http.StatusCreated || !first.Created || first.DM.Kind != PlaceDM {
		t.Fatalf("first start-dm = %d %#v", code, first)
	}
	// The pair has exactly one dm: asking again returns it rather than a second.
	code, again := startDM(`{"participants":[{"kind":"human","human_id":"` + world.humanA.ID + `"}]}`)
	if code != http.StatusOK || again.Created || again.DM.DMID != first.DM.DMID {
		t.Fatalf("repeat start-dm = %d %#v (first %s)", code, again, first.DM.DMID)
	}

	code, group := startDM(`{"participants":[
		{"kind":"human","human_id":"` + world.humanA.ID + `"},
		{"kind":"human","human_id":"` + world.humanB.ID + `"}]}`)
	if code != http.StatusCreated || group.DM.Kind != PlaceGroupDM {
		t.Fatalf("group start-dm = %d %#v", code, group)
	}
	// The place the agent just opened is one it can actually write in.
	if _, err := world.store.PlaceFor(ctx, group.DM.DMID, world.agent); err != nil {
		t.Fatalf("agent cannot see the group dm it opened: %v", err)
	}

	for _, body := range []string{
		`{"participants":[]}`,
		`{"participants":[{"kind":"app","human_id":"` + world.humanA.ID + `"}]}`,
	} {
		if code, _ := startDM(body); code != http.StatusBadRequest {
			t.Fatalf("start-dm %s status = %d, want 400", body, code)
		}
	}
}

// The agent's channel lifecycle is the human context menu's, through the same
// Store calls: create, rename/retopic, duplicate.
func TestLocalChannelLifecycleMatchesTheHumanMenu(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	world := newWorld(t, ctx)
	server := NewServer(world.store, nil)
	authorization := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: world.agent.ID}
	// Channel administration is the same operation on both lanes, including
	// the same manage_channels permission. Give this actor that capability so
	// the test exercises lifecycle parity instead of accidentally depending on
	// the pre-role-model default membership.
	if err := world.store.EnsureDefaultWorkspaceMembership(ctx, world.humanA); err != nil {
		t.Fatalf("admit founding admin: %v", err)
	}
	if _, err := world.store.SetParticipantRoles(
		ctx, DefaultWorkspaceID, world.humanA, world.agent, []string{DefaultAdminRoleID},
	); err != nil {
		t.Fatalf("grant channel administration to agent: %v", err)
	}

	post := func(path, body string, handler func(http.ResponseWriter, *http.Request, agentevents.LocalRuntimeAuthorization)) (int, channelWire) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler(response, request, authorization)
		var decoded struct {
			Channel channelWire `json:"channel"`
		}
		if response.Code < 300 {
			if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
				t.Fatalf("decode %s: %v (%s)", path, err, response.Body.String())
			}
		}
		return response.Code, decoded.Channel
	}

	// workspace_id may be omitted: the agent is in exactly one workspace.
	code, created := post(LocalCreateChannelPath, `{"name":"設計","topic":"図面の相談"}`, server.localCreateChannel)
	if code != http.StatusCreated || created.Name != "設計" || created.WorkspaceID != DefaultWorkspaceID {
		t.Fatalf("create-channel = %d %#v", code, created)
	}

	code, renamed := post(LocalUpdateChannelPath,
		`{"place_id":"`+created.ChannelID+`","name":"設計と素材"}`, server.localUpdateChannel)
	if code != http.StatusOK || renamed.Name != "設計と素材" || renamed.Topic != "図面の相談" {
		t.Fatalf("update-channel = %d %#v, want the topic left alone", code, renamed)
	}

	code, copied := post(LocalDuplicateChannelPath,
		`{"place_id":"`+created.ChannelID+`"}`, server.localDuplicateChannel)
	if code != http.StatusCreated || copied.Name != "設計と素材 のコピー" ||
		copied.ChannelID == created.ChannelID {
		t.Fatalf("duplicate-channel = %d %#v", code, copied)
	}

	// An edit that names nothing is refused rather than silently succeeding.
	if code, _ := post(LocalUpdateChannelPath,
		`{"place_id":"`+created.ChannelID+`"}`, server.localUpdateChannel); code != http.StatusBadRequest {
		t.Fatalf("empty update status = %d, want 400", code)
	}
	if code, _ := post(LocalCreateChannelPath, `{"name":""}`, server.localCreateChannel); code != http.StatusBadRequest {
		t.Fatalf("empty name status = %d, want 400", code)
	}
}
