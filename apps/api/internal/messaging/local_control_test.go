package messaging

import (
	"context"
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
	workspaces, err := world.store.WorkspacesFor(ctx, world.agent)
	if err != nil {
		t.Fatalf("list agent workspaces: %v", err)
	}
	if len(workspaces) != 1 || workspaces[0].WorkspaceID != DefaultWorkspaceID {
		t.Fatalf("agent workspaces = %#v, want only default Workspace", workspaces)
	}
}
