package messaging

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
)

type scopedContractFixture struct {
	workspace    Workspace
	installation applicationapps.Installation
}

func newScopedContractFixture(t *testing.T, ctx context.Context, w world, name string, members ...ParticipantRef) scopedContractFixture {
	t.Helper()
	workspace, err := w.store.createWorkspace(ctx, name, w.humanA)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if err := w.store.addWorkspaceMember(ctx, workspace.WorkspaceID, member); err != nil {
			t.Fatal(err)
		}
	}
	installation, err := w.apps.ResolveEnabledInstallation(ctx,
		applicationapps.WorkspaceOwner(workspace.WorkspaceID), w.humanA, MessagingAppID)
	if err != nil {
		t.Fatal(err)
	}
	return scopedContractFixture{workspace: workspace, installation: installation}
}

func (f scopedContractFixture) scope(t *testing.T, w world, actor ParticipantRef) *ScopedStore {
	t.Helper()
	scoped, err := w.store.core.Scoped(Scope{
		WorkspaceID: f.workspace.WorkspaceID, InstallationID: f.installation.InstallationID,
		AuthorityEpoch: f.installation.AuthorityEpoch, Actor: actor,
	})
	return mustScopedStore(t, scoped, err)
}

func TestExactScopeIsolatesWorkspacesAndInstallationLifecycles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	first := newScopedContractFixture(t, ctx, w, "first", w.humanB)
	second := newScopedContractFixture(t, ctx, w, "second", w.humanB)
	firstChannel, err := first.scope(t, w, w.humanA).CreateChannel(ctx, "general", "")
	if err != nil {
		t.Fatal(err)
	}
	secondChannel, err := second.scope(t, w, w.humanA).CreateChannel(ctx, "general", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.scope(t, w, w.humanA).PlaceFor(ctx, secondChannel.PlaceID); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("cross-Workspace place read = %v, want ErrPlaceNotFound", err)
	}
	if _, err := second.scope(t, w, w.humanB).PlaceFor(ctx, firstChannel.PlaceID); !errors.Is(err, ErrPlaceNotFound) {
		t.Fatalf("reverse cross-Workspace place read = %v, want ErrPlaceNotFound", err)
	}

	if _, err := w.apps.SetEnabledByID(ctx, first.installation.InstallationID, w.humanA, false); err != nil {
		t.Fatal(err)
	}
	if _, err := first.scope(t, w, w.humanA).PlaceFor(ctx, firstChannel.PlaceID); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
		t.Fatalf("disabled stale scope = %v", err)
	}
	if err := w.apps.UninstallByID(ctx, first.installation.InstallationID, w.humanA); err != nil {
		t.Fatal(err)
	}
	reinstalled, err := w.apps.InstallAtOperation(ctx, applicationapps.WorkspaceOwner(first.workspace.WorkspaceID), w.humanA, MessagingAppID, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.scope(t, w, w.humanA).PlaceFor(ctx, firstChannel.PlaceID); !errors.Is(err, applicationapps.ErrInstallationNotFound) {
		t.Fatalf("uninstalled stale scope = %v", err)
	}
	first.installation = reinstalled
	if _, err := first.scope(t, w, w.humanA).PlaceFor(ctx, firstChannel.PlaceID); err != nil {
		t.Fatalf("new exact installation did not recover preserved data: %v", err)
	}
}

func TestStaleAuthorityEpochCannotReviveAcrossMessagingAuthorizers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "epoch", w.agent)
	stale := fixture.scope(t, w, w.humanA)
	channel, err := stale.CreateChannel(ctx, "before-disable", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.apps.SetEnabledByID(
		ctx, fixture.installation.InstallationID, w.humanA, false,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := w.apps.SetEnabledByID(
		ctx, fixture.installation.InstallationID, w.humanA, true,
	); err != nil {
		t.Fatal(err)
	}

	assertStale := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, applicationapps.ErrInstallationNotFound) {
			t.Fatalf("%s stale epoch error = %v, want installation not found", name, err)
		}
	}
	_, err = stale.Workspace(ctx)
	assertStale("read", err)
	_, err = stale.OpenSnapshot(ctx, channel.PlaceID, HistoryOptions{Limit: 10})
	assertStale("snapshot", err)
	_, _, err = stale.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "stale mutation", ClientNonce: "stale-epoch",
	})
	assertStale("mutation", err)
	_, err = stale.CreateChannel(ctx, "stale-manage", "")
	assertStale("manage channels", err)

	current := w.store.mustScope(t, ctx, fixture.workspace.WorkspaceID, w.humanA)
	if current.Scope.AuthorityEpoch != stale.Scope.AuthorityEpoch+1 {
		t.Fatalf("current epoch = %d, stale = %d",
			current.Scope.AuthorityEpoch, stale.Scope.AuthorityEpoch)
	}
	if _, err := current.Workspace(ctx); err != nil {
		t.Fatalf("current read rejected: %v", err)
	}
	if _, _, err := current.AppendMessage(ctx, AppendInput{
		PlaceID: channel.PlaceID, Content: "current mutation", ClientNonce: "current-epoch",
	}); err != nil {
		t.Fatalf("current mutation rejected: %v", err)
	}
}

func TestExactScopedReadsHaveNoPersistenceSideEffects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "reads", w.humanB)
	channel, err := fixture.scope(t, w, w.humanA).CreateChannel(ctx, "general", "")
	if err != nil {
		t.Fatal(err)
	}
	scoped := fixture.scope(t, w, w.humanB)
	counts := func() [3]int {
		var got [3]int
		if err := w.store.core.pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM place_members),
			       (SELECT count(*) FROM read_markers),
			       (SELECT count(*) FROM notification_settings)`).Scan(&got[0], &got[1], &got[2]); err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := counts()
	if _, err := scoped.Workspace(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.WorkspaceMembers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.PlaceFor(ctx, channel.PlaceID); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.History(ctx, channel.PlaceID, HistoryOptions{}); err != nil {
		t.Fatal(err)
	}
	if marker, err := scoped.ReadMarker(ctx, channel.PlaceID); err != nil || marker != 0 {
		t.Fatalf("marker=%d err=%v", marker, err)
	}
	if _, err := scoped.NotificationSettingFor(ctx); err != nil {
		t.Fatal(err)
	}
	if after := counts(); after != before {
		t.Fatalf("read side effects: before=%v after=%v", before, after)
	}
}

func TestRESTRejectsMissingDisabledAndStaleExactScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "rest")
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	request := func(workspaceID, installationID, authorityEpoch string) *http.Response {
		path := "/messaging/bootstrap"
		if workspaceID != "" || installationID != "" || authorityEpoch != "" {
			q := url.Values{
				"workspace_id": {workspaceID}, "installation_id": {installationID},
				"authority_epoch": {authorityEpoch},
			}
			path += "?" + q.Encode()
		}
		req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if got := request("", "", "").StatusCode; got != http.StatusBadRequest {
		t.Fatalf("missing scope=%d", got)
	}
	if _, err := w.apps.SetEnabledByID(ctx, fixture.installation.InstallationID, w.humanA, false); err != nil {
		t.Fatal(err)
	}
	epoch := strconv.FormatInt(fixture.installation.AuthorityEpoch, 10)
	if got := request(fixture.workspace.WorkspaceID, fixture.installation.InstallationID, epoch).StatusCode; got != http.StatusNotFound {
		t.Fatalf("disabled exact scope=%d", got)
	}
	reenabled, err := w.apps.SetEnabledByID(
		ctx, fixture.installation.InstallationID, w.humanA, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := request(fixture.workspace.WorkspaceID, fixture.installation.InstallationID, epoch).StatusCode; got != http.StatusNotFound {
		t.Fatalf("pre-disable epoch revived after re-enable: status=%d", got)
	}
	currentEpoch := strconv.FormatInt(reenabled.AuthorityEpoch, 10)
	if got := request(fixture.workspace.WorkspaceID, fixture.installation.InstallationID, currentEpoch).StatusCode; got != http.StatusOK {
		t.Fatalf("current re-enabled epoch rejected: status=%d", got)
	}
	if err := w.apps.UninstallByID(ctx, fixture.installation.InstallationID, w.humanA); err != nil {
		t.Fatal(err)
	}
	if _, err := w.apps.InstallAtOperation(ctx, applicationapps.WorkspaceOwner(fixture.workspace.WorkspaceID), w.humanA, MessagingAppID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if got := request(fixture.workspace.WorkspaceID, fixture.installation.InstallationID, epoch).StatusCode; got != http.StatusNotFound {
		t.Fatalf("uninstalled stale exact scope=%d", got)
	}
}

func TestRESTAuthorityEpochWireIsCanonical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	fixture := newScopedContractFixture(t, ctx, w, "rest-wire")
	server := NewServer(w.store.core, stubSessions{})
	server.AllowedOrigins = []string{testOrigin}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	base := "/messaging/bootstrap?workspace_id=" + fixture.workspace.WorkspaceID +
		"&installation_id=" + fixture.installation.InstallationID
	for name, suffix := range map[string]string{
		"missing":      "",
		"duplicate":    "&authority_epoch=1&authority_epoch=1",
		"null":         "&authority_epoch=null",
		"empty":        "&authority_epoch=",
		"plus":         "&authority_epoch=%2B1",
		"leading zero": "&authority_epoch=01",
		"zero":         "&authority_epoch=0",
		"overflow":     "&authority_epoch=9223372036854775808",
	} {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, ts.URL+base+suffix, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.AddCookie(&http.Cookie{
				Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID,
			})
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
	req, err := http.NewRequest(http.MethodGet, ts.URL+base+"&authority_epoch=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: w.humanA.ID})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("canonical epoch status = %d, want 200", resp.StatusCode)
	}
}

func TestWSRejectsDisabledAndUninstalledExactInstallation(t *testing.T) {
	for _, lifecycle := range []string{"disabled", "uninstalled"} {
		t.Run(lifecycle, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			w, ts := newWSWorld(t, ctx)
			_, channel := w.workspaceWithChannel(t, ctx)
			scoped := w.store.mustScopeForPlace(t, ctx, channel.PlaceID, w.humanA)
			conn := dialWS(t, ts, w.humanA.ID, nil)
			if lifecycle == "disabled" {
				if _, err := w.apps.SetEnabledByID(ctx, scoped.Scope.InstallationID, w.humanA, false); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := w.apps.UninstallByID(ctx, scoped.Scope.InstallationID, w.humanA); err != nil {
					t.Fatal(err)
				}
			}
			if err := conn.WriteJSON(map[string]any{
				"type": "send", "place_id": channel.PlaceID, "content": "stale",
				"client_nonce": "stale-" + lifecycle,
			}); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err == nil && frame["type"] != "error" {
				t.Fatalf("stale %s WS remained admitted: %v", lifecycle, frame)
			}
		})
	}
}
