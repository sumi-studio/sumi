package handler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/store"
)

func TestInternalTombstoneRoutesRequireBoundRuntimeIdentity(t *testing.T) {
	s, err := store.OpenTombstoneStore(filepath.Join(t.TempDir(), "tombstones.json"))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterAuthenticatedTombstoneRoutes(mux, s, RuntimeIdentity{TenantID: "tenant-a", AgentID: "agent-a", Token: "runtime-token"}, true)
	body := []byte(`{"tenant_id":"tenant-a","agent_id":"agent-a","conversation_id":"old","replacement_conversation_id":"new","command_id":"cmd","command_seq":1,"scope":"conversation","purge_after":"2030-01-01T00:00:00Z"}`)
	for name, req := range map[string]*http.Request{
		"missing-auth":   httptest.NewRequest(http.MethodPost, "/internal/agent/tombstones", bytes.NewReader(body)),
		"wrong-identity": httptest.NewRequest(http.MethodPost, "/internal/agent/tombstones", bytes.NewReader(bytes.Replace(body, []byte("tenant-a"), []byte("tenant-b"), 1))),
	} {
		req.Header.Set("Authorization", "Bearer runtime-token")
		req.Header.Set("X-Sumi-Internal-Principal", "agent-runtime")
		if name == "missing-auth" {
			req.Header.Del("Authorization")
		}
		got := httptest.NewRecorder()
		mux.ServeHTTP(got, req)
		want := http.StatusForbidden
		if name == "missing-auth" {
			want = http.StatusUnauthorized
		}
		if got.Code != want {
			t.Fatalf("%s status=%d want=%d", name, got.Code, want)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/agent/tombstones", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer runtime-token")
	req.Header.Set("X-Sumi-Internal-Principal", "agent-runtime")
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("authenticated create status=%d body=%s", res.Code, res.Body.String())
	}
	if len(s.ListForAgent("tenant-a", "agent-a")) != 1 {
		t.Fatal("authenticated tombstone was not persisted")
	}
}
