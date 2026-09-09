package publicweb

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestLocalControlRechecksEpochWithoutHoldingLeaseDuringIO(t *testing.T) {
	for _, phase := range []string{"before_send", "after_fetch", "current"} {
		t.Run(phase, func(t *testing.T) {
			var released, held atomic.Bool
			var requests atomic.Int32
			f, dials := testFetcher(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if !released.Load() || held.Load() {
					t.Error("network I/O held epoch lease")
				}
				w.Header().Set("Content-Type", "text/plain")
				io.WriteString(w, "observed")
			})
			checks := 0
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/public-web/read", strings.NewReader(`{"url":"https://example.com/#anchor"}`))
			(&Server{Fetcher: f}).handle(w, r, agentevents.LocalRuntimeAuthorization{}, func() { released.Store(true) }, func(op func() error) (bool, error) {
				checks++
				if !released.Load() {
					t.Error("initial lease not released")
				}
				if phase == "before_send" || phase == "after_fetch" && checks == 2 {
					return false, nil
				}
				held.Store(true)
				err := op()
				held.Store(false)
				return true, err
			})
			if phase == "current" {
				if w.Code != 200 || requests.Load() != 1 {
					t.Fatal(w.Code, w.Body.String(), requests.Load())
				}
			} else {
				var failure Failure
				if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
					t.Fatal(err)
				}
				if w.Code != 409 || failure.Code != "runtime_epoch_changed" {
					t.Fatal(w.Code, w.Body.String())
				}
				if phase == "before_send" && dials.Load() != 0 {
					t.Fatal("stale epoch sent a request")
				}
			}
		})
	}
}
func TestLocalControlRejectsIdentityAndHeaderInjection(t *testing.T) {
	f, dials := testFetcher(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected fetch") })
	for _, body := range []string{`{"url":"https://example.com","personality_agent_id":"other"}`, `{"url":"https://example.com","headers":{"Cookie":"secret"}}`, `{"url":"https://example.com"} {}`} {
		w := httptest.NewRecorder()
		(&Server{Fetcher: f}).handle(w, httptest.NewRequest("POST", "/public-web/read", strings.NewReader(body)), agentevents.LocalRuntimeAuthorization{}, func() {}, func(op func() error) (bool, error) { return true, op() })
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if dials.Load() != 0 {
		t.Fatal("invalid request reached network")
	}
}
