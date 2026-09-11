package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/testfs"
)

type sessions struct{ revoked bool }

func (s sessions) VerifySession(_ context.Context, cookie string) (agentevents.UserSessionClaims, error) {
	return agentevents.UserSessionClaims{UserID: cookie}, nil
}
func (s sessions) AuthorizeSession(_ context.Context, _ agentevents.UserSessionClaims, op func() error) error {
	if s.revoked {
		return errors.New("revoked")
	}
	return op()
}
func TestFeedbackBrowserAndPABoundaries(t *testing.T) {
	w := fixture(t)
	s := &Server{Store: w.s, Sessions: sessions{}, AllowedOrigins: []string{"https://sumi.test"}}
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	request := func(method, path, cookie, origin, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if cookie != "" {
			r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: cookie})
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, r)
		return res
	}
	create := `{"title":"Feedback","body":"Description","request_id":"` + uuid.NewString() + `"}`
	if r := request("POST", "/feedback/threads", w.human.ID, "https://evil.test", create); r.Code != 403 {
		t.Fatal(r.Code, r.Body.String())
	}
	if r := request("GET", "/feedback/bootstrap", "", "", ""); r.Code != 401 {
		t.Fatal(r.Code)
	}
	s.Sessions = sessions{revoked: true}
	if r := request("POST", "/feedback/threads", w.human.ID, "https://sumi.test", create); r.Code != 401 {
		t.Fatal("revoked session committed", r.Code)
	}
	s.Sessions = sessions{}
	if r := request("POST", "/feedback/threads", w.human.ID, "https://sumi.test", strings.TrimSuffix(create, "}")+`,"author":"forged"}`); r.Code != 400 {
		t.Fatal(r.Code, r.Body.String())
	}
	res := request("POST", "/feedback/threads", w.human.ID, "https://sumi.test", create)
	if res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	var humanThread Thread
	if err := json.Unmarshal(res.Body.Bytes(), &humanThread); err != nil {
		t.Fatal(err)
	}
	if humanThread.Author.Participant.HumanID != w.human.ID {
		t.Fatal("wrong author")
	}
	if r := request("GET", "/feedback/threads/"+humanThread.ID, w.stranger.ID, "", ""); r.Code != 404 {
		t.Fatal(r.Code)
	}
	commands, err := agentevents.OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer commands.Close()
	gateway, err := agentevents.OpenDurableGateway(testfs.PrivateDir(t), commands)
	if err != nil {
		t.Fatal(err)
	}
	const bearer = "feedback-test-bearer-token-generation-one"
	control, err := agentevents.NewLocalControlServer(gateway, []byte("feedback-test-signing-secret-32-bytes"), []agentevents.LocalRuntimeAuthorization{{BearerToken: bearer, TenantID: "feedback-test", PersonalityAgentID: w.pa.ID, Generation: 1, RPCBootNonce: "feedback-test-boot", Audience: agentevents.DefaultAgentAudience(), DeliveryAuthorization: agentevents.LocalDeliveryRaw}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RegisterLocalControlRoutes(control); err != nil {
		t.Fatal(err)
	}
	handler, err := control.HandlerForLocalRuntime(w.pa.ID)
	if err != nil {
		t.Fatal(err)
	}
	local := func(op, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/local-control/v1/feedback:"+op, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, r)
		return res
	}
	if r := local("create", "wrong", create); r.Code != 401 {
		t.Fatal(r.Code, r.Body.String())
	}
	if r := local("open", bearer, `{"thread_id":"`+humanThread.ID+`"}`); r.Code != 404 {
		t.Fatal("PA inherited employer access", r.Code, r.Body.String())
	}
	res = local("create", bearer, create)
	if res.Code != 200 {
		t.Fatal(res.Code, res.Body.String())
	}
	var paThread Thread
	if err = json.Unmarshal(res.Body.Bytes(), &paThread); err != nil {
		t.Fatal(err)
	}
	if paThread.Author.Participant.PersonalityAgentID != w.pa.ID {
		t.Fatal("PA identity lost")
	}
	if r := request("GET", "/feedback/threads/"+paThread.ID, w.dev.ID, "", ""); r.Code != 200 {
		t.Fatal("developer cannot read PA feedback", r.Code, r.Body.String())
	}
	if r := local("create", bearer, strings.TrimSuffix(create, "}")+`,"human_id":"`+w.human.ID+`"}`); r.Code != 400 {
		t.Fatal("forged human identity accepted")
	}
}

func TestFeedbackRequestBoundaryRetainsEscapedUnicodeAndRejectsTrailingData(t *testing.T) {
	var p struct {
		Body string `json:"body"`
	}
	raw := `{"body":"` + strings.Repeat(`\ud83d\ude00`, 20000) + `"}`
	if err := decode(httptest.NewRequest("POST", "/", strings.NewReader(raw)), &p); err != nil {
		t.Fatal("valid maximum Unicode content rejected", err)
	}
	if !validText(p.Body, 20000) {
		t.Fatal("escaped Unicode content was lost")
	}
	raw = `{"body":"hello"}` + strings.Repeat(" ", 256<<10) + `{"body":"hidden"}`
	if err := decode(httptest.NewRequest("POST", "/", strings.NewReader(raw)), &p); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversize request hid trailing data", err)
	}
}
