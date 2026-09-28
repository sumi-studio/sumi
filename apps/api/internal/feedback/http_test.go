package feedback

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
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
