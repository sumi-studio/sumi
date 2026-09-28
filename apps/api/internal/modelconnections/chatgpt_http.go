package modelconnections

import (
	"context"
	"errors"
	"net/http"
)

func (s *Service) registerChatGPTRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/model-connections/chatgpt/login", s.chatGPTLogin)
	mux.HandleFunc("GET /api/model-connections/chatgpt/login/{id}", s.chatGPTLoginStatus)
	mux.HandleFunc("DELETE /api/model-connections/chatgpt/login/{id}", s.chatGPTLoginCancel)
	mux.HandleFunc("PUT /api/model-connections/chatgpt/{id}", s.chatGPTSettings)
}

// chatGPTAvailability is the list response's subscription section.
func (s *Service) chatGPTAvailability() map[string]any {
	if s.Store != nil && s.Store.ChatGPTEnabled() {
		return map[string]any{"available": true}
	}
	return map[string]any{"available": false, "unavailableReason": "このサーバーではChatGPTのサブスクリプション接続を利用できません。"}
}

func chatGPTFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrChatGPTDisabled):
		respond(w, 409, map[string]any{"error": map[string]string{"message": "このサーバーではChatGPTのサブスクリプション接続を利用できません。"}})
	case errors.Is(err, ErrDeviceLoginUnavailable):
		respond(w, 502, map[string]any{"error": map[string]string{"code": loginErrUnavailable, "message": "ChatGPTのデバイスコードによるログインを開始できませんでした。ChatGPTのセキュリティ設定でCodexのデバイスコード認証が有効か確認して、もう一度お試しください。"}})
	case errors.Is(err, errIssuerTransient), errors.Is(err, errLoginFailed):
		respond(w, 502, map[string]any{"error": map[string]string{"message": "ChatGPTのログインを開始できませんでした。しばらくしてからもう一度お試しください。"}})
	default:
		failure(w, err)
	}
}

func (s *Service) chatGPTLogin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ConnectionID string `json:"connectionId,omitempty"`
	}
	if !decode(w, r, &in) {
		return
	}
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	if s.Store == nil {
		chatGPTFailure(w, ErrChatGPTDisabled)
		return
	}
	var view LoginView
	err := id.Authorize(r.Context(), func(ctx context.Context) error {
		var err error
		view, err = s.Store.BeginChatGPTLogin(ctx, id.HumanID, id.SessionID, in.ConnectionID)
		return err
	})
	if err != nil {
		chatGPTFailure(w, err)
		return
	}
	respond(w, 200, view)
}

func (s *Service) chatGPTLoginStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	if s.Store == nil {
		chatGPTFailure(w, ErrChatGPTDisabled)
		return
	}
	// A read may complete the login (store + select the connection), so it
	// runs under the session's revalidation like any other mutation and
	// reports a runtime-affecting selection change.
	before, err := s.Store.RuntimeFingerprint(r.Context(), id.HumanID)
	if err != nil {
		failure(w, err)
		return
	}
	var view LoginView
	err = id.Authorize(r.Context(), func(ctx context.Context) error {
		var err error
		view, err = s.Store.PollChatGPTLogin(ctx, id.HumanID, id.SessionID, r.PathValue("id"))
		return err
	})
	if err != nil {
		chatGPTFailure(w, err)
		return
	}
	if view.Status == "completed" && s.Changed != nil {
		after, ferr := s.Store.RuntimeFingerprint(r.Context(), id.HumanID)
		if ferr != nil || after != before {
			s.Changed(id.HumanID)
		}
	}
	respond(w, 200, view)
}

func (s *Service) chatGPTLoginCancel(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	if s.Store == nil {
		chatGPTFailure(w, ErrChatGPTDisabled)
		return
	}
	var view LoginView
	err := id.Authorize(r.Context(), func(ctx context.Context) error {
		var err error
		view, err = s.Store.CancelChatGPTLogin(ctx, id.HumanID, id.SessionID, r.PathValue("id"))
		return err
	})
	if err != nil {
		chatGPTFailure(w, err)
		return
	}
	respond(w, 200, view)
}

func (s *Service) chatGPTSettings(w http.ResponseWriter, r *http.Request) {
	var in ChatGPTSettings
	if !decode(w, r, &in) {
		return
	}
	var out Connection
	if s.mutate(w, r, func(ctx context.Context, human string) error {
		var err error
		out, err = s.Store.SetChatGPTSettings(ctx, human, r.PathValue("id"), in)
		return err
	}) {
		respond(w, 200, out)
	}
}
