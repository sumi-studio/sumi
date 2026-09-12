package modelconnections

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
)

type Service struct {
	Store        *Store
	Authenticate func(*http.Request) (chatgpt.LoginIdentity, error)
	Changed      func(string)
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/model-connections", s.list)
	mux.HandleFunc("POST /api/model-connections/api", s.save)
	mux.HandleFunc("PUT /api/model-connections/api/{id}", s.save)
	mux.HandleFunc("DELETE /api/model-connections/api/{id}", s.remove)
	mux.HandleFunc("PUT /api/model-connections/selection", s.selectConnection)
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status != 204 {
		_ = json.NewEncoder(w).Encode(v)
	}
}
func failure(w http.ResponseWriter, err error) {
	status := 503
	message := "AI接続を更新できませんでした。"
	if errors.Is(err, ErrInvalid) {
		status = 400
		message = "接続の入力内容を確認してください。"
	}
	if errors.Is(err, ErrNotFound) {
		status = 404
		message = "接続が見つかりません。"
	}
	respond(w, status, map[string]any{"error": map[string]string{"message": message}})
}
func (s *Service) identity(w http.ResponseWriter, r *http.Request) (chatgpt.LoginIdentity, bool) {
	if s.Authenticate == nil {
		respond(w, 401, map[string]string{"error": "Sign in to Sumi"})
		return chatgpt.LoginIdentity{}, false
	}
	id, err := s.Authenticate(r)
	if err != nil {
		respond(w, 401, map[string]string{"error": "Sign in to Sumi"})
		return id, false
	}
	return id, true
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 72<<10))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		failure(w, ErrInvalid)
		return false
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		failure(w, ErrInvalid)
		return false
	}
	return true
}
func (s *Service) list(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	if s.Store == nil {
		respond(w, 200, map[string]any{"available": false, "unavailableReason": "このサーバーにはAPI接続の保存用キーが設定されていません。", "connections": []Connection{}, "selection": nil, "activation": "next_start"})
		return
	}
	list, err := s.Store.List(r.Context(), id.HumanID)
	if err != nil {
		failure(w, err)
		return
	}
	v, exists, err := s.Store.Selected(r.Context(), id.HumanID)
	if err != nil {
		failure(w, err)
		return
	}
	var selected *Selection
	if exists {
		selected = &v
	}
	response := map[string]any{"available": s.Store.CredentialsAvailable(), "connections": list, "selection": selected, "activation": "next_start"}
	if !s.Store.CredentialsAvailable() {
		response["unavailableReason"] = "このサーバーにはAPI接続の保存用キーが設定されていません。保存済みの選択は維持されています。"
	}
	respond(w, 200, response)
}
func (s *Service) mutate(w http.ResponseWriter, r *http.Request, effect func(context.Context, string) error) bool {
	id, ok := s.identity(w, r)
	if !ok {
		return false
	}
	if !s.Store.CredentialsAvailable() {
		failure(w, ErrUnavailable)
		return false
	}
	before, err := s.Store.RuntimeFingerprint(r.Context(), id.HumanID)
	if err != nil {
		failure(w, err)
		return false
	}
	err = id.Authorize(r.Context(), func(ctx context.Context) error { return effect(ctx, id.HumanID) })
	if err != nil {
		failure(w, err)
		return false
	}
	after, fingerprintErr := s.Store.RuntimeFingerprint(r.Context(), id.HumanID)
	if s.Changed != nil && (fingerprintErr != nil || before != after) {
		s.Changed(id.HumanID)
	}
	return true
}
func (s *Service) save(w http.ResponseWriter, r *http.Request) {
	var in Input
	if !decode(w, r, &in) {
		return
	}
	var out Connection
	if s.mutate(w, r, func(ctx context.Context, human string) error {
		var err error
		out, err = s.Store.Save(ctx, human, r.PathValue("id"), in)
		return err
	}) {
		respond(w, 200, out)
	}
}
func (s *Service) remove(w http.ResponseWriter, r *http.Request) {
	if s.mutate(w, r, func(ctx context.Context, human string) error { return s.Store.Delete(ctx, human, r.PathValue("id")) }) {
		respond(w, 204, nil)
	}
}
func (s *Service) selectConnection(w http.ResponseWriter, r *http.Request) {
	var in Selection
	if !decode(w, r, &in) {
		return
	}
	if s.mutate(w, r, func(ctx context.Context, human string) error { return s.Store.Select(ctx, human, in) }) {
		respond(w, 200, in)
	}
}
