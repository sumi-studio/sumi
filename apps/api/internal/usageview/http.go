// Package usageview exposes the authenticated human's usage and budget
// surface: which funding source each secretary call spent, what it cost
// under the configured rate card, and the explicit budget that stops
// further spend. Authority derives only from the authenticated session's
// human — never a browser-supplied actor id.
package usageview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/chatgpt"
)

// Service is the human-facing usage/budget API. Store is the core state
// store (shared with the persona-scoped internal surface); Authenticate
// resolves the browser session to its human.
type Service struct {
	Store        *agentstate.Store
	Authenticate func(*http.Request) (chatgpt.LoginIdentity, error)
	// Resumed is invoked after a funding change resumes parked inputs —
	// useful for surfacing/telemetry wiring; optional.
	Resumed func(humanID string, resumed int)
}

func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/usage", s.overview)
	mux.HandleFunc("GET /api/usage/funding/{kind}/{id}/facts", s.facts)
	mux.HandleFunc("PUT /api/usage/funding/{kind}/{id}/budget", s.setBudget)
	mux.HandleFunc("DELETE /api/usage/funding/{kind}/{id}/budget", s.clearBudget)
}

// FundingChanged resumes this human's budget-parked inputs — wired to
// modelConnectionService.Changed so a selection or connection change
// reopens the funding question for waiting work.
func (s *Service) FundingChanged(humanID string) {
	if s == nil || s.Store == nil || humanID == "" {
		return
	}
	if n, err := s.Store.ResumeWaitsForHuman(context.Background(), humanID); err == nil && n > 0 && s.Resumed != nil {
		s.Resumed(humanID, n)
	}
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
	status := 500
	message := "利用状況を確認できませんでした。"
	switch {
	case errors.Is(err, agentstate.ErrFundingNotFound):
		status, message = 404, "その資金源は見つかりません。"
	case errors.Is(err, agentstate.ErrFundingForbidden):
		status, message = 403, "その資金源はあなたのものではありません。"
	case errors.Is(err, agentstate.ErrBadRequest):
		status, message = 400, "上限の入力内容を確認してください。"
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

func (s *Service) overview(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	sources, err := s.Store.UsageForHuman(r.Context(), id.HumanID, 20)
	if err != nil {
		failure(w, err)
		return
	}
	waits, err := s.Store.BudgetWaitsForHuman(r.Context(), id.HumanID)
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, map[string]any{"sources": sources, "waits": waits})
}

func (s *Service) facts(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		var n int
		if _, err := fmt.Sscanf(raw, "%d", &n); err == nil {
			limit = n
		}
	}
	facts, err := s.Store.FactsForFunding(r.Context(), id.HumanID,
		r.PathValue("kind"), r.PathValue("id"), limit)
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, map[string]any{"facts": facts})
}

type budgetInput struct {
	LimitMinor        int64  `json:"limit_minor"`
	Currency          string `json:"currency"`
	RateInputPerMTok  int64  `json:"rate_input_per_mtok"`
	RateOutputPerMTok int64  `json:"rate_output_per_mtok"`
	RateCachedPerMTok *int64 `json:"rate_cached_per_mtok"`
	PricingRevision   string `json:"pricing_revision"`
}

func decodeBudget(w http.ResponseWriter, r *http.Request, in *budgetInput) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(in); err != nil {
		failure(w, agentstate.ErrBadRequest)
		return false
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		failure(w, agentstate.ErrBadRequest)
		return false
	}
	return true
}

func (s *Service) setBudget(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	var in budgetInput
	if !decodeBudget(w, r, &in) {
		return
	}
	// A 'sumi' allocation's budget belongs to the funder, not the
	// grantee — configuring it is not a self-service act.
	if r.PathValue("kind") == "sumi" {
		failure(w, agentstate.ErrFundingForbidden)
		return
	}
	var out agentstate.UsageBudget
	err := id.Authorize(r.Context(), func(ctx context.Context) error {
		var err error
		out, err = s.Store.SetBudget(ctx, id.HumanID,
			r.PathValue("kind"), r.PathValue("id"), agentstate.UsageBudget{
				LimitMinor:        in.LimitMinor,
				Currency:          in.Currency,
				RateInputPerMTok:  in.RateInputPerMTok,
				RateOutputPerMTok: in.RateOutputPerMTok,
				RateCachedPerMTok: in.RateCachedPerMTok,
				PricingRevision:   in.PricingRevision,
			})
		return err
	})
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, map[string]any{"budget": out})
}

func (s *Service) clearBudget(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}
	if r.PathValue("kind") == "sumi" {
		failure(w, agentstate.ErrFundingForbidden)
		return
	}
	err := id.Authorize(r.Context(), func(ctx context.Context) error {
		return s.Store.ClearBudget(ctx, id.HumanID,
			r.PathValue("kind"), r.PathValue("id"))
	})
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 204, nil)
}
