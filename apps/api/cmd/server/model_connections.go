package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

func modelConnectionStoreFromEnv(pool *pgxpool.Pool) (*modelconnections.Store, error) {
	raw := strings.TrimSpace(os.Getenv("SUMI_MODEL_CONNECTION_KEY"))
	if raw == "" {
		if pool == nil {
			return nil, nil
		}
		return modelconnections.MetadataOnly(pool), nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil, errors.New("SUMI_MODEL_CONNECTION_KEY must encode 32 bytes")
	}
	defer clear(key)
	return modelconnections.New(pool, key)
}

type userModelStore interface {
	Selected(context.Context, string) (modelconnections.Selection, bool, error)
	Metadata(context.Context, string, string) (modelconnections.Access, error)
}

func userModelActivation(store userModelStore, employers chatGPTEmployerAuthority, chat *chatGPTRuntime, fallback runtimeActivationResolver) runtimeActivationResolver {
	return func(ctx context.Context, pa string, base runtimeprovision.ActivationConfig) (runtimeprovision.ActivationConfig, error) {
		kind, human, err := employers.CurrentEmployer(ctx, pa)
		if err != nil {
			return base, err
		}
		if kind != "human" {
			if fallback != nil {
				return fallback(ctx, pa, base)
			}
			return base, nil
		}
		var selected modelconnections.Selection
		var exists bool
		err = employers.AuthorizeCurrentHumanEmployer(ctx, human, pa, func() error { var e error; selected, exists, e = store.Selected(ctx, human); return e })
		if err != nil {
			return base, err
		}
		if !exists {
			if fallback != nil {
				return fallback(ctx, pa, base)
			}
			return base, nil
		}
		switch selected.Kind {
		case "none":
			return base, errors.New("no model connection selected")
		case "chatgpt":
			if chat == nil {
				return base, errors.New("ChatGPT connection unavailable")
			}
			// An explicit selection must never silently fall back to the operator key.
			base.ProviderAPIKey = ""
			base.ModelPreset = ""
			base.ModelID = ""
			base.ModelBaseURL = ""
			base.ModelPublicEndpoint = false
			base.APIConnectionID = ""
			base.APIConnectionVersion = ""
			base.APIConnectionHumanID = ""
			next, e := chat.activation(ctx, pa, base)
			if e != nil {
				return base, e
			}
			if next.ChatGPTConnectionID == "" {
				return base, errors.New("ChatGPT connection unavailable")
			}
			return next, nil
		case "api":
			err = employers.AuthorizeCurrentHumanEmployer(ctx, human, pa, func() error {
				// Re-read selection under the same employer authority immediately before resolving.
				latest, ok, e := store.Selected(ctx, human)
				if e != nil {
					return e
				}
				if !ok || latest != selected {
					return errors.New("model selection changed; retry")
				}
				a, e := store.Metadata(ctx, human, selected.ConnectionID)
				if e != nil {
					return e
				}
				base.ProviderAPIKey = ""
				base.APIConnectionID = a.Connection.ID
				base.APIConnectionVersion = a.Version
				base.APIConnectionHumanID = human
				base.ModelPreset = a.Connection.Preset
				base.ModelID = a.Connection.Model
				base.ModelBaseURL = a.Connection.BaseURL
				base.ModelPublicEndpoint = true
				base.ModelAccountScope = "human-api:" + human + ":" + a.Connection.ID + ":" + a.Version
				base.ChatGPTConnectionID = ""
				base.ModelReasoningEffort = ""
				return nil
			})
			return base, err
		default:
			return base, modelconnections.ErrInvalid
		}
	}
}

const userAPIAccessPath = "/internal/providers/api/access"

type userAPIAccessStore interface {
	Selected(context.Context, string) (modelconnections.Selection, bool, error)
	Resolve(context.Context, string, string) (modelconnections.Access, error)
}

func userAPIAccess(store userAPIAccessStore, employers chatGPTEmployerAuthority) agentevents.LocalAuthorizedHandler {
	return func(w http.ResponseWriter, r *http.Request, auth agentevents.LocalRuntimeAuthorization) {
		w.Header().Set("Cache-Control", "no-store")
		var request struct {
			ConnectionID string `json:"connection_id"`
			Version      string `json:"version"`
			HumanID      string `json:"human_id"`
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2048))
		if err != nil || agentevents.DecodeStrictJSON(body, &request) != nil || request.ConnectionID == "" || request.Version == "" || request.HumanID == "" {
			http.Error(w, "invalid API access request", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		committed := false
		err = employers.AuthorizeCurrentHumanEmployer(ctx, request.HumanID, auth.PersonalityAgentID, func() error {
			selected, ok, e := store.Selected(ctx, request.HumanID)
			if e != nil {
				return e
			}
			if !ok || selected.Kind != "api" || selected.ConnectionID != request.ConnectionID {
				return modelconnections.ErrUnavailable
			}
			access, e := store.Resolve(ctx, request.HumanID, request.ConnectionID)
			if e != nil {
				return e
			}
			if access.Version != request.Version {
				return modelconnections.ErrUnavailable
			}
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
			w.Header().Set("Content-Type", "application/json")
			committed = true
			return json.NewEncoder(w).Encode(struct {
				APIKey string `json:"api_key"`
			}{access.APIKey})
		})
		if err != nil && !committed {
			http.Error(w, "API connection changed or unavailable; restart with current selection", 503)
		}
	}
}
