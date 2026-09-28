package main

import (
	"encoding/base64"
	"errors"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
)

func modelConnectionStoreFromEnv(pool *pgxpool.Pool) (*modelconnections.Store, error) {
	raw := strings.TrimSpace(os.Getenv("SUMI_MODEL_CONNECTION_KEY"))
	if raw == "" {
		if strings.TrimSpace(os.Getenv("SUMI_CHATGPT_SUBSCRIPTION")) == "enabled" {
			return nil, errors.New("SUMI_CHATGPT_SUBSCRIPTION=enabled requires SUMI_MODEL_CONNECTION_KEY")
		}
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
	store, err := modelconnections.New(pool, key)
	if err != nil {
		return nil, err
	}
	if err := enableChatGPTFromEnv(store, os.Getenv); err != nil {
		return nil, err
	}
	return store, nil
}

// enableChatGPTFromEnv turns on ChatGPT subscription connections when the
// operator sets SUMI_CHATGPT_SUBSCRIPTION=enabled. It is off by default:
// whether a hosted deployment may offer "Sign in with ChatGPT" to its
// users is an operator/policy decision (docs/agent/chatgpt-subscription.md),
// not something the code assumes. Any other non-empty value is a
// configuration error rather than a silent default.
func enableChatGPTFromEnv(store *modelconnections.Store, getenv func(string) string) error {
	switch strings.TrimSpace(getenv("SUMI_CHATGPT_SUBSCRIPTION")) {
	case "", "disabled":
		return nil
	case "enabled":
		if err := store.EnableChatGPT(modelconnections.NewOAuthClient()); err != nil {
			return errors.New("SUMI_CHATGPT_SUBSCRIPTION=enabled requires SUMI_MODEL_CONNECTION_KEY")
		}
		return nil
	default:
		return errors.New("SUMI_CHATGPT_SUBSCRIPTION must be enabled or disabled")
	}
}
