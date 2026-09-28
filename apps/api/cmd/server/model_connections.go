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
