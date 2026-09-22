// Package mcpconnections connects human-granted remote tools to durable Core jobs.
package mcpconnections

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

var ErrInvalid = errors.New("invalid MCP connection")
var ErrUnavailable = errors.New("MCP connection unavailable or permission revoked")

type Connection struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Enabled  bool   `json:"enabled"`
}
type Input struct {
	Name        string `json:"name"`
	Endpoint    string `json:"endpoint"`
	Enabled     bool   `json:"enabled"`
	BearerToken string `json:"bearerToken"`
}
type Store struct {
	pool          *pgxpool.Pool
	aead          cipher.AEAD
	allowLoopback bool
}

func New(pool *pgxpool.Pool, key []byte) (*Store, error) {
	if pool == nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	a, e := cipher.NewGCM(b)
	return &Store{pool: pool, aead: a}, e
}
func (s *Store) Save(ctx context.Context, human, id string, in Input) (Connection, error) {
	u, e := url.Parse(in.Endpoint)
	if e != nil || len(in.Endpoint) > 2048 || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(s.allowLoopback && u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return Connection{}, fmt.Errorf("%w: endpoint must be HTTPS without credentials, query or fragment", ErrInvalid)
	}
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 120 || len(in.BearerToken) > 8192 || strings.IndexFunc(in.Name+in.BearerToken, unicode.IsControl) >= 0 || strings.ContainsAny(in.BearerToken, " \t") {
		return Connection{}, ErrInvalid
	}
	create := id == ""
	if create {
		id = uuid.NewString()
	} else if _, e := uuid.Parse(id); e != nil {
		return Connection{}, ErrInvalid
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, e := rand.Read(nonce); e != nil {
		return Connection{}, e
	}
	aad, _ := json.Marshal([]string{"sumi.mcp.v1", human, id})
	sealed := s.aead.Seal(nonce, nonce, []byte(in.BearerToken), aad)
	var tagErr error
	if create {
		_, tagErr = s.pool.Exec(ctx, `INSERT INTO mcp_connections VALUES($1,$2,$3,$4,$5,$6,$7)`, human, id, in.Name, in.Endpoint, in.Enabled, sealed, uuid.NewString())
	} else {
		tag, e := s.pool.Exec(ctx, `UPDATE mcp_connections SET name=$3,endpoint=$4,enabled=$5,credential_ciphertext=$6,version=$7 WHERE human_id=$1 AND connection_id=$2`, human, id, in.Name, in.Endpoint, in.Enabled, sealed, uuid.NewString())
		tagErr = e
		if e == nil && tag.RowsAffected() == 0 {
			tagErr = ErrUnavailable
		}
	}
	return Connection{id, in.Name, in.Endpoint, in.Enabled}, tagErr
}
func (s *Store) List(ctx context.Context, human string) ([]Connection, error) {
	rows, e := s.pool.Query(ctx, `SELECT connection_id,name,endpoint,enabled FROM mcp_connections WHERE human_id=$1 ORDER BY name,connection_id`, human)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Connection{}
	for rows.Next() {
		var c Connection
		if e := rows.Scan(&c.ID, &c.Name, &c.Endpoint, &c.Enabled); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *Store) Delete(ctx context.Context, human, id string) error {
	if _, e := uuid.Parse(id); e != nil {
		return ErrInvalid
	}
	tag, e := s.pool.Exec(ctx, `DELETE FROM mcp_connections WHERE human_id=$1 AND connection_id=$2`, human, id)
	if e == nil && tag.RowsAffected() == 0 {
		return ErrUnavailable
	}
	return e
}
func (s *Store) Effects() map[string]agentstate.ToolEffect {
	out := map[string]agentstate.ToolEffect{}
	out["mcp.connections"] = agentstate.ToolEffect{ReadOnly: agentstate.AlwaysReadOnly, Apply: func(ctx context.Context, tx pgx.Tx, persona, idem string, req map[string]any) (map[string]any, error) {
		rows, e := tx.Query(ctx, `SELECT c.connection_id,c.name FROM mcp_connections c JOIN core_personas p ON p.human_id=c.human_id WHERE p.persona_id=$1 AND c.enabled ORDER BY c.name,c.connection_id LIMIT 100`, persona)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id, name string
			if e := rows.Scan(&id, &name); e != nil {
				return nil, e
			}
			out = append(out, map[string]any{"connection_id": id, "name": name})
		}
		return map[string]any{"connections": out}, rows.Err()
	}}
	for _, method := range []string{"list_tools", "call"} {
		method := method
		out["mcp."+method] = agentstate.ToolEffect{Apply: func(ctx context.Context, tx pgx.Tx, persona, idem string, req map[string]any) (map[string]any, error) {
			id, _ := req["connection_id"].(string)
			if _, e := uuid.Parse(id); e != nil {
				return nil, fmt.Errorf("%w: connection_id required", agentstate.ErrBadRequest)
			}
			name, _ := req["name"].(string)
			if method == "call" && (name == "" || len(name) > 256) {
				return nil, fmt.Errorf("%w: tool name required", agentstate.ErrBadRequest)
			}
			args, ok := req["arguments"].(map[string]any)
			if method == "call" && !ok {
				return nil, fmt.Errorf("%w: arguments must be an object", agentstate.ErrBadRequest)
			}
			raw, e := json.Marshal(args)
			if e != nil || len(raw) > 32<<10 {
				return nil, fmt.Errorf("%w: arguments exceed 32 KiB", agentstate.ErrBadRequest)
			}
			cursor, _ := req["cursor"].(string)
			if len(cursor) > 2048 {
				return nil, fmt.Errorf("%w: cursor too long", agentstate.ErrBadRequest)
			}
			var version string
			e = tx.QueryRow(ctx, `SELECT c.version FROM mcp_connections c JOIN core_personas p ON p.human_id=c.human_id WHERE p.persona_id=$1 AND p.authority='active' AND c.connection_id=$2 AND c.enabled FOR SHARE OF c`, persona, id).Scan(&version)
			if errors.Is(e, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrUnavailable)
			}
			if e != nil {
				return nil, e
			}
			request := map[string]any{"connection_id": id, "version": version, "method": method}
			if method == "call" {
				request["name"] = name
				request["arguments"] = args
			} else {
				request["cursor"] = cursor
			}
			jobID, e := agentstate.EffectJobID(idem)
			if e != nil {
				return nil, e
			}
			_, e = tx.Exec(ctx, `INSERT INTO core_jobs(persona_id,job_id,kind,request,status,created_by) VALUES($1,$2,'mcp',$3,'queued',$4)`, persona, jobID, request, "tool:"+idem)
			return map[string]any{"job": map[string]any{"job_id": jobID, "kind": "mcp", "status": "queued"}}, e
		}}
	}
	return out
}
