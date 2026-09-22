package mcpconnections

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LocalInput is human-provided executable configuration, never a model tool.
// Only safe connection metadata is returned by CRUD or Core discovery.
type LocalInput struct {
	Name        string            `json:"name"`
	Transport   string            `json:"transport"`
	Endpoint    string            `json:"endpoint,omitempty"`
	Enabled     bool              `json:"enabled"`
	BearerToken string            `json:"bearerToken,omitempty"`
	Command     string            `json:"command,omitempty"`
	Args        []string          `json:"args,omitempty"`
	Cwd         string            `json:"cwd,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	PrivateEnv  []string          `json:"privateEnv,omitempty"`
	PrivateArgs []int             `json:"privateArgs,omitempty"`
}

type localScope struct{ host, persona string }

func NewLocal(pool *pgxpool.Pool, key []byte, host, persona string) (*Store, error) {
	if _, e := uuid.Parse(host); e != nil {
		return nil, ErrInvalid
	}
	if _, e := uuid.Parse(persona); e != nil {
		return nil, ErrInvalid
	}
	s, e := New(pool, key)
	if e != nil {
		return nil, e
	}
	s.local = &localScope{host, persona}
	return s, nil
}
func (s *Store) jobKind() string {
	if s.local != nil {
		return "mcp_local:" + s.local.host
	}
	return "mcp"
}
func (s *Store) localAAD(id string) []byte {
	b, _ := json.Marshal([]string{"sumi.local-mcp.v1", s.local.host, s.local.persona, id})
	return b
}
func (s *Store) SaveLocal(ctx context.Context, id string, in LocalInput) (Connection, error) {
	if s.local == nil {
		return Connection{}, ErrUnavailable
	}
	if in.Transport == "" {
		in.Transport = "https"
	}
	if strings.TrimSpace(in.Name) == "" || len(in.Name) > 120 || strings.IndexFunc(in.Name, unicode.IsControl) >= 0 {
		return Connection{}, ErrInvalid
	}
	switch in.Transport {
	case "https":
		if e := s.validateInput(Input{Name: in.Name, Endpoint: in.Endpoint, BearerToken: in.BearerToken}); e != nil {
			return Connection{}, e
		}
		if in.Command != "" || len(in.Args) > 0 || in.Cwd != "" || len(in.Env) > 0 || len(in.PrivateEnv) > 0 || len(in.PrivateArgs) > 0 {
			return Connection{}, ErrInvalid
		}
	case "stdio":
		if in.Endpoint != "" || in.BearerToken != "" || !filepath.IsAbs(in.Command) || !filepath.IsAbs(in.Cwd) || len(in.Args) > 128 || len(in.Env) > 128 {
			return Connection{}, ErrInvalid
		}
		if stat, e := os.Stat(in.Cwd); e != nil || !stat.IsDir() {
			return Connection{}, ErrInvalid
		}
		envName := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
		for k, v := range in.Env {
			if !envName.MatchString(k) || strings.ContainsRune(v, 0) {
				return Connection{}, ErrInvalid
			}
		}
		privateEnv := map[string]bool{}
		for _, key := range in.PrivateEnv {
			if _, exists := in.Env[key]; !exists || privateEnv[key] {
				return Connection{}, ErrInvalid
			}
			privateEnv[key] = true
		}
		privateArgs := map[int]bool{}
		for _, index := range in.PrivateArgs {
			if index < 0 || index >= len(in.Args) || privateArgs[index] {
				return Connection{}, ErrInvalid
			}
			privateArgs[index] = true
		}
		for _, v := range append([]string{in.Command, in.Cwd}, in.Args...) {
			if strings.ContainsRune(v, 0) {
				return Connection{}, ErrInvalid
			}
		}
	default:
		return Connection{}, ErrInvalid
	}
	raw, e := json.Marshal(in)
	if e != nil || len(raw) > 32<<10 {
		return Connection{}, ErrInvalid
	}
	defer clear(raw)
	create := id == ""
	if create {
		id = uuid.NewString()
	} else if _, e := uuid.Parse(id); e != nil {
		return Connection{}, ErrInvalid
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return Connection{}, e
	}
	sealed := s.aead.Seal(nonce, nonce, raw, s.localAAD(id))
	if create {
		_, e = s.pool.Exec(ctx, `INSERT INTO local_mcp_connections(host_id,persona_id,connection_id,name,transport,endpoint,enabled,configuration_ciphertext,version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.local.host, s.local.persona, id, in.Name, in.Transport, in.Endpoint, in.Enabled, sealed, uuid.NewString())
	} else {
		tag, err := s.pool.Exec(ctx, `UPDATE local_mcp_connections SET name=$4,transport=$5,endpoint=$6,enabled=$7,configuration_ciphertext=$8,version=$9 WHERE host_id=$1 AND persona_id=$2 AND connection_id=$3`, s.local.host, s.local.persona, id, in.Name, in.Transport, in.Endpoint, in.Enabled, sealed, uuid.NewString())
		e = err
		if e == nil && tag.RowsAffected() == 0 {
			e = ErrUnavailable
		}
	}
	return Connection{ID: id, Name: in.Name, Endpoint: in.Endpoint, Enabled: in.Enabled, Transport: in.Transport}, e
}
func (s *Store) ListLocal(ctx context.Context) ([]Connection, error) {
	if s.local == nil {
		return nil, ErrUnavailable
	}
	rows, e := s.pool.Query(ctx, `SELECT connection_id,name,endpoint,enabled,transport FROM local_mcp_connections WHERE host_id=$1 AND persona_id=$2 ORDER BY name,connection_id`, s.local.host, s.local.persona)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Connection{}
	for rows.Next() {
		var c Connection
		if e := rows.Scan(&c.ID, &c.Name, &c.Endpoint, &c.Enabled, &c.Transport); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *Store) DeleteLocal(ctx context.Context, id string) error {
	if s.local == nil {
		return ErrUnavailable
	}
	if _, e := uuid.Parse(id); e != nil {
		return ErrInvalid
	}
	tag, e := s.pool.Exec(ctx, `DELETE FROM local_mcp_connections WHERE host_id=$1 AND persona_id=$2 AND connection_id=$3`, s.local.host, s.local.persona, id)
	if e == nil && tag.RowsAffected() == 0 {
		return ErrUnavailable
	}
	return e
}

// Both transports use the same grant pin, schema checks and durable job path.
func (s *Store) configuration(ctx context.Context, tx pgx.Tx, persona, id, version string) (LocalInput, error) {
	var sealed, aad []byte
	var cfg LocalInput
	if s.local != nil {
		if persona != s.local.persona {
			return cfg, ErrUnavailable
		}
		e := tx.QueryRow(ctx, `SELECT c.configuration_ciphertext FROM local_mcp_connections c JOIN core_personas p ON p.persona_id=c.persona_id WHERE p.persona_id=$1 AND c.host_id=$2 AND c.connection_id=$3 AND c.version=$4 AND c.enabled AND p.authority='active' FOR SHARE OF c,p`, persona, s.local.host, id, version).Scan(&sealed)
		if e != nil {
			return cfg, ErrUnavailable
		}
		aad = s.localAAD(id)
	} else {
		var human string
		e := tx.QueryRow(ctx, `SELECT c.human_id,c.endpoint,c.credential_ciphertext FROM mcp_connections c JOIN core_personas p ON p.human_id=c.human_id WHERE p.persona_id=$1 AND p.authority='active' AND c.connection_id=$2 AND c.version=$3 AND c.enabled FOR SHARE OF c,p`, persona, id, version).Scan(&human, &cfg.Endpoint, &sealed)
		if e != nil {
			return cfg, ErrUnavailable
		}
		aad, _ = json.Marshal([]string{"sumi.mcp.v1", human, id})
		cfg.Transport = "https"
	}
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return cfg, errors.New("MCP credential unavailable")
	}
	raw, e := s.aead.Open(nil, sealed[:n], sealed[n:], aad)
	if e != nil {
		return cfg, errors.New("MCP credential unavailable")
	}
	defer clear(raw)
	if s.local != nil {
		e = json.Unmarshal(raw, &cfg)
	} else {
		cfg.BearerToken = string(raw)
	}
	return cfg, e
}

// protectedValues selects only deliberately private values. Configuration is
// always encrypted, but ordinary flags, paths and environment values must not
// rewrite a server's tool identifiers or schema vocabulary.
func (cfg LocalInput) protectedValues() []string {
	values := []string{cfg.BearerToken}
	for _, key := range cfg.PrivateEnv {
		values = append(values, cfg.Env[key])
	}
	for _, index := range cfg.PrivateArgs {
		if index >= 0 && index < len(cfg.Args) {
			values = append(values, cfg.Args[index])
		}
	}
	return values
}
