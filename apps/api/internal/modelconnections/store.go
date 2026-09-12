// Package modelconnections manages Human-owned API credentials. Secrets are write-only to browsers.
package modelconnections

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalid = errors.New("invalid model connection")
var ErrNotFound = errors.New("model connection not found")
var ErrUnavailable = errors.New("model credential unavailable")

type Connection struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Preset  string `json:"preset"`
	BaseURL string `json:"baseUrl"`
	Model   string `json:"model"`
}
type Input struct {
	Name    string  `json:"name"`
	Preset  string  `json:"preset"`
	BaseURL string  `json:"baseUrl"`
	Model   string  `json:"model"`
	APIKey  *string `json:"apiKey,omitempty"`
}
type Selection struct {
	Kind         string `json:"kind"`
	ConnectionID string `json:"connectionId,omitempty"`
}
type Access struct {
	Connection Connection `json:"-"`
	APIKey     string     `json:"-"`
	Version    string     `json:"-"`
}
type Store struct {
	pool *pgxpool.Pool
	aead cipher.AEAD
}

// MetadataOnly keeps persisted selection authoritative while decryption is unavailable.
func MetadataOnly(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }
func (s *Store) CredentialsAvailable() bool  { return s != nil && s.aead != nil }

func New(pool *pgxpool.Pool, key []byte) (*Store, error) {
	if pool == nil || len(key) != 32 {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	return &Store{pool, aead}, err
}
func bounded(s string, n int) bool {
	return strings.TrimSpace(s) != "" && len(s) <= n && strings.IndexFunc(s, unicode.IsControl) < 0
}
func Validate(in Input) error {
	if !bounded(in.Name, 120) || !bounded(in.Model, 128) {
		return ErrInvalid
	}
	switch in.Preset {
	case "openai-chat", "openai-responses", "anthropic", "kimi-k3", "glm-5.2", "umans", "umans-kimi-k2.7", "opencode-go", "opencode-zen-go":
	default:
		return ErrInvalid
	}
	u, err := url.Parse(in.BaseURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(in.BaseURL) > 2048 {
		return ErrInvalid
	}
	// Activation additionally requires transport-level destination enforcement.
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || (ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast())) {
		return ErrInvalid
	}
	if in.APIKey != nil && (!bounded(*in.APIKey, 65536) || strings.TrimSpace(*in.APIKey) != *in.APIKey) {
		return ErrInvalid
	}
	return nil
}
func (s *Store) seal(human, id, key string) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, ErrUnavailable
	}
	aad, _ := json.Marshal([]string{"sumi.api.v1", human, id})
	return s.aead.Seal(nonce, nonce, []byte(key), aad), nil
}
func (s *Store) open(human, id string, b []byte) (string, error) {
	n := s.aead.NonceSize()
	if len(b) < n {
		return "", ErrUnavailable
	}
	aad, _ := json.Marshal([]string{"sumi.api.v1", human, id})
	v, err := s.aead.Open(nil, b[:n], b[n:], aad)
	if err != nil {
		return "", ErrUnavailable
	}
	return string(v), nil
}
func lockHuman(ctx context.Context, tx pgx.Tx, human string) error {
	var id string
	return tx.QueryRow(ctx, "SELECT human_id::text FROM humans WHERE human_id=$1 FOR UPDATE", human).Scan(&id)
}
func (s *Store) Save(ctx context.Context, human, id string, in Input) (Connection, error) {
	if !s.CredentialsAvailable() {
		return Connection{}, ErrUnavailable
	}
	if err := Validate(in); err != nil {
		return Connection{}, err
	}
	create := id == ""
	if create {
		id = uuid.NewString()
	} else {
		parsed, err := uuid.Parse(id)
		if err != nil {
			return Connection{}, ErrNotFound
		}
		id = parsed.String()
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Connection{}, err
	}
	defer tx.Rollback(context.Background())
	if err = lockHuman(ctx, tx, human); err != nil {
		return Connection{}, err
	}
	var ciphertext []byte
	var previousURL, previousPreset, version string
	if !create {
		err = tx.QueryRow(ctx, "SELECT credential_ciphertext,base_url,preset,version::text FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", human, id).Scan(&ciphertext, &previousURL, &previousPreset, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			return Connection{}, ErrNotFound
		}
		if err != nil {
			return Connection{}, err
		}
	}
	if !create && in.APIKey == nil && previousURL != in.BaseURL {
		return Connection{}, ErrInvalid
	}
	if in.APIKey != nil {
		ciphertext, err = s.seal(human, id, *in.APIKey)
		if err != nil {
			return Connection{}, err
		}
	} else if create {
		return Connection{}, ErrInvalid
	}
	// The binding identifies credential authority, not the display name or model.
	// Model-only edits can finish the current run with its existing model safely;
	// RuntimeFingerprint still schedules the new model for the next idle start.
	if create || in.APIKey != nil || previousURL != in.BaseURL || previousPreset != in.Preset {
		version = uuid.NewString()
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_api_connections(human_id,connection_id,name,preset,base_url,model,credential_ciphertext,version) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(human_id,connection_id) DO UPDATE SET name=EXCLUDED.name,preset=EXCLUDED.preset,base_url=EXCLUDED.base_url,model=EXCLUDED.model,credential_ciphertext=EXCLUDED.credential_ciphertext,version=EXCLUDED.version`, human, id, in.Name, in.Preset, in.BaseURL, in.Model, ciphertext, version)
	if err != nil {
		return Connection{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Connection{}, err
	}
	return Connection{id, in.Name, in.Preset, in.BaseURL, in.Model}, nil
}
func (s *Store) List(ctx context.Context, human string) ([]Connection, error) {
	rows, err := s.pool.Query(ctx, "SELECT connection_id::text,name,preset,base_url,model FROM model_api_connections WHERE human_id=$1 ORDER BY name,connection_id", human)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Connection{}
	for rows.Next() {
		var c Connection
		if err = rows.Scan(&c.ID, &c.Name, &c.Preset, &c.BaseURL, &c.Model); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *Store) Selected(ctx context.Context, human string) (Selection, bool, error) {
	var v Selection
	err := s.pool.QueryRow(ctx, "SELECT kind,COALESCE(connection_id::text,'') FROM model_connection_selections WHERE human_id=$1", human).Scan(&v.Kind, &v.ConnectionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Selection{}, false, nil
	}
	return v, err == nil, err
}
func (s *Store) Select(ctx context.Context, human string, v Selection) error {
	if v.Kind != "none" && v.Kind != "chatgpt" && v.Kind != "api" {
		return ErrInvalid
	}
	if (v.Kind == "api") != (v.ConnectionID != "") {
		return ErrInvalid
	}
	if v.Kind == "api" {
		if _, err := uuid.Parse(v.ConnectionID); err != nil {
			return ErrInvalid
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = lockHuman(ctx, tx, human); err != nil {
		return err
	}
	var found bool
	if v.Kind == "api" {
		err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM model_api_connections WHERE human_id=$1 AND connection_id=$2)", human, v.ConnectionID).Scan(&found)
	} else if v.Kind == "chatgpt" {
		err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM chatgpt_connections WHERE human_id=$1 AND NOT reconnect_required)", human).Scan(&found)
	} else {
		found = true
	}
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_connection_selections(human_id,kind,connection_id) VALUES($1,$2,NULLIF($3,'')::uuid) ON CONFLICT(human_id) DO UPDATE SET kind=EXCLUDED.kind,connection_id=EXCLUDED.connection_id`, human, v.Kind, v.ConnectionID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Delete(ctx context.Context, human, id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return ErrNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = lockHuman(ctx, tx, human); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "UPDATE model_connection_selections SET kind='none',connection_id=NULL WHERE human_id=$1 AND connection_id=$2", human, id)
	if err != nil {
		return err
	}
	res, err := tx.Exec(ctx, "DELETE FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", human, id)
	if err != nil {
		return err
	}
	if res.RowsAffected() != 1 {
		return ErrNotFound
	}
	return tx.Commit(ctx)
}
func (s *Store) Resolve(ctx context.Context, human, id string) (Access, error) {
	if !s.CredentialsAvailable() {
		return Access{}, ErrUnavailable
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Access{}, ErrNotFound
	}
	id = parsed.String()

	var a Access
	var b []byte
	err = s.pool.QueryRow(ctx, "SELECT connection_id::text,name,preset,base_url,model,credential_ciphertext,version::text FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", human, id).Scan(&a.Connection.ID, &a.Connection.Name, &a.Connection.Preset, &a.Connection.BaseURL, &a.Connection.Model, &b, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	if err != nil {
		return a, err
	}
	a.APIKey, err = s.open(human, id, b)
	return a, err
}

// Metadata resolves the selected identity without decrypting its credential.
func (s *Store) Metadata(ctx context.Context, human, id string) (Access, error) {
	if !s.CredentialsAvailable() {
		return Access{}, ErrUnavailable
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Access{}, ErrNotFound
	}
	var a Access
	err = s.pool.QueryRow(ctx, "SELECT connection_id::text,name,preset,base_url,model,version::text FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", human, parsed.String()).Scan(&a.Connection.ID, &a.Connection.Name, &a.Connection.Preset, &a.Connection.BaseURL, &a.Connection.Model, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// RuntimeFingerprint excludes display-only fields and inactive connections.
func (s *Store) RuntimeFingerprint(ctx context.Context, human string) (string, error) {
	var value string
	err := s.pool.QueryRow(ctx, `SELECT s.kind || ':' || COALESCE(s.connection_id::text,'') || ':' || COALESCE(c.version::text,'') || ':' || COALESCE(c.model,'') FROM model_connection_selections s LEFT JOIN model_api_connections c ON c.human_id=s.human_id AND c.connection_id=s.connection_id WHERE s.human_id=$1`, human).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return value, err
}
