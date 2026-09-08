// Package chatgpt stores Human-owned inference connections, not Sumi sign-in identities.
package chatgpt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotConnected          = errors.New("ChatGPT is not connected")
	ErrReconnectRequired     = errors.New("ChatGPT reconnect required")
	ErrAccountChanged        = errors.New("ChatGPT account changed")
	ErrInvalidConnection     = errors.New("invalid ChatGPT connection")
	ErrCredentialUnavailable = errors.New("ChatGPT credential unavailable")
	ErrRefreshFailed         = errors.New("ChatGPT refresh failed")
)

// Credentials is an internal OAuth handoff. Token fields never serialize to JSON.
// Refresh may omit AccountID or RefreshToken, retaining the existing values.
type Credentials struct {
	AccessToken  string    `json:"-"`
	RefreshToken string    `json:"-"`
	AccountID    string    `json:"-"`
	ExpiresAt    time.Time `json:"-"`
}
type Selection struct {
	Model  string `json:"model"`
	Effort string `json:"effort"`
}
type Status struct {
	Connected    bool      `json:"connected"`
	ConnectionID string    `json:"connectionId,omitempty"`
	AccountID    string    `json:"accountId,omitempty"`
	ExpiresAt    time.Time `json:"expiresAt,omitempty"`
	Selection
	ReconnectRequired bool `json:"reconnectRequired"`
}

// Access is trusted server/runtime material. It must not be browser status.
type Access struct {
	Token        string    `json:"-"`
	AccountID    string    `json:"-"`
	ConnectionID string    `json:"-"`
	ExpiresAt    time.Time `json:"-"`
	Selection    `json:"-"`
}
type RefreshFunc func(context.Context, string) (Credentials, error)
type Store struct {
	pool *pgxpool.Pool
	aead cipher.AEAD
}

func New(pool *pgxpool.Pool, key []byte) (*Store, error) {
	if pool == nil || len(key) != 32 {
		return nil, ErrInvalidConnection
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool, aead: aead}, nil
}

var modelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func validSelection(s Selection) bool {
	if !modelPattern.MatchString(s.Model) {
		return false
	}
	switch s.Effort {
	case "low", "medium", "high", "xhigh", "max":
		return true
	}
	return false
}
func validAccount(s string) bool {
	return len(s) > 0 && len(s) <= 256 && strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) < 0
}
func validTokens(c Credentials) bool {
	return c.AccessToken != "" && len(c.AccessToken) <= 65536 && len(c.RefreshToken) <= 65536 && !c.ExpiresAt.IsZero() && c.ExpiresAt.After(time.Now())
}

type secretPayload struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

func aad(human, account string) []byte {
	b, _ := json.Marshal([]string{"sumi.chatgpt.v1", human, account})
	return b
}
func (s *Store) encrypt(human string, c Credentials) ([]byte, error) {
	plain, err := json.Marshal(secretPayload{c.AccessToken, c.RefreshToken})
	if err != nil {
		return nil, ErrCredentialUnavailable
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, ErrCredentialUnavailable
	}
	return s.aead.Seal(nonce, nonce, plain, aad(human, c.AccountID)), nil
}
func (s *Store) decrypt(human, account string, b []byte) (secretPayload, error) {
	var result secretPayload
	n := s.aead.NonceSize()
	if len(b) < n {
		return result, ErrCredentialUnavailable
	}
	plain, err := s.aead.Open(nil, b[:n], b[n:], aad(human, account))
	if err != nil {
		return result, ErrCredentialUnavailable
	}
	if json.Unmarshal(plain, &result) != nil {
		return result, ErrCredentialUnavailable
	}
	return result, nil
}
func (s *Store) Connect(ctx context.Context, human string, c Credentials, selection Selection) (Status, error) {
	if !validAccount(c.AccountID) || !validTokens(c) || c.RefreshToken == "" || !validSelection(selection) {
		return Status{}, ErrInvalidConnection
	}
	ciphertext, err := s.encrypt(human, c)
	if err != nil {
		return Status{}, err
	}
	id := uuid.NewString()
	_, err = s.pool.Exec(ctx, `INSERT INTO chatgpt_connections(human_id,connection_id,account_id,credential_ciphertext,expires_at,model,effort)
 VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(human_id) DO UPDATE SET connection_id=EXCLUDED.connection_id,
 account_id=EXCLUDED.account_id,credential_ciphertext=EXCLUDED.credential_ciphertext,expires_at=EXCLUDED.expires_at,
 model=EXCLUDED.model,effort=EXCLUDED.effort,reconnect_required=false,updated_at=now()`, human, id, c.AccountID, ciphertext, c.ExpiresAt, selection.Model, selection.Effort)
	if err != nil {
		return Status{}, err
	}
	return Status{Connected: true, ConnectionID: id, AccountID: c.AccountID, ExpiresAt: c.ExpiresAt, Selection: selection}, nil
}
func (s *Store) Status(ctx context.Context, human string) (Status, error) {
	var status Status
	err := s.pool.QueryRow(ctx, `SELECT connection_id::text,account_id,expires_at,model,effort,reconnect_required FROM chatgpt_connections WHERE human_id=$1`, human).Scan(&status.ConnectionID, &status.AccountID, &status.ExpiresAt, &status.Model, &status.Effort, &status.ReconnectRequired)
	if errors.Is(err, pgx.ErrNoRows) {
		return status, nil
	}
	status.Connected = err == nil
	return status, err
}
func (s *Store) Disconnect(ctx context.Context, human string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM chatgpt_connections WHERE human_id=$1`, human)
	return err
}
func (s *Store) SetSelection(ctx context.Context, human, connectionID string, selection Selection) error {
	if !validSelection(selection) {
		return ErrInvalidConnection
	}
	result, err := s.pool.Exec(ctx, `UPDATE chatgpt_connections SET model=$3,effort=$4,updated_at=now() WHERE human_id=$1 AND connection_id::text=$2`, human, connectionID, selection.Model, selection.Effort)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrNotConnected
	}
	return nil
}

// ResolveAccess serializes refresh across API processes. Disconnect/reconnect
// share the row lock, so a finished disconnect cannot be resurrected by refresh.
// rejectedToken is the access token rejected by an upstream 401, or empty.
// A concurrent refresh that already replaced it needs no second rotation.
func (s *Store) ResolveAccess(ctx context.Context, human, connectionID, rejectedToken string, refresh RefreshFunc) (Access, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Access{}, err
	}
	defer tx.Rollback(context.Background())
	var out Access
	var ciphertext []byte
	var reconnect bool
	err = tx.QueryRow(ctx, `SELECT connection_id::text,account_id,credential_ciphertext,expires_at,model,effort,reconnect_required
 FROM chatgpt_connections WHERE human_id=$1 FOR UPDATE`, human).Scan(&out.ConnectionID, &out.AccountID, &ciphertext, &out.ExpiresAt, &out.Model, &out.Effort, &reconnect)
	if errors.Is(err, pgx.ErrNoRows) {
		return Access{}, ErrNotConnected
	}
	if err != nil {
		return Access{}, err
	}
	if connectionID == "" || out.ConnectionID != connectionID {
		return Access{}, ErrNotConnected
	}
	if reconnect {
		return Access{}, ErrReconnectRequired
	}
	secret, err := s.decrypt(human, out.AccountID, ciphertext)
	if err != nil {
		return Access{}, err
	}
	out.Token = secret.AccessToken
	if out.ExpiresAt.After(time.Now().Add(5*time.Minute)) && (rejectedToken == "" || rejectedToken != out.Token) {
		if err = tx.Commit(ctx); err != nil {
			return Access{}, err
		}
		return out, nil
	}
	if refresh == nil {
		return Access{}, ErrRefreshFailed
	}
	updated, err := refresh(ctx, secret.RefreshToken)
	if errors.Is(err, ErrReconnectRequired) {
		if _, e := tx.Exec(ctx, `UPDATE chatgpt_connections SET reconnect_required=true,updated_at=now() WHERE human_id=$1`, human); e != nil {
			return Access{}, e
		}
		if e := tx.Commit(ctx); e != nil {
			return Access{}, e
		}
		return Access{}, ErrReconnectRequired
	}
	// Do not propagate arbitrary provider response text, which may contain tokens.
	if err != nil {
		return Access{}, ErrRefreshFailed
	}
	if updated.AccountID == "" {
		updated.AccountID = out.AccountID
	}
	if updated.AccountID != out.AccountID {
		return Access{}, ErrAccountChanged
	}
	if updated.RefreshToken == "" {
		updated.RefreshToken = secret.RefreshToken
	}
	unchangedAccess := updated.AccessToken == "" || updated.AccessToken == secret.AccessToken
	if unchangedAccess {
		// An omitted/unchanged access token cannot acquire a new lifetime, nor
		// repair a 401. Persist refresh rotation without pretending otherwise.
		updated.AccessToken = secret.AccessToken
		updated.ExpiresAt = out.ExpiresAt
	}
	if len(updated.RefreshToken) > 65536 || (!unchangedAccess && !validTokens(updated)) {
		return Access{}, ErrRefreshFailed
	}
	ciphertext, err = s.encrypt(human, updated)
	if err != nil {
		return Access{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE chatgpt_connections SET credential_ciphertext=$2,expires_at=$3,updated_at=now() WHERE human_id=$1`, human, ciphertext, updated.ExpiresAt)
	if err != nil {
		return Access{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Access{}, err
	}
	if unchangedAccess {
		return Access{}, ErrRefreshFailed
	}
	out.Token = updated.AccessToken
	out.ExpiresAt = updated.ExpiresAt
	return out, nil
}
