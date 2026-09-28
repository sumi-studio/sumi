package modelconnections

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EnableChatGPT turns on subscription connections for this store. It
// requires an armed (credential-sealing) store; the OAuth client is the
// only party that talks to the issuer.
func (s *Store) EnableChatGPT(c *OAuthClient) error {
	if !s.CredentialsAvailable() || c == nil {
		return ErrChatGPTDisabled
	}
	s.oauth = c
	return nil
}

// ChatGPTEnabled reports whether subscription logins and token use are on.
func (s *Store) ChatGPTEnabled() bool { return s.CredentialsAvailable() && s.oauth != nil }

var chatGPTModelRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func validEffort(e string) bool {
	switch e {
	case "low", "medium", "high", "xhigh", "max":
		return true
	}
	return false
}

// ChatGPTSettings are the person-editable parts of a subscription connection.
type ChatGPTSettings struct {
	Name            string `json:"name,omitempty"`
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoningEffort"`
}

func (v ChatGPTSettings) validate() error {
	if v.Name != "" && !bounded(v.Name, 120) {
		return invalid("name must be at most 120 characters")
	}
	if !chatGPTModelRe.MatchString(v.Model) {
		return invalid("choose a ChatGPT model")
	}
	if !validEffort(v.ReasoningEffort) {
		return invalid("reasoningEffort must be low, medium, high, xhigh or max")
	}
	return nil
}

// ChatGPTAccess is trusted runtime material for one model call.
type ChatGPTAccess struct {
	Connection  Connection
	Version     string
	AccessToken string
	AccountID   string
	ExpiresAt   time.Time
}

type chatGPTPayload struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// The AAD binds the sealed grant to its owner, connection and ChatGPT
// account: a ciphertext copied to another person's row, another
// connection, or re-labelled with another account does not open.
func chatGPTAAD(human, id, account string) []byte {
	b, _ := json.Marshal([]string{"sumi.chatgpt.v1", human, id, account})
	return b
}

func (s *Store) sealChatGPT(human, id, account string, p chatGPTPayload) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, ErrUnavailable
	}
	plain, err := json.Marshal(p)
	if err != nil {
		return nil, ErrUnavailable
	}
	return s.aead.Seal(nonce, nonce, plain, chatGPTAAD(human, id, account)), nil
}

func (s *Store) openChatGPT(human, id, account string, b []byte) (chatGPTPayload, error) {
	var p chatGPTPayload
	n := s.aead.NonceSize()
	if len(b) < n {
		return p, ErrUnavailable
	}
	plain, err := s.aead.Open(nil, b[:n], b[n:], chatGPTAAD(human, id, account))
	if err != nil || json.Unmarshal(plain, &p) != nil || p.AccessToken == "" || p.RefreshToken == "" {
		return chatGPTPayload{}, ErrUnavailable
	}
	return p, nil
}

// TokenDigest is how a caller names a rejected access token without
// sending the token back: lowercase hex SHA-256 of its bytes.
func TokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// refreshMargin: a token closer than this to expiry is refreshed before
// use, so a streamed call does not start with a token about to lapse.
const refreshMargin = 5 * time.Minute

// resolveTimeout bounds one resolve (row lock wait, issuer refresh and the
// write of its result). It is longer than the OAuth client's HTTP timeout so
// an issuer answer that arrives is always stored.
const resolveTimeout = 30 * time.Second

// ResolveChatGPT returns a usable access token for the person's
// subscription connection, refreshing it when it is near expiry or when
// the caller reports it was rejected (rejectedDigest = TokenDigest of the
// rejected token). The row lock serializes refresh across concurrent calls
// and API processes: a waiter that finds the rejected token already
// replaced uses the new one instead of rotating again, so a refresh token
// is never spent twice. Rows are keyed by (human, connection) — one
// person's refresh or failure never touches another's.
func (s *Store) ResolveChatGPT(ctx context.Context, human, id, rejectedDigest string) (ChatGPTAccess, error) {
	if !s.ChatGPTEnabled() {
		return ChatGPTAccess{}, ErrChatGPTDisabled
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return ChatGPTAccess{}, ErrNotFound
	}
	id = parsed.String()
	// The issuer rotates the refresh token as soon as it answers; the new
	// one must be stored even when the caller has already gone (a Core
	// timeout, a cancelled Worker request, a dropped connection), or the
	// next refresh presents a spent token and the connection is lost. The
	// operation therefore runs on its own bounded context, not the caller's.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resolveTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ChatGPTAccess{}, err
	}
	defer tx.Rollback(context.Background())
	var out ChatGPTAccess
	var account *string
	var expires *time.Time
	var ciphertext []byte
	err = tx.QueryRow(ctx, `SELECT connection_id::text,name,preset,base_url,model,COALESCE(reasoning_effort,''),reconnect_required,version::text,account_id,access_expires_at,credential_ciphertext
 FROM model_api_connections WHERE human_id=$1 AND connection_id=$2 FOR UPDATE`, human, id).Scan(
		&out.Connection.ID, &out.Connection.Name, &out.Connection.Preset, &out.Connection.BaseURL, &out.Connection.Model,
		&out.Connection.ReasoningEffort, &out.Connection.ReconnectRequired, &out.Version, &account, &expires, &ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChatGPTAccess{}, ErrNotFound
	}
	if err != nil {
		return ChatGPTAccess{}, err
	}
	if out.Connection.Preset != ChatGPTPreset || account == nil || expires == nil {
		return ChatGPTAccess{}, ErrInvalid
	}
	if out.Connection.ReconnectRequired {
		return ChatGPTAccess{}, ErrReconnectRequired
	}
	out.AccountID = *account
	secret, err := s.openChatGPT(human, id, out.AccountID, ciphertext)
	if err != nil {
		return ChatGPTAccess{}, err
	}
	now := s.oauth.now()
	rejected := rejectedDigest != "" && rejectedDigest == TokenDigest(secret.AccessToken)
	if !rejected && expires.After(now.Add(refreshMargin)) {
		out.AccessToken, out.ExpiresAt = secret.AccessToken, *expires
		return out, tx.Commit(ctx)
	}
	fresh, err := s.oauth.Refresh(ctx, secret.RefreshToken)
	if err == nil && fresh.AccountID != "" && fresh.AccountID != out.AccountID {
		// The grant now names a different account than the one this
		// connection (and its sealed AAD) belongs to: never switch
		// accounts silently.
		err = ErrReconnectRequired
	}
	if errors.Is(err, ErrReconnectRequired) {
		if _, e := tx.Exec(ctx, `UPDATE model_api_connections SET reconnect_required=true WHERE human_id=$1 AND connection_id=$2`, human, id); e != nil {
			return ChatGPTAccess{}, e
		}
		if e := tx.Commit(ctx); e != nil {
			return ChatGPTAccess{}, e
		}
		return ChatGPTAccess{}, ErrReconnectRequired
	}
	if err == nil && fresh.AccessToken == "" {
		// Without a new access token nothing was repaired; a rotated
		// refresh token, if any, must still be kept.
		if fresh.RefreshToken != "" && fresh.RefreshToken != secret.RefreshToken {
			secret.RefreshToken = fresh.RefreshToken
			sealed, e := s.sealChatGPT(human, id, out.AccountID, secret)
			if e != nil {
				return ChatGPTAccess{}, e
			}
			if _, e = tx.Exec(ctx, `UPDATE model_api_connections SET credential_ciphertext=$3 WHERE human_id=$1 AND connection_id=$2`, human, id, sealed); e != nil {
				return ChatGPTAccess{}, e
			}
		}
		err = ErrRefreshFailed
	}
	if err != nil {
		// Transient: a proactive refresh may still use the current token
		// while it is valid; a rejected or expired one cannot be used.
		if !rejected && expires.After(now.Add(30*time.Second)) {
			out.AccessToken, out.ExpiresAt = secret.AccessToken, *expires
			return out, tx.Commit(ctx)
		}
		if e := tx.Commit(ctx); e != nil {
			return ChatGPTAccess{}, e
		}
		return ChatGPTAccess{}, ErrRefreshFailed
	}
	if fresh.RefreshToken == "" {
		fresh.RefreshToken = secret.RefreshToken
	}
	sealed, err := s.sealChatGPT(human, id, out.AccountID, chatGPTPayload{AccessToken: fresh.AccessToken, RefreshToken: fresh.RefreshToken})
	if err != nil {
		return ChatGPTAccess{}, err
	}
	// The connection version names credential authority (which grant), not
	// a token generation: a refresh keeps it, so the running secretary is
	// not rescheduled for an ordinary rotation.
	if _, err = tx.Exec(ctx, `UPDATE model_api_connections SET credential_ciphertext=$3,access_expires_at=$4 WHERE human_id=$1 AND connection_id=$2`, human, id, sealed, fresh.ExpiresAt); err != nil {
		return ChatGPTAccess{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ChatGPTAccess{}, err
	}
	out.AccessToken, out.ExpiresAt = fresh.AccessToken, fresh.ExpiresAt
	return out, nil
}

// SetChatGPTSettings changes the model / effort / name of a subscription
// connection. The grant and its version are unchanged.
func (s *Store) SetChatGPTSettings(ctx context.Context, human, id string, v ChatGPTSettings) (Connection, error) {
	if err := v.validate(); err != nil {
		return Connection{}, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Connection{}, ErrNotFound
	}
	var c Connection
	err = s.pool.QueryRow(ctx, `UPDATE model_api_connections SET model=$3,reasoning_effort=$4,name=COALESCE(NULLIF($5,''),name)
 WHERE human_id=$1 AND connection_id=$2 AND preset=$6
 RETURNING connection_id::text,name,preset,base_url,model,COALESCE(reasoning_effort,''),reconnect_required`,
		human, parsed.String(), v.Model, v.ReasoningEffort, v.Name, ChatGPTPreset).Scan(
		&c.ID, &c.Name, &c.Preset, &c.BaseURL, &c.Model, &c.ReasoningEffort, &c.ReconnectRequired)
	if errors.Is(err, pgx.ErrNoRows) {
		return Connection{}, ErrNotFound
	}
	return c, err
}

// connectChatGPT stores a freshly authorized grant inside the login's
// transaction. A reconnect (target set, same person, still a subscription
// connection) replaces that connection's grant and gives it a new version,
// leaving the person's selection unchanged; otherwise a new connection is
// created and selected.
func (s *Store) connectChatGPT(ctx context.Context, tx pgx.Tx, human, target string, t OAuthTokens) (Connection, error) {
	if !validAccount(t.AccountID) || t.AccessToken == "" || t.RefreshToken == "" || t.ExpiresAt.IsZero() {
		return Connection{}, errLoginFailed
	}
	if err := lockHuman(ctx, tx, human); err != nil {
		return Connection{}, err
	}
	c := Connection{Name: "ChatGPT", Preset: ChatGPTPreset, BaseURL: ChatGPTBaseURL, Model: DefaultChatGPTModel, ReasoningEffort: DefaultChatGPTEffort}
	existing := false
	if target != "" {
		err := tx.QueryRow(ctx, `SELECT connection_id::text,name,model,COALESCE(reasoning_effort,$3) FROM model_api_connections WHERE human_id=$1 AND connection_id=$2 AND preset=$4`,
			human, target, DefaultChatGPTEffort, ChatGPTPreset).Scan(&c.ID, &c.Name, &c.Model, &c.ReasoningEffort)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Connection{}, err
		}
		existing = err == nil
	}
	if !existing {
		c.ID = uuid.NewString()
	}
	sealed, err := s.sealChatGPT(human, c.ID, t.AccountID, chatGPTPayload{AccessToken: t.AccessToken, RefreshToken: t.RefreshToken})
	if err != nil {
		return Connection{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_api_connections(human_id,connection_id,name,preset,base_url,model,max_output_tokens,credential_ciphertext,version,account_id,access_expires_at,reconnect_required,reasoning_effort)
 VALUES($1,$2,$3,$4,$5,$6,NULL,$7,$8,$9,$10,false,$11)
 ON CONFLICT(human_id,connection_id) DO UPDATE SET credential_ciphertext=EXCLUDED.credential_ciphertext,version=EXCLUDED.version,
 account_id=EXCLUDED.account_id,access_expires_at=EXCLUDED.access_expires_at,reconnect_required=false`,
		human, c.ID, c.Name, ChatGPTPreset, ChatGPTBaseURL, c.Model, sealed, uuid.NewString(), t.AccountID, t.ExpiresAt, c.ReasoningEffort)
	if err != nil {
		return Connection{}, err
	}
	if existing {
		// Reconnecting repairs this connection's sign-in; which connection
		// the person uses is a separate choice and stays as it was.
		return c, nil
	}
	// Signing in to add a connection is the person's explicit choice of it.
	_, err = tx.Exec(ctx, `INSERT INTO model_connection_selections(human_id,kind,connection_id) VALUES($1,'api',$2)
 ON CONFLICT(human_id) DO UPDATE SET kind=EXCLUDED.kind,connection_id=EXCLUDED.connection_id`, human, c.ID)
	if err != nil {
		return Connection{}, err
	}
	return c, nil
}
