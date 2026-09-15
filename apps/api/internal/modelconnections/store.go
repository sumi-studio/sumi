// Package modelconnections manages Human-owned API credentials. Secrets are write-only to browsers.
package modelconnections

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
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
	// MaxOutputTokens is the connection's requested bound on generated
	// tokens (Anthropic max_tokens / Responses max_output_tokens). Nil
	// means "use the protocol default": Anthropic sends the core's
	// default budget; Responses omits the field so the model's own cap
	// applies. Non-secret metadata — returned in list/read responses.
	MaxOutputTokens *int `json:"maxOutputTokens,omitempty"`
}
type Input struct {
	Name    string  `json:"name"`
	Preset  string  `json:"preset"`
	BaseURL string  `json:"baseUrl"`
	Model   string  `json:"model"`
	APIKey  *string `json:"apiKey,omitempty"`
	// MaxOutputTokens overrides the default output bound for models whose
	// cap differs from it. A model field is free text; without this knob a
	// connection pointed at a model whose output cap is below the default
	// would fail every request with a provider 400. Nil keeps the default.
	MaxOutputTokens *int `json:"maxOutputTokens,omitempty"`
	// ExtraHeaders are per-connection request headers sent only to this
	// connection's endpoint (gateway routing, provider betas). They are
	// sealed with the credential — a value may itself be secret material —
	// so they are write-only like the key and can only change together
	// with a resubmitted key.
	ExtraHeaders map[string]string `json:"extraHeaders,omitempty"`
}
type Selection struct {
	Kind         string `json:"kind"`
	ConnectionID string `json:"connectionId,omitempty"`
}
type Access struct {
	Connection   Connection        `json:"-"`
	APIKey       string            `json:"-"`
	ExtraHeaders map[string]string `json:"-"`
	Version      string            `json:"-"`
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

// headerNameRe is the RFC 7230 token grammar — the only shape a header
// name may take on any of the supported wires.
var headerNameRe = regexp.MustCompile(`^[!#$%&'*+\-.^_` + "`" + `|~0-9A-Za-z]+$`)

// reservedHeaders may not be overridden per connection: the request's own
// authentication, protocol-version, and transport framing fields stay
// adapter-controlled so a custom header can never silently replace the
// selected credential or corrupt the request.
var reservedHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"proxy-authenticate":  true,
	"www-authenticate":    true,
	"x-api-key":           true,
	"anthropic-version":   true,
	"content-type":        true,
	"content-length":      true,
	"host":                true,
	"connection":          true,
	"keep-alive":          true,
	"transfer-encoding":   true,
	"upgrade":             true,
	"te":                  true,
	"trailer":             true,
	"cookie":              true,
	"set-cookie":          true,
}

const (
	maxExtraHeaders     = 16
	maxExtraHeaderName  = 128
	maxExtraHeaderValue = 1024
	// maxOutputTokensBound is a sanity ceiling, not a model capability
	// claim: above every documented provider cap, it only rejects obvious
	// garbage. The endpoint remains the authority on what a model allows.
	maxOutputTokensBound = 1_000_000
)

// invalid is ErrInvalid with a public-safe reason: it may name the field
// or header the user supplied, never a stored credential or header value.
func invalid(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, reason)
}

func validateHeaders(headers map[string]string) error {
	if len(headers) > maxExtraHeaders {
		return invalid(fmt.Sprintf("at most %d extra headers per connection", maxExtraHeaders))
	}
	for name, value := range headers {
		if len(name) > maxExtraHeaderName {
			return invalid("extra header name exceeds 128 characters")
		}
		if !headerNameRe.MatchString(name) {
			return invalid(fmt.Sprintf("extra header name %q is not an RFC 7230 token", name))
		}
		if reservedHeaders[strings.ToLower(name)] {
			return invalid(fmt.Sprintf("extra header %q is reserved by the request itself", name))
		}
		if len(value) > maxExtraHeaderValue {
			return invalid(fmt.Sprintf("extra header %q value exceeds %d characters", name, maxExtraHeaderValue))
		}
		if strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return invalid(fmt.Sprintf("extra header %q value contains a control character", name))
		}
		// The fetch/undici Headers contract accepts only ByteString
		// values (each code point ≤ U+00FF): a wider character would
		// otherwise pass this check and then deterministically fail every
		// request inside fetch, misclassified as a transient outage.
		if strings.IndexFunc(value, func(r rune) bool { return r > 0xFF }) >= 0 {
			return invalid(fmt.Sprintf("extra header %q value contains a character outside Latin-1", name))
		}
	}
	return nil
}

// validateShape checks everything except the endpoint's transport rules
// (SaveUnchecked bypasses only the endpoint rules for loopback fixtures).
func validateShape(in Input) error {
	if !bounded(in.Name, 120) {
		return invalid("name is required (max 120 characters)")
	}
	if !bounded(in.Model, 128) {
		return invalid("model is required (max 128 characters)")
	}
	switch in.Preset {
	case "openai-chat", "openai-responses", "anthropic", "kimi-k3", "glm-5.2", "umans", "umans-kimi-k2.7", "opencode-go", "opencode-zen-go":
	default:
		return invalid(fmt.Sprintf("unsupported preset %q", in.Preset))
	}
	if in.APIKey != nil && (!bounded(*in.APIKey, 65536) || strings.TrimSpace(*in.APIKey) != *in.APIKey) {
		return invalid("API key is empty, too long, or has surrounding whitespace")
	}
	if in.MaxOutputTokens != nil {
		if *in.MaxOutputTokens < 1 || *in.MaxOutputTokens > maxOutputTokensBound {
			return invalid(fmt.Sprintf("maxOutputTokens must be between 1 and %d", maxOutputTokensBound))
		}
		// Only the Anthropic and Responses adapters put a bound on the
		// wire; on any other preset the saved value would do nothing.
		if in.Preset != "anthropic" && in.Preset != "openai-responses" {
			return invalid(fmt.Sprintf("maxOutputTokens has no effect on preset %q — only anthropic and openai-responses send an output bound", in.Preset))
		}
	}
	// Extra headers live inside the sealed credential: setting or clearing
	// them (present field, even an empty map) requires the key alongside.
	if in.ExtraHeaders != nil && in.APIKey == nil {
		return invalid("changing extra headers requires resubmitting the API key")
	}
	return validateHeaders(in.ExtraHeaders)
}

func validateEndpoint(baseURL string) error {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(baseURL) > 2048 {
		return invalid("base URL must be a public https URL without credentials, query, or fragment")
	}
	// Only the literal URL is checked: hostnames are never resolved, so a
	// public name may still point at a private address at request time.
	// Redirect refusal is enforced at the transport layer in the core's
	// provider adapters (redirect "manual" + 3xx rejection); private-range
	// egress for resolved addresses is deployment policy, not implemented
	// here or in the adapters.
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || (ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast())) {
		return invalid("base URL must be a public host")
	}
	return nil
}

func Validate(in Input) error {
	if err := validateShape(in); err != nil {
		return err
	}
	return validateEndpoint(in.BaseURL)
}

// credentialPayload is the sealed form: the API key plus any
// per-connection extra headers. Header values may themselves be secret
// (a gateway session token), so they live inside the ciphertext and are
// never exposed through metadata.
type credentialPayload struct {
	APIKey       string            `json:"api_key"`
	ExtraHeaders map[string]string `json:"extra_headers,omitempty"`
}

func (s *Store) seal(human, id string, p credentialPayload) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, ErrUnavailable
	}
	plaintext, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	aad, _ := json.Marshal([]string{"sumi.api.v1", human, id})
	return s.aead.Seal(nonce, nonce, plaintext, aad), nil
}
func (s *Store) open(human, id string, b []byte) (credentialPayload, error) {
	n := s.aead.NonceSize()
	if len(b) < n {
		return credentialPayload{}, ErrUnavailable
	}
	aad, _ := json.Marshal([]string{"sumi.api.v1", human, id})
	v, err := s.aead.Open(nil, b[:n], b[n:], aad)
	if err != nil {
		return credentialPayload{}, ErrUnavailable
	}
	var p credentialPayload
	if err := json.Unmarshal(v, &p); err != nil || p.APIKey == "" {
		return credentialPayload{}, ErrUnavailable
	}
	return p, nil
}
func lockHuman(ctx context.Context, tx pgx.Tx, human string) error {
	var id string
	return tx.QueryRow(ctx, "SELECT human_id::text FROM humans WHERE human_id=$1 FOR UPDATE", human).Scan(&id)
}
func (s *Store) Save(ctx context.Context, human, id string, in Input) (Connection, error) {
	if err := Validate(in); err != nil {
		return Connection{}, err
	}
	return s.save(ctx, human, id, in)
}

// SaveUnchecked is Save without endpoint transport validation — for
// dev/test harnesses (state-dev fixture seeding) that must point a
// connection at a loopback stub. Input shape (name/model/preset/key/
// headers) is still validated and the credential is still sealed through
// the armed store; an unarmed store refuses.
func (s *Store) SaveUnchecked(ctx context.Context, human, id string, in Input) (Connection, error) {
	if err := validateShape(in); err != nil {
		return Connection{}, err
	}
	return s.save(ctx, human, id, in)
}

func (s *Store) save(ctx context.Context, human, id string, in Input) (Connection, error) {
	if !s.CredentialsAvailable() {
		return Connection{}, ErrUnavailable
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
		return Connection{}, invalid("changing the base URL requires resubmitting the API key")
	}
	if in.APIKey != nil {
		headers := in.ExtraHeaders
		if headers == nil && !create {
			// "Omit the field to retain the stored headers" applies to a
			// key resubmission too: an explicit empty map clears, a nil
			// field carries the sealed set forward. A previous ciphertext
			// that cannot be opened fails honestly rather than guessing.
			prev, err := s.open(human, id, ciphertext)
			if err != nil {
				return Connection{}, err
			}
			headers = prev.ExtraHeaders
		}
		ciphertext, err = s.seal(human, id, credentialPayload{
			APIKey:       *in.APIKey,
			ExtraHeaders: headers,
		})
		if err != nil {
			return Connection{}, err
		}
	} else if create {
		return Connection{}, invalid("a new connection requires an API key")
	}
	// The binding identifies credential authority, not the display name or model.
	// Model-only edits can finish the current run with its existing model safely;
	// RuntimeFingerprint still schedules the new model for the next idle start.
	if create || in.APIKey != nil || previousURL != in.BaseURL || previousPreset != in.Preset {
		version = uuid.NewString()
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_api_connections(human_id,connection_id,name,preset,base_url,model,max_output_tokens,credential_ciphertext,version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(human_id,connection_id) DO UPDATE SET name=EXCLUDED.name,preset=EXCLUDED.preset,base_url=EXCLUDED.base_url,model=EXCLUDED.model,max_output_tokens=EXCLUDED.max_output_tokens,credential_ciphertext=EXCLUDED.credential_ciphertext,version=EXCLUDED.version`, human, id, in.Name, in.Preset, in.BaseURL, in.Model, in.MaxOutputTokens, ciphertext, version)
	if err != nil {
		return Connection{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Connection{}, err
	}
	return Connection{id, in.Name, in.Preset, in.BaseURL, in.Model, in.MaxOutputTokens}, nil
}
func (s *Store) List(ctx context.Context, human string) ([]Connection, error) {
	rows, err := s.pool.Query(ctx, "SELECT connection_id::text,name,preset,base_url,model,max_output_tokens FROM model_api_connections WHERE human_id=$1 ORDER BY name,connection_id", human)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Connection{}
	for rows.Next() {
		var c Connection
		if err = rows.Scan(&c.ID, &c.Name, &c.Preset, &c.BaseURL, &c.Model, &c.MaxOutputTokens); err != nil {
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
	err = s.pool.QueryRow(ctx, "SELECT connection_id::text,name,preset,base_url,model,max_output_tokens,credential_ciphertext,version::text FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", human, id).Scan(&a.Connection.ID, &a.Connection.Name, &a.Connection.Preset, &a.Connection.BaseURL, &a.Connection.Model, &a.Connection.MaxOutputTokens, &b, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	if err != nil {
		return a, err
	}
	cred, err := s.open(human, id, b)
	if err != nil {
		return a, err
	}
	a.APIKey = cred.APIKey
	a.ExtraHeaders = cred.ExtraHeaders
	return a, nil
}

// Metadata resolves the selected identity without decrypting its credential.
func (s *Store) Metadata(ctx context.Context, human, id string) (Access, error) {
	if !s.CredentialsAvailable() {
		return Access{}, ErrUnavailable
	}
	return s.Describe(ctx, human, id)
}

// Describe returns non-secret connection metadata (identity, preset,
// endpoint, model, version) without requiring the credential key — a
// metadata-only store can still describe the selection authoritatively.
func (s *Store) Describe(ctx context.Context, human, id string) (Access, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Access{}, ErrNotFound
	}
	var a Access
	err = s.pool.QueryRow(ctx, "SELECT connection_id::text,name,preset,base_url,model,max_output_tokens,version::text FROM model_api_connections WHERE human_id=$1 AND connection_id=$2", human, parsed.String()).Scan(&a.Connection.ID, &a.Connection.Name, &a.Connection.Preset, &a.Connection.BaseURL, &a.Connection.Model, &a.Connection.MaxOutputTokens, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// RuntimeFingerprint excludes display-only fields and inactive connections.
func (s *Store) RuntimeFingerprint(ctx context.Context, human string) (string, error) {
	var value string
	err := s.pool.QueryRow(ctx, `SELECT s.kind || ':' || COALESCE(s.connection_id::text,'') || ':' || COALESCE(c.version::text,'') || ':' || COALESCE(c.model,'') || ':' || COALESCE(c.max_output_tokens::text,'') FROM model_connection_selections s LEFT JOIN model_api_connections c ON c.human_id=s.human_id AND c.connection_id=s.connection_id WHERE s.human_id=$1`, human).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return value, err
}
