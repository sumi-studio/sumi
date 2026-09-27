// Package cloudbrowser keeps the canonical record of each secretary's
// Sumi-owned Cloud browser: its profile, incarnations, sealed semantic
// checkpoints and the optional user-scoped Jev key. The remote browser and
// its viewer run in the browser Worker; this package authorizes people,
// issues viewer tickets and serves the Worker's host routes.
package cloudbrowser

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalid     = errors.New("invalid cloud browser request")
	ErrUnavailable = errors.New("cloud browser unavailable or not authorized")
	ErrNotFound    = errors.New("cloud browser profile not found")
	ErrStale       = errors.New("cloud browser incarnation or checkpoint is stale")
)

// SnapshotVersion is the checkpoint serializer this API accepts and returns.
// A checkpoint sealed under another version is never handed to a host.
const SnapshotVersion = 1

// MaxSnapshotBytes bounds one checkpoint's plaintext (cookies, per-origin
// localStorage and JSON-able IndexedDB records, tab list).
const MaxSnapshotBytes = 2 << 20

// TabProfile is the fixed TabRef.profileId of every Cloud browser tab. The
// TabRef.runtimeId is the stable profile id and TabRef.tabId the stable tab
// slot, so a standing grant survives checkpoint restoration.
const TabProfile = "cloud"

type Profile struct {
	ID               string     `json:"profile_id"`
	PersonaID        string     `json:"persona_id"`
	PersonaName      string     `json:"persona_name"`
	State            string     `json:"state"`
	StateAt          time.Time  `json:"state_at"`
	Incarnation      int64      `json:"incarnation"`
	TabIDs           []string   `json:"tab_ids"`
	CheckpointAt     *time.Time `json:"checkpoint_at"`
	CheckpointBytes  *int       `json:"checkpoint_bytes"`
	Grants           []Grant    `json:"grants"`
	humanID          string
	snapshotIncarn   *int64
	snapshotVersion  *int
	snapshotSequence int64
}

type Grant struct {
	AttachmentID string `json:"attachment_id"`
	TabID        string `json:"tab_id"`
	Name         string `json:"name"`
	AllowActions bool   `json:"allow_actions"`
}

type Persona struct {
	ID   string `json:"persona_id"`
	Name string `json:"name"`
}

type JevStatus struct {
	Configured bool       `json:"configured"`
	Rejected   bool       `json:"rejected"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
}

// HostCredential is one enabled grant with a freshly rotated tab host token.
// Only the browser Worker receives it, over the runtime-token channel.
type HostCredential struct {
	Attachment struct {
		ID           string         `json:"attachment_id"`
		PersonaID    string         `json:"persona_id"`
		Name         string         `json:"name"`
		Tab          map[string]any `json:"tab"`
		AllowActions bool           `json:"allow_actions"`
		Available    bool           `json:"available"`
	} `json:"attachment"`
	HostToken string `json:"host_token"`
}

type HostSession struct {
	ProfileID   string           `json:"profile_id"`
	PersonaID   string           `json:"persona_id"`
	Enabled     bool             `json:"enabled"`
	Incarnation int64            `json:"incarnation"`
	Attachments []HostCredential `json:"attachments"`
	// Snapshot is the plaintext checkpoint (begin only); nil when none
	// exists or it was sealed under another serializer version.
	Snapshot        json.RawMessage `json:"snapshot,omitempty"`
	SnapshotSeq     int64           `json:"snapshot_seq"`
	SnapshotAt      *time.Time      `json:"snapshot_at,omitempty"`
	SnapshotVersion int             `json:"snapshot_version"`
	JevKey          string          `json:"jev_key,omitempty"`
	// JevKeyVersion names the stored key JevKey is (its save time in
	// microseconds); a rejection is recorded only against that version.
	JevKeyVersion int64 `json:"jev_key_version,omitempty"`
}

type Store struct {
	Pool *pgxpool.Pool
	aead cipher.AEAD
	// ticketKey signs viewer tickets verified by the browser Worker.
	ticketKey []byte
	now       func() time.Time
}

// New derives separate sealing and ticket keys: the checkpoint/Jev sealing
// key from the model-connection master key, the ticket key from the shared
// runtime token the browser Worker also holds.
func New(pool *pgxpool.Pool, masterKey []byte, runtimeToken string) (*Store, error) {
	if pool == nil || len(masterKey) != 32 || len(runtimeToken) < 32 {
		return nil, ErrInvalid
	}
	block, err := aes.NewCipher(derive(masterKey, "sumi.cloud-browser.seal.v1"))
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: pool, aead: aead, ticketKey: derive([]byte(runtimeToken), "sumi.cloud-browser.viewer-ticket.v1"), now: time.Now}, nil
}

func derive(key []byte, label string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(label))
	return m.Sum(nil)
}

func (s *Store) seal(domain, human, id string, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, ErrUnavailable
	}
	aad, _ := json.Marshal([]string{domain, human, id})
	return s.aead.Seal(nonce, nonce, plaintext, aad), nil
}

func (s *Store) open(domain, human, id string, b []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(b) < n {
		return nil, ErrUnavailable
	}
	aad, _ := json.Marshal([]string{domain, human, id})
	v, err := s.aead.Open(nil, b[:n], b[n:], aad)
	if err != nil {
		return nil, ErrUnavailable
	}
	return v, nil
}

func snapshotDomain(version int) string {
	return fmt.Sprintf("sumi.cloud-browser.snapshot.v%d", version)
}

const jevDomain = "sumi.cloud-browser.jev.v1"

func validUUID(id string) (string, bool) {
	u, err := uuid.Parse(id)
	if err != nil {
		return "", false
	}
	return u.String(), true
}

// Personas lists the human's active secretaries (the owners a profile can belong to).
func (s *Store) Personas(ctx context.Context, human string) ([]Persona, error) {
	rows, err := s.Pool.Query(ctx, `SELECT persona_id::text,display_name FROM core_personas WHERE human_id=$1 AND authority='active' ORDER BY created_at LIMIT 20`, human)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Persona{}
	for rows.Next() {
		var p Persona
		if err := rows.Scan(&p.ID, &p.Name); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

const profileColumns = `c.profile_id::text,c.persona_id::text,p.display_name,c.state,c.state_at,c.incarnation,c.tab_ids,c.snapshot_at,c.snapshot_bytes,c.human_id::text,c.snapshot_incarnation,c.snapshot_version,c.snapshot_seq`

func scanProfile(row pgx.Row) (Profile, error) {
	var p Profile
	err := row.Scan(&p.ID, &p.PersonaID, &p.PersonaName, &p.State, &p.StateAt, &p.Incarnation, &p.TabIDs, &p.CheckpointAt, &p.CheckpointBytes, &p.humanID, &p.snapshotIncarn, &p.snapshotVersion, &p.snapshotSequence)
	if p.TabIDs == nil {
		p.TabIDs = []string{}
	}
	p.Grants = []Grant{}
	return p, err
}

// Profiles lists the human's enabled profiles whose secretary is still active.
func (s *Store) Profiles(ctx context.Context, human string) ([]Profile, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+profileColumns+` FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id AND p.human_id=c.human_id WHERE c.human_id=$1 AND c.enabled AND p.authority='active' ORDER BY c.created_at LIMIT 20`, human)
	if err != nil {
		return nil, err
	}
	out := []Profile{}
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		grants, err := s.grants(ctx, s.Pool, human, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].Grants = grants
	}
	return out, nil
}

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) grants(ctx context.Context, q querier, human, profile string) ([]Grant, error) {
	rows, err := q.Query(ctx, `SELECT attachment_id::text,tab->>'tabId',name,allow_actions FROM browser_tab_attachments WHERE human_id=$1 AND enabled AND tab->>'profileId'=$2 AND tab->>'runtimeId'=$3 ORDER BY created_at`, human, TabProfile, profile)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Grant{}
	for rows.Next() {
		var g Grant
		if err := rows.Scan(&g.AttachmentID, &g.TabID, &g.Name, &g.AllowActions); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Profile reads one profile the human owns through a still-active secretary.
func (s *Store) Profile(ctx context.Context, human, id string) (Profile, error) {
	id, ok := validUUID(id)
	if !ok {
		return Profile{}, ErrNotFound
	}
	p, err := scanProfile(s.Pool.QueryRow(ctx, `SELECT `+profileColumns+` FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id AND p.human_id=c.human_id WHERE c.profile_id=$1 AND c.human_id=$2 AND c.enabled AND p.authority='active'`, id, human))
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, err
	}
	p.Grants, err = s.grants(ctx, s.Pool, human, p.ID)
	return p, err
}

// CreateProfile gives an active secretary its browser profile. It is
// idempotent per secretary: the enabled profile is returned if one exists.
// No cookies or storage from the person's own browsers are imported.
func (s *Store) CreateProfile(ctx context.Context, human, persona string) (Profile, error) {
	persona, ok := validUUID(persona)
	if !ok {
		return Profile{}, ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Profile{}, err
	}
	defer tx.Rollback(ctx)
	var owner string
	err = tx.QueryRow(ctx, `SELECT human_id::text FROM core_personas WHERE persona_id=$1 AND authority='active' FOR SHARE`, persona).Scan(&owner)
	if err != nil || owner != human {
		return Profile{}, ErrUnavailable
	}
	var id string
	err = tx.QueryRow(ctx, `SELECT profile_id::text FROM cloud_browser_profiles WHERE persona_id=$1 AND enabled FOR UPDATE`, persona).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		id = uuid.NewString()
		_, err = tx.Exec(ctx, `INSERT INTO cloud_browser_profiles(profile_id,human_id,persona_id)VALUES($1,$2,$3)`, id, human, persona)
	}
	if err != nil {
		return Profile{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Profile{}, err
	}
	return s.Profile(ctx, human, id)
}

// ResetProfile disables the profile, forgets its checkpoint and revokes its
// grants. The Worker is woken to close any live browser; the next profile
// for the secretary starts empty.
func (s *Store) ResetProfile(ctx context.Context, human, id string) error {
	id, ok := validUUID(id)
	if !ok {
		return ErrNotFound
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET enabled=false,snapshot=NULL,snapshot_bytes=NULL,tab_ids='[]',refresh_requested_at=now() WHERE profile_id=$1 AND human_id=$2 AND enabled`, id, human)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx, `UPDATE browser_tab_attachments SET enabled=false WHERE human_id=$1 AND enabled AND tab->>'profileId'=$2 AND tab->>'runtimeId'=$3`, human, TabProfile, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Grant gives the profile's secretary a standing grant to one stable tab
// slot. The grant's host token is not returned to anyone: the browser Worker
// receives a freshly rotated token when it next starts or refreshes.
func (s *Store) Grant(ctx context.Context, human, profileID, tabID, name string, allowActions bool) (Grant, error) {
	profileID, ok := validUUID(profileID)
	if !ok {
		return Grant{}, ErrNotFound
	}
	tabID, ok = validUUID(tabID)
	name = strings.TrimSpace(name)
	if !ok || name == "" || len(name) > 120 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return Grant{}, ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer tx.Rollback(ctx)
	var persona string
	err = tx.QueryRow(ctx, `SELECT c.persona_id::text FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id AND p.human_id=c.human_id WHERE c.profile_id=$1 AND c.human_id=$2 AND c.enabled AND p.authority='active' FOR UPDATE OF c FOR SHARE OF p`, profileID, human).Scan(&persona)
	if errors.Is(err, pgx.ErrNoRows) {
		return Grant{}, ErrNotFound
	}
	if err != nil {
		return Grant{}, err
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return Grant{}, err
	}
	digest := sha256.Sum256(token)
	g := Grant{AttachmentID: uuid.NewString(), TabID: tabID, Name: name, AllowActions: allowActions}
	tab := map[string]string{"runtimeId": profileID, "profileId": TabProfile, "tabId": tabID}
	// One enabled grant per exact tab (browser_tab_identity): changing the
	// action permission replaces the grant.
	if _, err := tx.Exec(ctx, `UPDATE browser_tab_attachments SET enabled=false WHERE human_id=$1 AND enabled AND tab->>'runtimeId'=$2 AND tab->>'tabId'=$3`, human, profileID, tabID); err != nil {
		return Grant{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO browser_tab_attachments(attachment_id,human_id,persona_id,name,tab,host_token_hash,allow_actions)VALUES($1,$2,$3,$4,$5,$6,$7)`, g.AttachmentID, human, persona, name, tab, digest[:], allowActions); err != nil {
		return Grant{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=now() WHERE profile_id=$1`, profileID); err != nil {
		return Grant{}, err
	}
	return g, tx.Commit(ctx)
}

// Revoke ends a Cloud tab grant. Future dispatch stops at the API; an
// action the host already admitted may still complete and is recorded.
func (s *Store) Revoke(ctx context.Context, human, profileID, attachmentID string) error {
	profileID, ok := validUUID(profileID)
	attachmentID, ok2 := validUUID(attachmentID)
	if !ok || !ok2 {
		return ErrNotFound
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE browser_tab_attachments SET enabled=false WHERE attachment_id=$1 AND human_id=$2 AND enabled AND tab->>'profileId'=$3 AND tab->>'runtimeId'=$4`, attachmentID, human, TabProfile, profileID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// ---------- Jev key (BYOK) ----------

func (s *Store) JevStatus(ctx context.Context, human string) (JevStatus, error) {
	var st JevStatus
	var at time.Time
	err := s.Pool.QueryRow(ctx, `SELECT rejected,updated_at FROM cloud_browser_jev_credentials WHERE human_id=$1`, human).Scan(&st.Rejected, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return JevStatus{}, nil
	}
	if err != nil {
		return JevStatus{}, err
	}
	st.Configured = true
	st.UpdatedAt = &at
	return st, nil
}

func validJevKey(key string) bool {
	return len(key) >= 8 && len(key) <= 512 && strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || r > unicode.MaxASCII }) < 0
}

// SetJevKey stores the human's Jev key sealed. It is write-only to people.
func (s *Store) SetJevKey(ctx context.Context, human, key string) (JevStatus, error) {
	if !validJevKey(key) {
		return JevStatus{}, ErrInvalid
	}
	sealed, err := s.seal(jevDomain, human, human, []byte(key))
	if err != nil {
		return JevStatus{}, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return JevStatus{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO cloud_browser_jev_credentials(human_id,sealed)VALUES($1,$2) ON CONFLICT(human_id) DO UPDATE SET sealed=EXCLUDED.sealed,rejected=false,updated_at=now()`, human, sealed); err != nil {
		return JevStatus{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=now() WHERE human_id=$1 AND enabled`, human); err != nil {
		return JevStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return JevStatus{}, err
	}
	return s.JevStatus(ctx, human)
}

func (s *Store) DeleteJevKey(ctx context.Context, human string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM cloud_browser_jev_credentials WHERE human_id=$1`, human); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=now() WHERE human_id=$1 AND enabled`, human); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// jevKeyVersionSQL is a stored key's version: its save time in microseconds.
const jevKeyVersionSQL = `(extract(epoch FROM updated_at)*1000000)::bigint`

func (s *Store) jevKey(ctx context.Context, q querier, human string) (string, int64, error) {
	var sealed []byte
	var rejected bool
	var version int64
	err := q.QueryRow(ctx, `SELECT sealed,rejected,`+jevKeyVersionSQL+` FROM cloud_browser_jev_credentials WHERE human_id=$1`, human).Scan(&sealed, &rejected, &version)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && rejected {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	key, err := s.open(jevDomain, human, human, sealed)
	if err != nil {
		// A key sealed under another master key is unusable, not fatal:
		// the direct path continues without Jev.
		return "", 0, nil
	}
	return string(key), version, nil
}

// ---------- viewer tickets ----------

// Ticket is what a viewer presents to the browser Worker. It names the
// authenticated owner, the secretary, the profile, the checkpoint serializer
// and a short expiry; the Worker verifies the signature with the shared key.
type Ticket struct {
	Version   int    `json:"v"`
	HumanID   string `json:"h"`
	PersonaID string `json:"p"`
	ProfileID string `json:"b"`
	Serial    int    `json:"s"`
	Expires   int64  `json:"e"`
	Nonce     string `json:"n"`
}

const ticketTTL = 60 * time.Second

func (s *Store) IssueTicket(ctx context.Context, human, profileID string) (string, time.Time, error) {
	p, err := s.Profile(ctx, human, profileID)
	if err != nil {
		return "", time.Time{}, err
	}
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return "", time.Time{}, err
	}
	exp := s.now().Add(ticketTTL)
	body, _ := json.Marshal(Ticket{Version: 1, HumanID: human, PersonaID: p.PersonaID, ProfileID: p.ID, Serial: SnapshotVersion, Expires: exp.UnixMilli(), Nonce: base64.RawURLEncoding.EncodeToString(nonce)})
	payload := base64.RawURLEncoding.EncodeToString(body)
	m := hmac.New(sha256.New, s.ticketKey)
	m.Write([]byte(payload))
	return "sbt1." + payload + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil)), exp, nil
}

// ---------- host (browser Worker) side ----------

// Begin starts a host session for the profile. With fresh=true the Worker
// is about to create a new remote browser: the incarnation increments,
// every grant's host token rotates, and the latest checkpoint is returned.
// With fresh=false (the Worker reconnected to a still-live browser of
// `incarnation`) only the tokens rotate; a mismatched incarnation is stale.
func (s *Store) Begin(ctx context.Context, profileID string, fresh bool, incarnation int64) (HostSession, error) {
	profileID, ok := validUUID(profileID)
	if !ok {
		return HostSession{}, ErrNotFound
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return HostSession{}, err
	}
	defer tx.Rollback(ctx)
	p, err := scanProfile(tx.QueryRow(ctx, `SELECT `+profileColumns+` FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id WHERE c.profile_id=$1 FOR UPDATE OF c`, profileID))
	if errors.Is(err, pgx.ErrNoRows) {
		return HostSession{}, ErrNotFound
	}
	if err != nil {
		return HostSession{}, err
	}
	var enabled bool
	var active string
	if err := tx.QueryRow(ctx, `SELECT c.enabled,p.authority FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id WHERE c.profile_id=$1`, profileID).Scan(&enabled, &active); err != nil {
		return HostSession{}, err
	}
	out := HostSession{ProfileID: p.ID, PersonaID: p.PersonaID, Enabled: enabled && active == "active", SnapshotVersion: SnapshotVersion, Attachments: []HostCredential{}}
	if !out.Enabled {
		// A reset profile or retired secretary: the Worker closes the browser.
		if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=NULL,state='sleeping',state_at=now() WHERE profile_id=$1`, profileID); err != nil {
			return HostSession{}, err
		}
		return out, tx.Commit(ctx)
	}
	if fresh {
		p.Incarnation++
		if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET incarnation=$2,state='live',state_at=now(),refresh_requested_at=NULL WHERE profile_id=$1`, profileID, p.Incarnation); err != nil {
			return HostSession{}, err
		}
	} else {
		if incarnation != p.Incarnation {
			return HostSession{}, ErrStale
		}
		if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET state='live',state_at=now(),refresh_requested_at=NULL WHERE profile_id=$1`, profileID); err != nil {
			return HostSession{}, err
		}
	}
	out.Incarnation = p.Incarnation
	out.Attachments, err = s.rotate(ctx, tx, p, nil)
	if err != nil {
		return HostSession{}, err
	}
	// The checkpoint is returned on reconnect too: the host restores it only
	// for a new browser, but needs it to carry origins without an open tab
	// and to continue the sequence.
	out.SnapshotSeq = p.snapshotSequence
	if p.snapshotVersion != nil && *p.snapshotVersion == SnapshotVersion {
		var sealed []byte
		if err := tx.QueryRow(ctx, `SELECT snapshot FROM cloud_browser_profiles WHERE profile_id=$1`, profileID).Scan(&sealed); err != nil {
			return HostSession{}, err
		}
		if sealed != nil {
			plain, err := s.open(snapshotDomain(SnapshotVersion), p.humanID, p.ID, sealed)
			if err == nil {
				out.Snapshot = plain
				out.SnapshotAt = p.CheckpointAt
			}
		}
	}
	if out.JevKey, out.JevKeyVersion, err = s.jevKey(ctx, tx, p.humanID); err != nil {
		return HostSession{}, err
	}
	return out, tx.Commit(ctx)
}

// Refresh delivers grants the live host does not know yet (rotating only
// those tokens, so receipts of in-flight jobs on known grants still land),
// the current Jev key, and whether the profile is still enabled.
func (s *Store) Refresh(ctx context.Context, profileID string, incarnation int64, known []string) (HostSession, error) {
	profileID, ok := validUUID(profileID)
	if !ok || len(known) > 64 {
		return HostSession{}, ErrNotFound
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return HostSession{}, err
	}
	defer tx.Rollback(ctx)
	p, err := scanProfile(tx.QueryRow(ctx, `SELECT `+profileColumns+` FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id WHERE c.profile_id=$1 FOR UPDATE OF c`, profileID))
	if errors.Is(err, pgx.ErrNoRows) {
		return HostSession{}, ErrNotFound
	}
	if err != nil {
		return HostSession{}, err
	}
	if p.Incarnation != incarnation {
		return HostSession{}, ErrStale
	}
	var enabled bool
	var active string
	if err := tx.QueryRow(ctx, `SELECT c.enabled,p.authority FROM cloud_browser_profiles c JOIN core_personas p ON p.persona_id=c.persona_id WHERE c.profile_id=$1`, profileID).Scan(&enabled, &active); err != nil {
		return HostSession{}, err
	}
	out := HostSession{ProfileID: p.ID, PersonaID: p.PersonaID, Enabled: enabled && active == "active", Incarnation: p.Incarnation, SnapshotVersion: SnapshotVersion, Attachments: []HostCredential{}}
	if _, err := tx.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=NULL WHERE profile_id=$1`, profileID); err != nil {
		return HostSession{}, err
	}
	if !out.Enabled {
		return out, tx.Commit(ctx)
	}
	skip := map[string]bool{}
	for _, id := range known {
		skip[id] = true
	}
	if out.Attachments, err = s.rotate(ctx, tx, p, skip); err != nil {
		return HostSession{}, err
	}
	if out.JevKey, out.JevKeyVersion, err = s.jevKey(ctx, tx, p.humanID); err != nil {
		return HostSession{}, err
	}
	return out, tx.Commit(ctx)
}

func (s *Store) rotate(ctx context.Context, tx pgx.Tx, p Profile, skip map[string]bool) ([]HostCredential, error) {
	rows, err := tx.Query(ctx, `SELECT attachment_id::text,persona_id::text,name,tab,allow_actions FROM browser_tab_attachments WHERE human_id=$1 AND persona_id=$2 AND enabled AND tab->>'profileId'=$3 AND tab->>'runtimeId'=$4 ORDER BY created_at LIMIT 64 FOR UPDATE`, p.humanID, p.PersonaID, TabProfile, p.ID)
	if err != nil {
		return nil, err
	}
	creds := []HostCredential{}
	for rows.Next() {
		var c HostCredential
		if err := rows.Scan(&c.Attachment.ID, &c.Attachment.PersonaID, &c.Attachment.Name, &c.Attachment.Tab, &c.Attachment.AllowActions); err != nil {
			rows.Close()
			return nil, err
		}
		if !skip[c.Attachment.ID] {
			c.Attachment.Available = true
			creds = append(creds, c)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range creds {
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return nil, err
		}
		token := "browser_" + base64.RawURLEncoding.EncodeToString(raw)
		digest := sha256.Sum256([]byte(token))
		if _, err := tx.Exec(ctx, `UPDATE browser_tab_attachments SET host_token_hash=$2 WHERE attachment_id=$1`, creds[i].Attachment.ID, digest[:]); err != nil {
			return nil, err
		}
		creds[i].HostToken = token
	}
	return creds, nil
}

// SaveSnapshot records a semantic checkpoint from the profile's current
// incarnation. Checkpoints from an older incarnation or with a sequence not
// above the stored one are refused as stale.
func (s *Store) SaveSnapshot(ctx context.Context, profileID string, incarnation, seq int64, version int, tabIDs []string, payload json.RawMessage) (time.Time, error) {
	profileID, ok := validUUID(profileID)
	if !ok {
		return time.Time{}, ErrNotFound
	}
	if version != SnapshotVersion || len(payload) == 0 || len(payload) > MaxSnapshotBytes || !json.Valid(payload) || len(tabIDs) > 32 {
		return time.Time{}, ErrInvalid
	}
	for i, id := range tabIDs {
		canonical, ok := validUUID(id)
		if !ok {
			return time.Time{}, ErrInvalid
		}
		tabIDs[i] = canonical
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx)
	var human string
	var current, stored int64
	var enabled bool
	err = tx.QueryRow(ctx, `SELECT human_id::text,incarnation,snapshot_seq,enabled FROM cloud_browser_profiles WHERE profile_id=$1 FOR UPDATE`, profileID).Scan(&human, &current, &stored, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrNotFound
	}
	if err != nil {
		return time.Time{}, err
	}
	if !enabled || incarnation != current || seq <= stored {
		return time.Time{}, ErrStale
	}
	sealed, err := s.seal(snapshotDomain(version), human, profileID, payload)
	if err != nil {
		return time.Time{}, err
	}
	var at time.Time
	if err := tx.QueryRow(ctx, `UPDATE cloud_browser_profiles SET snapshot=$2,snapshot_seq=$3,snapshot_version=$4,snapshot_incarnation=$5,snapshot_bytes=$6,snapshot_at=now(),tab_ids=$7 WHERE profile_id=$1 RETURNING snapshot_at`, profileID, sealed, seq, version, incarnation, len(payload), tabIDs).Scan(&at); err != nil {
		return time.Time{}, err
	}
	return at, tx.Commit(ctx)
}

// SetState records live/sleeping/lost for the person's UI; stale reports
// from an older incarnation are ignored.
func (s *Store) SetState(ctx context.Context, profileID string, incarnation int64, state string) error {
	profileID, ok := validUUID(profileID)
	if !ok || (state != "sleeping" && state != "live" && state != "lost") {
		return ErrInvalid
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE cloud_browser_profiles SET state=$3,state_at=now() WHERE profile_id=$1 AND incarnation=$2`, profileID, incarnation, state)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrStale
	}
	return err
}

// JevRejected marks the human's Jev key rejected after Jev refused it, so
// browser.tabs stops offering the Jev layer until the person replaces it.
// Only the refused version is marked: a key saved since then is untouched
// (the result is false).
func (s *Store) JevRejected(ctx context.Context, profileID string, version int64) (bool, error) {
	profileID, ok := validUUID(profileID)
	if !ok || version <= 0 {
		return false, ErrNotFound
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE cloud_browser_jev_credentials SET rejected=true WHERE human_id=(SELECT human_id FROM cloud_browser_profiles WHERE profile_id=$1) AND `+jevKeyVersionSQL+`=$2`, profileID, version)
	return err == nil && tag.RowsAffected() > 0, err
}

// DropRefresh clears a pending grant/key delivery that the browser Worker
// answered without a running browser (its next start receives everything).
// A request newer than `asOf` (the one the wake was sent for) stays pending.
func (s *Store) DropRefresh(ctx context.Context, profileID string, asOf time.Time) error {
	profileID, ok := validUUID(profileID)
	if !ok {
		return ErrNotFound
	}
	_, err := s.Pool.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=NULL WHERE profile_id=$1 AND refresh_requested_at<=$2`, profileID, asOf)
	return err
}

// Wake names a profile the browser Worker must hear about. Work means a
// browser job is queued on one of its Cloud tabs (the Worker starts or
// restores the browser); otherwise a live host has grants, credentials or a
// reset it has not received yet.
type Wake struct {
	ProfileID string `json:"profile_id"`
	Work      bool   `json:"work"`
	// Refresh: grant or key changes wait for a running browser.
	Refresh bool `json:"refresh"`
	// RefreshAt is the pending request's time (DropRefresh clears only it).
	RefreshAt *time.Time `json:"-"`
}

// WakeCandidates lists profiles to wake. Refresh requests for a profile
// without a live host are dropped: its next start receives everything.
func (s *Store) WakeCandidates(ctx context.Context, limit int) ([]Wake, error) {
	if _, err := s.Pool.Exec(ctx, `UPDATE cloud_browser_profiles SET refresh_requested_at=NULL WHERE refresh_requested_at IS NOT NULL AND state<>'live'`); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT profile_id,work,refresh,refresh_at FROM (SELECT c.profile_id::text AS profile_id,
 c.enabled AND EXISTS(SELECT 1 FROM core_jobs j JOIN browser_tab_attachments b ON b.attachment_id::text=j.request->>'attachment_id' WHERE j.kind='browser' AND j.status='queued' AND b.enabled AND b.tab->>'profileId'=$2 AND b.tab->>'runtimeId'=c.profile_id::text AND b.persona_id=j.persona_id) AS work,
 c.refresh_requested_at IS NOT NULL AS refresh, c.refresh_requested_at AS refresh_at
 FROM cloud_browser_profiles c WHERE c.enabled OR c.refresh_requested_at IS NOT NULL) w WHERE work OR refresh ORDER BY profile_id LIMIT $1`, limit, TabProfile)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Wake{}
	for rows.Next() {
		var w Wake
		if err := rows.Scan(&w.ProfileID, &w.Work, &w.Refresh, &w.RefreshAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
