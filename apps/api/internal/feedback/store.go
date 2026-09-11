package feedback

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

type Store struct {
	pool       *pgxpool.Pool
	recipients []participant.Ref
}

func New(pool *pgxpool.Pool, recipients []participant.Ref) *Store {
	return &Store{pool: pool, recipients: slices.Clone(recipients)}
}
func (s *Store) isRecipient(actor participant.Ref) bool { return slices.Contains(s.recipients, actor) }
func (s *Store) available(ctx context.Context, q participant.QueryRower) (bool, error) {
	if len(s.recipients) == 0 {
		return false, nil
	}
	for _, ref := range s.recipients {
		ok, err := participant.Exists(ctx, q, ref)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}
func (s *Store) Bootstrap(ctx context.Context, actor participant.Ref) (Bootstrap, error) {
	b := Bootstrap{RecipientName: "Sumi開発", Participant: wire(actor), IsRecipient: s.isRecipient(actor)}
	if actor.Validate() != nil {
		return b, ErrInvalid
	}
	var err error
	b.Available, err = s.available(ctx, s.pool)
	if err != nil {
		return b, err
	}
	err = s.pool.QueryRow(ctx, `SELECT installation_id, enabled FROM app_installations WHERE owner_kind=$1 AND owner_id=$2 AND app_id='feedback'`, actor.Kind, actor.ID).Scan(&b.InstallationID, &b.Enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, nil
	}
	b.Installed = err == nil
	return b, err
}
func (s *Store) begin(ctx context.Context, actor participant.Ref) (pgx.Tx, error) {
	if actor.Validate() != nil {
		return nil, ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var enabled bool
	err = tx.QueryRow(ctx, `SELECT enabled FROM app_installations WHERE owner_kind=$1 AND owner_id=$2 AND app_id='feedback' FOR SHARE`, actor.Kind, actor.ID).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrInstallation
	}
	if err == nil && !enabled {
		err = ErrDisabled
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}
func author(ctx context.Context, q participant.QueryRower, actor participant.Ref) (Author, error) {
	a := Author{Participant: wire(actor)}
	query := `SELECT display_name FROM humans WHERE human_id=$1`
	if actor.Kind == participant.KindPersonalityAgent {
		query = `SELECT display_name FROM agents WHERE personality_agent_id=$1`
	}
	err := q.QueryRow(ctx, query, actor.ID).Scan(&a.DisplayName)
	return a, err
}

const threadColumns = `t.thread_id,t.title,t.body,t.status,t.author,t.created_at,t.updated_at,t.revision,t.diagnostics,
 COALESCE((SELECT r.revision FROM feedback_reads r WHERE r.thread_id=t.thread_id AND r.reader_key=$1),0)<t.revision,
 (SELECT jsonb_build_object('id',e.event_id::text,'author',e.author,'body',e.body,'created_at',e.created_at,'revision',e.revision)
 FROM feedback_events e WHERE e.thread_id=t.thread_id AND e.kind='message' ORDER BY e.revision DESC LIMIT 1)`

func scanThread(row pgx.Row) (Thread, error) {
	var t Thread
	err := row.Scan(&t.ID, &t.Title, &t.Body, &t.Status, &t.Author, &t.CreatedAt, &t.UpdatedAt, &t.Revision, &t.Diagnostics, &t.Unread, &t.LatestMessage)
	return t, err
}
func (s *Store) thread(ctx context.Context, tx pgx.Tx, actor participant.Ref, id string, lock bool) (Thread, error) {
	if !validID(id, 7) {
		return Thread{}, ErrNotFound
	}
	query := `SELECT ` + threadColumns + ` FROM feedback_threads t WHERE t.thread_id=$2 AND (t.author_key=$1 OR $3)`
	if lock {
		query += ` FOR UPDATE OF t`
	}
	t, err := scanThread(tx.QueryRow(ctx, query, actor.Key(), id, s.isRecipient(actor)))
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	if err != nil {
		return t, err
	}
	return t, nil
}

type listCursor struct {
	Time time.Time `json:"t"`
	ID   string    `json:"i"`
}

func (s *Store) List(ctx context.Context, actor participant.Ref, status, cursor string) (ThreadList, error) {
	result := ThreadList{Threads: []Thread{}}
	if status == "" {
		status = "all"
	}
	if status != "all" && status != "open" && status != "resolved" {
		return result, ErrInvalid
	}
	c := listCursor{Time: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), ID: "ffffffff-ffff-7fff-bfff-ffffffffffff"}
	if cursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(data, &c) != nil || !validID(c.ID, 7) || c.Time.IsZero() {
			return result, ErrInvalid
		}
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT `+threadColumns+` FROM feedback_threads t WHERE (t.author_key=$1 OR $2) AND ($3='all' OR t.status=$3) AND (t.updated_at,t.thread_id)<($4,$5::uuidv7) ORDER BY t.updated_at DESC,t.thread_id DESC LIMIT 51`, actor.Key(), s.isRecipient(actor), status, c.Time, c.ID)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		t, e := scanThread(rows)
		if e != nil {
			rows.Close()
			return result, e
		}
		t.Diagnostics = nil // Full diagnostic context belongs to the opened thread, not list summaries.
		result.Threads = append(result.Threads, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	if len(result.Threads) > pageSize {
		result.Threads = result.Threads[:pageSize]
		last := result.Threads[pageSize-1]
		data, _ := json.Marshal(listCursor{last.UpdatedAt, last.ID})
		next := base64.RawURLEncoding.EncodeToString(data)
		result.NextCursor = &next
	}
	for i := range result.Threads {
		result.Threads[i].IsSummary = true
		result.Threads[i].Body = excerpt(result.Threads[i].Body)
		if result.Threads[i].LatestMessage != nil {
			result.Threads[i].LatestMessage.Body = excerpt(result.Threads[i].LatestMessage.Body)
		}
	}
	return result, tx.Commit(ctx)
}
func (s *Store) Open(ctx context.Context, actor participant.Ref, id, cursor string) (Detail, error) {
	result := Detail{Messages: []Message{}, Activities: []Activity{}}
	before := int64(9223372036854775807)
	if cursor != "" {
		var err error
		before, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || before <= 1 {
			return result, ErrInvalid
		}
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	// A share lock gives thread revision and event page one consistent boundary.
	result.Thread, err = s.thread(ctx, tx, actor, id, true)
	if err != nil {
		return result, err
	}
	rows, err := tx.Query(ctx, `SELECT event_id,kind,author,COALESCE(body,''),COALESCE(status,''),created_at,revision FROM feedback_events WHERE thread_id=$1 AND revision<$2 ORDER BY revision DESC LIMIT 51`, id, before)
	if err != nil {
		return result, err
	}
	count := 0
	last := int64(0)
	for rows.Next() {
		var eid, kind, body, status string
		var a Author
		var at time.Time
		var rev int64
		if err = rows.Scan(&eid, &kind, &a, &body, &status, &at, &rev); err != nil {
			rows.Close()
			return result, err
		}
		count++
		if count > pageSize {
			next := strconv.FormatInt(last, 10)
			result.NextCursor = &next
			break
		}
		last = rev
		if kind == "message" {
			result.Messages = append(result.Messages, Message{eid, a, body, at, rev})
		} else {
			result.Activities = append(result.Activities, Activity{eid, a, status, at, rev})
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	slices.Reverse(result.Messages)
	slices.Reverse(result.Activities)
	return result, tx.Commit(ctx)
}
func requestFingerprint(parts ...string) string {
	data, _ := json.Marshal(parts)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func receipt(ctx context.Context, tx pgx.Tx, actor participant.Ref, nonce, fingerprint string, out any) (bool, error) {
	if !validID(nonce, 4) {
		return false, ErrInvalid
	}
	// Serialize only retries sharing an actor/nonce; transaction lock releases on error.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, actor.Key()+":"+nonce); err != nil {
		return false, err
	}
	var existing string
	var raw []byte
	err := tx.QueryRow(ctx, `SELECT fingerprint,response FROM feedback_requests WHERE actor_key=$1 AND request_id=$2`, actor.Key(), nonce).Scan(&existing, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if existing != fingerprint {
		return false, ErrRequest
	}
	return true, json.Unmarshal(raw, out)
}
func saveReceipt(ctx context.Context, tx pgx.Tx, actor participant.Ref, nonce, fingerprint string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO feedback_requests(actor_key,request_id,fingerprint,response) VALUES($1,$2,$3,$4)`, actor.Key(), nonce, fingerprint, data)
	return err
}
func markRead(ctx context.Context, tx pgx.Tx, actor participant.Ref, id string, revision int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO feedback_reads(thread_id,reader_key,revision) VALUES($1,$2,$3) ON CONFLICT(thread_id,reader_key) DO UPDATE SET revision=GREATEST(feedback_reads.revision,EXCLUDED.revision)`, id, actor.Key(), revision)
	return err
}
func (s *Store) Create(ctx context.Context, actor participant.Ref, title, body, nonce string, diagnostics *Diagnostics) (Thread, error) {
	var t Thread
	if !validText(title, 160) || !validText(body, 20000) || !diagnostics.valid() {
		return t, ErrInvalid
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return t, err
	}
	defer tx.Rollback(ctx)
	diagnosticJSON, _ := json.Marshal(diagnostics)
	fingerprint := requestFingerprint("create", title, body, string(diagnosticJSON))
	hit, err := receipt(ctx, tx, actor, nonce, fingerprint, &t)
	if err != nil || hit {
		return t, err
	}
	ok, err := s.available(ctx, tx)
	if err != nil {
		return t, err
	}
	if !ok {
		return t, ErrUnavailable
	}
	a, err := author(ctx, tx, actor)
	if err != nil {
		return t, err
	}
	t = Thread{ID: newID(), Title: title, Body: body, Diagnostics: diagnostics, Status: "open", Author: a, Revision: 1}
	err = tx.QueryRow(ctx, `INSERT INTO feedback_threads(thread_id,author_key,author,title,body,diagnostics) VALUES($1,$2,$3,$4,$5,$6) RETURNING created_at,updated_at`, t.ID, actor.Key(), a, title, body, diagnostics).Scan(&t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return t, err
	}
	if err = markRead(ctx, tx, actor, t.ID, 1); err != nil {
		return t, err
	}
	if err = s.enqueue(ctx, tx, actor, t.ID, t.ID, 1); err != nil {
		return t, err
	}
	if err = saveReceipt(ctx, tx, actor, nonce, fingerprint, t); err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}
func (s *Store) Reply(ctx context.Context, actor participant.Ref, id, body, nonce string) (Message, error) {
	var m Message
	if !validText(body, 20000) {
		return m, ErrInvalid
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return m, err
	}
	defer tx.Rollback(ctx)
	// Scope admission precedes receipt lookup; retries cannot revive revoked access.
	t, err := s.thread(ctx, tx, actor, id, true)
	if err != nil {
		return m, err
	}
	fingerprint := requestFingerprint("reply", id, body)
	hit, err := receipt(ctx, tx, actor, nonce, fingerprint, &m)
	if err != nil || hit {
		return m, err
	}
	a, err := author(ctx, tx, actor)
	if err != nil {
		return m, err
	}
	m = Message{ID: newID(), Author: a, Body: body, Revision: t.Revision + 1}
	err = tx.QueryRow(ctx, `INSERT INTO feedback_events(event_id,thread_id,revision,kind,author,body) VALUES($1,$2,$3,'message',$4,$5) RETURNING created_at`, m.ID, id, m.Revision, a, body).Scan(&m.CreatedAt)
	if err != nil {
		return m, err
	}
	if _, err = tx.Exec(ctx, `UPDATE feedback_threads SET revision=$2,updated_at=$3 WHERE thread_id=$1`, id, m.Revision, m.CreatedAt); err != nil {
		return m, err
	}
	if err = markRead(ctx, tx, actor, id, m.Revision); err != nil {
		return m, err
	}
	if err = s.enqueue(ctx, tx, actor, id, m.ID, m.Revision); err != nil {
		return m, err
	}
	if err = saveReceipt(ctx, tx, actor, nonce, fingerprint, m); err != nil {
		return m, err
	}
	return m, tx.Commit(ctx)
}
func (s *Store) Status(ctx context.Context, actor participant.Ref, id, status string, revision int64) (Thread, error) {
	var t Thread
	if status != "open" && status != "resolved" {
		return t, ErrInvalid
	}
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return t, err
	}
	defer tx.Rollback(ctx)
	t, err = s.thread(ctx, tx, actor, id, true)
	if err != nil {
		return t, err
	}
	if revision != t.Revision {
		return t, ErrRevision
	}
	if t.Status == status {
		return t, nil
	}
	a, err := author(ctx, tx, actor)
	if err != nil {
		return t, err
	}
	t.Revision++
	t.Status = status
	eventID := newID()
	err = tx.QueryRow(ctx, `INSERT INTO feedback_events(event_id,thread_id,revision,kind,author,status) VALUES($1,$2,$3,'status',$4,$5) RETURNING created_at`, eventID, id, t.Revision, a, status).Scan(&t.UpdatedAt)
	if err != nil {
		return t, err
	}
	if _, err = tx.Exec(ctx, `UPDATE feedback_threads SET revision=$2,status=$3,updated_at=$4 WHERE thread_id=$1`, id, t.Revision, status, t.UpdatedAt); err != nil {
		return t, err
	}
	if err = markRead(ctx, tx, actor, id, t.Revision); err != nil {
		return t, err
	}
	t.Unread = false
	if err = s.enqueue(ctx, tx, actor, id, eventID, t.Revision); err != nil {
		return t, err
	}
	return t, tx.Commit(ctx)
}
func (s *Store) Read(ctx context.Context, actor participant.Ref, id string, revision int64) error {
	tx, err := s.begin(ctx, actor)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	t, err := s.thread(ctx, tx, actor, id, true)
	if err != nil {
		return err
	}
	if revision < 1 || revision > t.Revision {
		return ErrInvalid
	}
	if err = markRead(ctx, tx, actor, id, revision); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Avoid accidental exposure of internal database errors on either transport.
func errorCode(err error) (int, string) {
	switch {
	case errors.Is(err, ErrInvalid):
		return 400, err.Error()
	case errors.Is(err, ErrNotFound):
		return 404, err.Error()
	case errors.Is(err, ErrInstallation), errors.Is(err, ErrDisabled):
		return 403, err.Error()
	case errors.Is(err, ErrUnavailable):
		return 503, err.Error()
	case errors.Is(err, ErrRequest), errors.Is(err, ErrRevision):
		return 409, err.Error()
	}
	return 500, "internal_error"
}

func excerpt(body string) string {
	r := []rune(body)
	if len(r) > 320 {
		return string(r[:319]) + "…"
	}
	return body
}
