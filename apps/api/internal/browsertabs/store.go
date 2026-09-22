// Package browsertabs grants a secretary access to an exact live desktop tab.
package browsertabs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

var ErrInvalid = errors.New("invalid browser attachment request")
var ErrUnavailable = errors.New("browser attachment unavailable or not authorized")

type TabRef struct {
	RuntimeID string `json:"runtimeId"`
	ProfileID string `json:"profileId"`
	TabID     string `json:"tabId"`
}
type Attachment struct {
	ID           string `json:"attachment_id"`
	PersonaID    string `json:"persona_id"`
	Name         string `json:"name"`
	Tab          TabRef `json:"tab"`
	AllowActions bool   `json:"allow_actions"`
	Available    bool   `json:"available"`
}
type AttachInput struct {
	PersonaID    string `json:"persona_id"`
	Name         string `json:"name"`
	Tab          TabRef `json:"tab"`
	AllowActions bool   `json:"allow_actions"`
}
type Store struct {
	Pool *pgxpool.Pool
	Core *agentstate.Store
}

func New(pool *pgxpool.Pool, core *agentstate.Store) *Store { return &Store{pool, core} }

func (s *Store) Attach(ctx context.Context, human string, in AttachInput) (Attachment, string, error) {
	a := Attachment{ID: uuid.NewString(), PersonaID: in.PersonaID, Name: strings.TrimSpace(in.Name), Tab: in.Tab, AllowActions: in.AllowActions, Available: false}
	for _, id := range []string{in.PersonaID, in.Tab.RuntimeID, in.Tab.TabID} {
		if _, e := uuid.Parse(id); e != nil {
			return a, "", ErrInvalid
		}
	}
	if a.Name == "" || len(a.Name) > 120 || len(in.Tab.ProfileID) == 0 || len(in.Tab.ProfileID) > 80 {
		return a, "", ErrInvalid
	}
	tokenBytes := make([]byte, 32)
	if _, e := rand.Read(tokenBytes); e != nil {
		return a, "", e
	}
	token := "browser_" + base64.RawURLEncoding.EncodeToString(tokenBytes)
	digest := sha256.Sum256([]byte(token))
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return a, "", e
	}
	defer tx.Rollback(ctx)
	var owner string
	e = tx.QueryRow(ctx, `SELECT human_id FROM core_personas WHERE persona_id=$1 AND authority='active' FOR SHARE`, in.PersonaID).Scan(&owner)
	if e != nil || owner != human {
		return a, "", ErrUnavailable
	}
	_, e = tx.Exec(ctx, `INSERT INTO browser_tab_attachments(attachment_id,human_id,persona_id,name,tab,host_token_hash,allow_actions)VALUES($1,$2,$3,$4,$5,$6,$7)`, a.ID, human, a.PersonaID, a.Name, a.Tab, digest[:], a.AllowActions)
	if e != nil {
		return a, "", ErrInvalid
	}
	e = tx.Commit(ctx)
	return a, token, e
}
func (s *Store) Revoke(ctx context.Context, human, id string) error {
	if _, e := uuid.Parse(id); e != nil {
		return ErrInvalid
	}
	tag, e := s.Pool.Exec(ctx, `UPDATE browser_tab_attachments SET enabled=false WHERE attachment_id=$1 AND human_id=$2`, id, human)
	if e == nil && tag.RowsAffected() == 0 {
		return ErrUnavailable
	}
	return e
}
func (s *Store) List(ctx context.Context, human string) ([]Attachment, error) {
	rows, e := s.Pool.Query(ctx, `SELECT attachment_id,persona_id,name,tab,allow_actions,enabled AND last_seen_at>now()-interval '30 seconds' FROM browser_tab_attachments WHERE human_id=$1 AND enabled ORDER BY created_at LIMIT 100`, human)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Attachment{}
	for rows.Next() {
		var a Attachment
		if e := rows.Scan(&a.ID, &a.PersonaID, &a.Name, &a.Tab, &a.AllowActions, &a.Available); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *Store) Effects() map[string]agentstate.ToolEffect {
	effects := map[string]agentstate.ToolEffect{}
	effects["browser.tabs"] = agentstate.ToolEffect{ReadOnly: agentstate.AlwaysReadOnly, Apply: func(ctx context.Context, tx pgx.Tx, persona, idem string, req map[string]any) (map[string]any, error) {
		rows, e := tx.Query(ctx, `SELECT b.attachment_id,b.name,b.tab,b.allow_actions,b.last_seen_at>now()-interval '30 seconds' FROM browser_tab_attachments b JOIN core_personas p ON p.persona_id=b.persona_id AND p.human_id=b.human_id WHERE b.persona_id=$1 AND p.authority='active' AND b.enabled ORDER BY b.last_seen_at DESC,b.created_at DESC LIMIT 100`, persona)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id, name string
			var tab TabRef
			var write, available bool
			if e := rows.Scan(&id, &name, &tab, &write, &available); e != nil {
				return nil, e
			}
			out = append(out, map[string]any{"attachment_id": id, "name": name, "tab": tab, "allow_actions": write, "available": available})
		}
		return map[string]any{"tabs": out}, rows.Err()
	}}
	for _, method := range []string{"observe", "act"} {
		method := method
		effects["browser."+method] = agentstate.ToolEffect{Apply: func(ctx context.Context, tx pgx.Tx, persona, idem string, req map[string]any) (map[string]any, error) {
			id, _ := req["attachment_id"].(string)
			if _, e := uuid.Parse(id); e != nil {
				return nil, fmt.Errorf("%w: attachment_id required", agentstate.ErrBadRequest)
			}
			if method == "act" {
				if e := validateAction(req); e != nil {
					return nil, e
				}
			}
			var tab TabRef
			var allow bool
			e := tx.QueryRow(ctx, `SELECT b.tab,b.allow_actions FROM browser_tab_attachments b JOIN core_personas p ON p.persona_id=b.persona_id AND p.human_id=b.human_id WHERE b.attachment_id=$1 AND b.persona_id=$2 AND p.authority='active' AND b.enabled AND b.last_seen_at>now()-interval '30 seconds' FOR SHARE OF b,p`, id, persona).Scan(&tab, &allow)
			if errors.Is(e, pgx.ErrNoRows) || e == nil && method == "act" && !allow {
				return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrUnavailable)
			}
			if e != nil {
				return nil, e
			}
			request := map[string]any{"attachment_id": id, "tab": tab, "method": method}
			if method == "act" {
				request["binding"] = req["binding"]
				request["action"] = req["action"]
			}
			jobID, e := agentstate.EffectJobID(idem)
			if e != nil {
				return nil, e
			}
			_, e = tx.Exec(ctx, `INSERT INTO core_jobs(persona_id,job_id,kind,request,status,created_by)VALUES($1,$2,'browser',$3,'queued',$4)`, persona, jobID, request, "tool:"+idem)
			return map[string]any{"job": map[string]any{"job_id": jobID, "kind": "browser", "status": "queued"}}, e
		}}
	}
	return effects
}
func validateAction(req map[string]any) error {
	bad := fmt.Errorf("%w: invalid browser action or observation binding", agentstate.ErrBadRequest)
	raw, e := json.Marshal(req)
	if e != nil || len(raw) > 32<<10 {
		return bad
	}
	b, ok := req["binding"].(map[string]any)
	if !ok {
		return bad
	}
	rev, ok := b["revision"].(float64)
	if !ok || rev < 0 || rev != float64(int64(rev)) {
		return bad
	}
	if _, ok := b["url"].(string); !ok {
		return bad
	}
	id, _ := b["observationId"].(string)
	if _, e := uuid.Parse(id); e != nil {
		return bad
	}
	a, ok := req["action"].(map[string]any)
	if !ok {
		return bad
	}
	switch a["kind"] {
	case "click", "fill", "scroll", "navigate":
		return nil
	}
	return bad
}

// Claim is the dispatch-admission linearization point. Revocation stops future
// admissions, not an already admitted remote action. A lost response is not retried.
func (s *Store) Claim(ctx context.Context, id, token string) (*agentstate.Job, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	a, e := s.authenticate(ctx, tx, id, token, true)
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec(ctx, `UPDATE browser_tab_attachments SET last_seen_at=now() WHERE attachment_id=$1`, id)
	if e != nil {
		return nil, e
	}
	var jobID string
	e = tx.QueryRow(ctx, `UPDATE core_jobs SET status='running',claimed_by=$2,claim_expires_at=now()+interval '30 seconds',started_at=now()
 WHERE persona_id=$1 AND job_id=(SELECT job_id FROM core_jobs WHERE persona_id=$1 AND kind='browser' AND status='queued' AND request->>'attachment_id'=$3 AND (request->>'method'='observe' OR $4) AND NOT EXISTS(SELECT 1 FROM core_jobs WHERE persona_id=$1 AND kind='browser' AND status IN('running','cancel_requested') AND request->>'attachment_id'=$3) ORDER BY created_at,job_id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING job_id`, a.PersonaID, "browser:"+id, id, a.AllowActions).Scan(&jobID)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	if jobID == "" {
		return nil, nil
	}
	j, e := s.Core.GetJob(ctx, a.PersonaID, jobID)
	return &j, e
}
func (s *Store) authenticate(ctx context.Context, tx pgx.Tx, id, token string, enabled bool) (Attachment, error) {
	var a Attachment
	if _, e := uuid.Parse(id); e != nil {
		return a, ErrUnavailable
	}
	if len(token) > 100 {
		return a, ErrUnavailable
	}
	digest := sha256.Sum256([]byte(token))
	e := tx.QueryRow(ctx, `SELECT b.attachment_id,b.persona_id,b.name,b.tab,b.allow_actions FROM browser_tab_attachments b JOIN core_personas p ON p.persona_id=b.persona_id AND p.human_id=b.human_id WHERE b.attachment_id=$1 AND b.host_token_hash=$2 AND (NOT $3 OR (b.enabled AND p.authority='active')) FOR UPDATE OF b FOR SHARE OF p`, id, digest[:], enabled).Scan(&a.ID, &a.PersonaID, &a.Name, &a.Tab, &a.AllowActions)
	if errors.Is(e, pgx.ErrNoRows) {
		return a, ErrUnavailable
	}
	return a, e
}
func (s *Store) Complete(ctx context.Context, id, token, jobID, status string, result map[string]any, problem string) (agentstate.Job, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return agentstate.Job{}, e
	}
	defer tx.Rollback(ctx)
	a, e := s.authenticate(ctx, tx, id, token, false)
	if e != nil {
		return agentstate.Job{}, e
	}
	// Release attachment lock before the job store's transaction. Completion is
	// evidence for already dispatched work and stays allowed after revocation.
	if e = tx.Commit(ctx); e != nil {
		return agentstate.Job{}, e
	}
	j, e := s.Core.GetJob(ctx, a.PersonaID, jobID)
	if e != nil || j.Kind != "browser" || j.Request["attachment_id"] != id || j.ClaimedBy == nil || *j.ClaimedBy != "browser:"+id {
		return agentstate.Job{}, ErrUnavailable
	}
	if len(problem) > 1024 {
		return agentstate.Job{}, ErrInvalid
	}
	raw, e := json.Marshal(result)
	if e != nil || len(raw) > 256<<10 {
		return agentstate.Job{}, ErrInvalid
	}
	return s.Core.CompleteJob(ctx, a.PersonaID, jobID, "browser:"+id, status, result, problem)
}

// Sweep makes vanished hosts and expired claims visible even without new user
// input. It never requeues a dispatched command.
func (s *Store) Sweep(ctx context.Context) error {
	if _, e := s.Core.SweepExpiredJobs(ctx, []string{"browser"}, 64); e != nil {
		return e
	}
	rows, e := s.Pool.Query(ctx, `UPDATE core_jobs j SET status='running',claimed_by='browser-expiry',claim_expires_at=now()+interval '30 seconds',started_at=now() WHERE (j.persona_id,j.job_id) IN (SELECT persona_id,job_id FROM core_jobs WHERE status='queued' AND kind='browser' AND created_at<now()-interval '60 seconds' ORDER BY created_at LIMIT 64 FOR UPDATE SKIP LOCKED) RETURNING j.persona_id,j.job_id`)
	if e != nil {
		return e
	}
	type key struct{ p, j string }
	keys := []key{}
	for rows.Next() {
		var k key
		if e = rows.Scan(&k.p, &k.j); e != nil {
			rows.Close()
			return e
		}
		keys = append(keys, k)
	}
	rows.Close()
	if e = rows.Err(); e != nil {
		return e
	}
	for _, k := range keys {
		if _, e = s.Core.CompleteJob(ctx, k.p, k.j, "browser-expiry", "failed", map[string]any{"dispatched": false, "outcome": "not_dispatched"}, "browser host unavailable or grant revoked before dispatch"); e != nil {
			return e
		}
	}
	return nil
}
func (s *Store) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if e := s.Sweep(ctx); e != nil && ctx.Err() == nil {
				log.Printf("browser job sweep failed: %v", e)
			}
		}
	}
}
