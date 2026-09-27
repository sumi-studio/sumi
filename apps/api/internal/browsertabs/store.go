// Package browsertabs grants a secretary access to an exact live desktop tab.
package browsertabs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

var ErrInvalid = errors.New("invalid browser attachment request")
var ErrUnavailable = errors.New("browser attachment unavailable or not authorized")
var ErrJevUnavailable = errors.New("the Jev operation layer is not configured on this tab's browser host; use browser.observe and browser.act directly")

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
	// Cloud is set when the Sumi-owned Cloud browser host is configured. Its
	// tabs are available whenever their profile is enabled: queued work wakes
	// the browser (cloudbrowser.Service), so no poll is required beforehand.
	Cloud bool
}

func New(pool *pgxpool.Pool, core *agentstate.Store) *Store { return &Store{Pool: pool, Core: core} }

// cloudTab is the TabRef.profileId of Sumi-owned Cloud browser tabs.
const cloudTab = "cloud"

// availableSQL is true for a grant whose host can take work now: a host that
// polled within 30 seconds, or an enabled Cloud profile the API can wake.
func (s *Store) availableSQL() string {
	live := `b.last_seen_at>now()-interval '30 seconds'`
	if !s.Cloud {
		return "(" + live + ")"
	}
	return `(` + live + ` OR (b.tab->>'profileId'='` + cloudTab + `' AND EXISTS(SELECT 1 FROM cloud_browser_profiles c WHERE c.profile_id::text=b.tab->>'runtimeId' AND c.enabled AND c.human_id=b.human_id AND c.persona_id=b.persona_id)))`
}

// jevSQL: a desktop host declares Jev on each poll; a Cloud tab has the Jev
// layer when its person stored a Jev key that Jev has not rejected and, while
// its browser runs, that browser has picked the key up (declared on its
// latest poll). A sleeping browser receives the key when it starts.
func (s *Store) jevSQL() string {
	if !s.Cloud {
		return "b.jev_available"
	}
	return `(CASE WHEN b.tab->>'profileId'='` + cloudTab + `' THEN EXISTS(SELECT 1 FROM cloud_browser_jev_credentials k WHERE k.human_id=b.human_id AND NOT k.rejected) AND (b.jev_available OR NOT EXISTS(SELECT 1 FROM cloud_browser_profiles c WHERE c.profile_id::text=b.tab->>'runtimeId' AND c.state='live')) ELSE b.jev_available END)`
}

func (s *Store) Attach(ctx context.Context, human string, in AttachInput) (Attachment, string, error) {
	a := Attachment{ID: uuid.NewString(), PersonaID: in.PersonaID, Name: strings.TrimSpace(in.Name), Tab: in.Tab, AllowActions: in.AllowActions, Available: false}
	for _, id := range []string{in.PersonaID, in.Tab.RuntimeID, in.Tab.TabID} {
		if _, e := uuid.Parse(id); e != nil {
			return a, "", ErrInvalid
		}
	}
	// Cloud tabs are granted through cloudbrowser, whose host is the Worker.
	if a.Name == "" || len(a.Name) > 120 || len(in.Tab.ProfileID) == 0 || len(in.Tab.ProfileID) > 80 || in.Tab.ProfileID == cloudTab {
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
	rows, e := s.Pool.Query(ctx, `SELECT b.attachment_id,b.persona_id,b.name,b.tab,b.allow_actions,b.enabled AND `+s.availableSQL()+` FROM browser_tab_attachments b WHERE b.human_id=$1 AND b.enabled ORDER BY b.created_at LIMIT 100`, human)
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
		rows, e := tx.Query(ctx, `SELECT b.attachment_id,b.name,b.tab,b.allow_actions,`+s.availableSQL()+`,`+s.jevSQL()+` FROM browser_tab_attachments b JOIN core_personas p ON p.persona_id=b.persona_id AND p.human_id=b.human_id WHERE b.persona_id=$1 AND p.authority='active' AND b.enabled ORDER BY b.last_seen_at DESC,b.created_at DESC LIMIT 100`, persona)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id, name string
			var tab TabRef
			var write, available, jev bool
			if e := rows.Scan(&id, &name, &tab, &write, &available, &jev); e != nil {
				return nil, e
			}
			// direct: browser.observe/act. jev: browser.goal, which also needs
			// the action grant and a host that declared a usable Jev key on
			// its latest poll. A host that stopped polling has no current
			// declaration, so its last one is not repeated as availability.
			layer := "not_configured"
			switch {
			case !available:
				layer = "host_offline"
			case jev:
				layer = "available"
			}
			out = append(out, map[string]any{"attachment_id": id, "name": name, "tab": tab, "allow_actions": write, "available": available, "operation_layers": map[string]any{"direct": true, "jev": layer}})
		}
		return map[string]any{"tabs": out}, rows.Err()
	}}
	for _, method := range []string{"observe", "act", "goal"} {
		method := method
		effects["browser."+method] = agentstate.ToolEffect{Validate: func(req map[string]any) error {
			return validateRequest(method, req)
		}, Apply: func(ctx context.Context, tx pgx.Tx, persona, idem string, req map[string]any) (map[string]any, error) {
			if e := validateRequest(method, req); e != nil {
				return nil, e
			}
			id, _ := req["attachment_id"].(string)
			var tab TabRef
			var allow, jev bool
			e := tx.QueryRow(ctx, `SELECT b.tab,b.allow_actions,`+s.jevSQL()+` FROM browser_tab_attachments b JOIN core_personas p ON p.persona_id=b.persona_id AND p.human_id=b.human_id WHERE b.attachment_id=$1 AND b.persona_id=$2 AND p.authority='active' AND b.enabled AND `+s.availableSQL()+` FOR SHARE OF b,p`, id, persona).Scan(&tab, &allow, &jev)
			if errors.Is(e, pgx.ErrNoRows) || e == nil && method != "observe" && !allow {
				return nil, fmt.Errorf("%w: %w", agentstate.ErrBadRequest, ErrUnavailable)
			}
			if e != nil {
				return nil, e
			}
			if method == "goal" && !jev {
				// Distinct from choosing the direct path: nothing was queued and
				// no other model is substituted for the missing operation layer.
				return nil, fmt.Errorf("%w: %w", agentstate.ErrBadRequest, ErrJevUnavailable)
			}
			request := map[string]any{"attachment_id": id, "tab": tab, "method": method}
			switch method {
			case "act":
				request["binding"] = req["binding"]
				request["action"] = req["action"]
			case "goal":
				for _, k := range []string{"goal", "inputs", "private_inputs", "max_steps"} {
					if v, ok := req[k]; ok {
						request[k] = v
					}
				}
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
func validateRequest(method string, req map[string]any) error {
	id, _ := req["attachment_id"].(string)
	if _, e := uuid.Parse(id); e != nil {
		return fmt.Errorf("%w: attachment_id required", agentstate.ErrBadRequest)
	}
	switch method {
	case "act":
		return validateAction(req)
	case "goal":
		return validateGoal(req)
	}
	return nil
}

// jsLength counts UTF-16 code units, matching the desktop host's bounds.
func jsLength(s string) int { return len(utf16.Encode([]rune(s))) }

var inputName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)

// validateGoal mirrors the host's bounds so a malformed goal fails before it
// is queued (and before an elevated call could ask a person to approve it).
func validateGoal(req map[string]any) error {
	bad := func(why string) error {
		return fmt.Errorf("%w: invalid browser goal: %s", agentstate.ErrBadRequest, why)
	}
	raw, e := json.Marshal(req)
	if e != nil || len(raw) > 32<<10 {
		return bad("request exceeds 32 KiB")
	}
	if hasNUL(string(raw)) {
		return bad("NUL characters are not allowed")
	}
	goal, _ := req["goal"].(string)
	if strings.TrimSpace(goal) == "" || jsLength(goal) > 2000 {
		return bad("goal text of at most 2000 characters is required")
	}
	inputs := map[string]any{}
	if v, ok := req["inputs"]; ok {
		if inputs, ok = v.(map[string]any); !ok || len(inputs) > 16 {
			return bad("inputs must be an object with at most 16 entries")
		}
	}
	for k, v := range inputs {
		s, ok := v.(string)
		if !inputName.MatchString(k) || !ok || jsLength(s) > 8000 {
			return bad("input names are lower_snake_case and values are text of at most 8000 characters")
		}
	}
	if v, ok := req["private_inputs"]; ok {
		list, ok := v.([]any)
		if !ok {
			return bad("private_inputs must be a list of input names")
		}
		for _, n := range list {
			s, _ := n.(string)
			if _, ok := inputs[s]; !ok {
				return bad("private_inputs must name provided inputs")
			}
		}
	}
	if v, ok := req["max_steps"]; ok {
		n, ok := v.(float64)
		if !ok || n != float64(int64(n)) || n < 1 || n > 40 {
			return bad("max_steps must be an integer from 1 to 40")
		}
	}
	return nil
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
	case "click", "fill", "select", "scroll", "navigate":
		return nil
	}
	return bad
}

// Claim is the dispatch-admission linearization point. Revocation stops future
// admissions, not an already admitted remote action. A lost response is not retried.
func (s *Store) Claim(ctx context.Context, id, token string, jev bool) (*agentstate.Job, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	a, e := s.authenticate(ctx, tx, id, token, true)
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec(ctx, `UPDATE browser_tab_attachments SET last_seen_at=now(),jev_available=$2 WHERE attachment_id=$1`, id, jev)
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

// Progress renews a running goal's claim and the tab's presence, and records
// the host's latest progress under result.progress (terminal completion
// replaces it). It returns the job status so the host stops before its next
// action once cancellation was requested. A revoked grant fails
// authentication; a lost/finished/over-budget goal is ErrJobNotClaimed.
func (s *Store) Progress(ctx context.Context, id, token, jobID string, progress map[string]any) (string, error) {
	raw, e := json.Marshal(progress)
	if e != nil || len(raw) > 16<<10 || hasNUL(string(raw)) {
		return "", ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	a, e := s.authenticate(ctx, tx, id, token, true)
	if e != nil {
		return "", e
	}
	if _, e = tx.Exec(ctx, `UPDATE browser_tab_attachments SET last_seen_at=now() WHERE attachment_id=$1`, id); e != nil {
		return "", e
	}
	var status string
	// Only a running goal is renewed. After job.cancel the host has the
	// remaining claim to report; a host that keeps going is swept lost.
	e = tx.QueryRow(ctx, `UPDATE core_jobs SET claim_expires_at=CASE WHEN status='running' THEN now()+interval '30 seconds' ELSE claim_expires_at END,result=COALESCE(result,'{}'::jsonb)||jsonb_build_object('progress',$4::jsonb)
 WHERE persona_id=$1 AND job_id=$2 AND kind='browser' AND claimed_by=$3 AND request->>'method'='goal' AND request->>'attachment_id'=$5 AND status IN('running','cancel_requested') AND started_at>now()-interval '6 minutes' RETURNING status`, a.PersonaID, jobID, "browser:"+id, raw, id).Scan(&status)
	if errors.Is(e, pgx.ErrNoRows) {
		return "", agentstate.ErrJobNotClaimed
	}
	if e != nil {
		return "", e
	}
	return status, tx.Commit(ctx)
}

// hasNUL checks encoded JSON: jsonb cannot store NUL, which website text can contain.
func hasNUL(s string) bool { return strings.ContainsRune(s, 0) || strings.Contains(s, `\u0000`) }
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
	if e != nil {
		return agentstate.Job{}, ErrInvalid
	}
	if j.Status == "lost" {
		return s.attachLostOutcome(ctx, a.PersonaID, jobID, "browser:"+id, status, result, problem)
	}
	if len(raw) > 256<<10 {
		return agentstate.Job{}, ErrInvalid
	}
	j, e = s.Core.CompleteJob(ctx, a.PersonaID, jobID, "browser:"+id, status, result, problem)
	if errors.Is(e, agentstate.ErrJobConflict) {
		// The claim expired to 'lost' between the read and the completion
		// transaction. The swept verdict still stands, but the host's
		// actually-observed outcome is durable evidence under it.
		if fresh, ferr := s.Core.GetJob(ctx, a.PersonaID, jobID); ferr == nil && fresh.Status == "lost" {
			return s.attachLostOutcome(ctx, a.PersonaID, jobID, "browser:"+id, status, result, problem)
		}
	}
	return j, e
}

// attachLostOutcome preserves the host's observed outcome on a job the
// claim-expiry sweep already marked lost: the 'lost' verdict and its
// already-sent notification stand, the report lands under
// result.observed_outcome, an identical receipt replays the stored row and
// a divergent one conflicts. Nothing re-executes and nothing notifies again.
func (s *Store) attachLostOutcome(ctx context.Context, personaID, jobID, runner, status string, result map[string]any, problem string) (agentstate.Job, error) {
	switch status {
	case "done", "failed", "cancelled":
	default:
		return agentstate.Job{}, fmt.Errorf("%w: complete status must be done, failed, or cancelled", agentstate.ErrBadRequest)
	}
	outcome := map[string]any{"status": status, "result": result}
	if problem != "" {
		outcome["error"] = problem
	}
	if raw, e := json.Marshal(outcome); e != nil || len(raw) > 64<<10 {
		// The report exceeds AttachLostOutcome's durable bound. Record that
		// it arrived — status, size and content digest — so an identical
		// receipt still replays and a divergent one still conflicts, rather
		// than leaving the host retrying an unstoreable receipt forever.
		sum := sha256.Sum256(raw)
		outcome = map[string]any{"status": status, "result_unavailable": "observed result exceeds the durable bound; size and digest identify the host's report", "result_bytes": len(raw), "result_sha256": hex.EncodeToString(sum[:])}
		if problem != "" {
			outcome["error"] = problem
		}
	}
	return s.Core.AttachLostOutcome(ctx, personaID, jobID, runner, outcome)
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
